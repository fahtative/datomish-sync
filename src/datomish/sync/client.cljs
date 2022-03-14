(ns datomish.sync.client
  (:require
   [taoensso.sente :as sente]
   [datomish.api :as d]
   [cljs.core.async :as a]
   [datomish.sync.common :refer [meta-datom?]]
   [datascript.core :as ds]))

;; ---------------------------------------- ;;
;;;; Utils

(defn third [x] (nth x 2))
(defn fourth [x] (nth x 3))
(defn fifth [x] (nth x 4))

(defn temp-str []
  "Generate placeholder unique string."
  (str (gensym)))

(defn remove-non-distinct
  "Apply f to each element then remove duplicated values."
  [f coll]
  (->> coll
       (group-by f)
       (mapcat (fn [[k elems]] (when (= (count elems) 1) elems)))
       (remove nil?)))

(defn pop-thunks-atom!
  "Pops and invokes all thunks in an atom."
  [atom]
  (loop [t (ffirst (swap-vals! atom pop))]
    (t)
    (when-not (empty? @atom)
      (recur (ffirst (swap-vals! atom pop))))))

(defn max-tx
  "Get the highest tx-id."
  [tx-data]
  (->> tx-data (map fourth) (apply max)))

(defn ea
  "Get some datom of a db matching the given entity and attr."
  [db entity attr]
  (some-> (d/q '[:find [?e ?a ?v ?t]
                 :in $ ?e ?a
                 :where [?e ?a ?v ?t]]
               db entity attr)
          (conj true)))

(defn map-style-schema
  "Convert Datomic-style schema to Datascript-style,
  i.e. maps collection -> maps indexed by attribute name"
  [scm]
  (->> scm
       (map (fn [{:keys [db/ident] :as s}]
              [ident (-> s (dissoc :db/ident))]))
       (into {})))

;; ---------------------------------------- ;;
;;;; Mandatory DB-Schema

(def schema
  "A schema the db intended to be synced must add."
  [{:db/ident      :datomish.sync.host-datom/host+hosteid+attr
    :db/tupleAttrs [:datomish.sync.host-datom/host
                    :datomish.sync.host-datom/host-eid
                    :datomish.sync.host-datom/attr]
    :db/unique     :db.unique/identity}])

(def attr-schema
  "The required schema in attribute-indexed (datascript) style."
  (map-style-schema schema))

;; -------------------------------------------------- ;;
;;;; Host-datom Meta
;; A db-entity describing an original host's datom,
;; with reference to a (client) db entity.

(defn hid->cid
  "Resolves a host-eid of a host to a client-eid."
  ([db host-eid]
   (d/q
    '[:find ?client-eid .
      :in $ ?host-eid
      :where
      [?e :datomish.sync.host-datom/host-eid ?host-eid]
      [?e :datomish.sync.host-datom/client-eid ?client-eid]]
    db host-eid))
  ([db host host-eid]
   (first (d/q
           '[:find [?client-eid (max ?t)]
             :in $ ?host ?host-eid
             :where
             [?e :datomish.sync.host-datom/host ?host ?t]
             [?e :datomish.sync.host-datom/host-eid ?host-eid]
             [?e :datomish.sync.host-datom/client-eid ?client-eid]]
           db host host-eid))))

(defn host-datom-meta-eid
  "Retrive the eid of a host datom matching the given entity and attr."
  [db host entity attr]
  (d/entid db
           [:datomish.sync.host-datom/host+hosteid+attr
            [host entity attr]]))

(defn ea->host-datom
  "Resolves an entity-attribute to a host's datom."
  [db host [e a]]
  (d/q
   '[:find [?host-eid ?attr ?val ?tx ?add?]
     :in $ ?host ?client-eid ?attr
     :where
     [?e :datomish.sync.host-datom/host ?host ?t]
     [?e :datomish.sync.host-datom/client-eid ?client-eid]
     [?e :datomish.sync.host-datom/host-eid ?host-eid]
     [?e :datomish.sync.host-datom/attr ?attr]
     [?e :datomish.sync.host-datom/val ?val]
     [?e :datomish.sync.host-datom/tx ?tx]
     [?e :datomish.sync.host-datom/add? ?add?]]
   db host e a))

(defn ref?
  "Test if an attribute is a reference type."
  [schema a]
  (when schema
    (= (some-> a schema :db/valueType) :db.type/ref))
  #_(d/q
     '[:find ?e .
       :in $ ?a
       :where
       [?e :db/ident ?a]
       [?e :db/valueType :db.type/ref]]
     db a))

;; -------------------------------------------------- ;;
;;;; Sync-filter Meta
;; A db-entity describing the last tx-id received
;; from a host with respect to a filter.

(defn sync-filter-eid
  "Get meta entity's id of a filter."
  [db host filter]
  (d/q
   '[:find ?e .
     :in $ ?host ?filter
     :where
     [?e :datomish.sync.sync-filter/host ?host]
     [?e :datomish.sync.sync-filter/filter ?filter]]
   db host filter))

(defn update-sync-filter
  "Update last-sync info of a filter meta."
  [db host filter last-sync-tx]
  #:datomish.sync.sync-filter
  {:db/id (or (sync-filter-eid db host filter) (temp-str))
   :host         host
   :filter       filter
   :last-sync-tx last-sync-tx})

(defn last-sync-tx
  "Get the last tx-id of a filter."
  [db host filter]
  (d/q
   '[:find ?tx .
     :in $ ?host ?filter
     :where
     [?e :datomish.sync.sync-filter/host ?host]
     [?e :datomish.sync.sync-filter/filter ?filter]
     [?e :datomish.sync.sync-filter/last-sync-tx ?tx]]
   db host filter))

;; -------------------------------------------------- ;;
;;;; Reading Host's Tick

(defn digest-host-datom
  "Returns tx-data and host-datom meta builder.
  The builder is a function of tempids intended to be called
  after transacting the tx-data to build meta info."
  [[e a v t add?] db host hid->cid-fn schema]
  (let [cid (or (hid->cid-fn e) (- e))
        ref-val? (ref? schema a)
        ref-val (when ref-val? (or (hid->cid-fn v) (- v)))]
    [[(if add? :db/add :db/retract) cid a (or ref-val v)]
     (fn [tempids]
       [#:datomish.sync.host-datom{:db/id (or (host-datom-meta-eid db host e a)
                                              (temp-str))
                                   :host-eid   e
                                   :client-eid (or (get tempids cid) cid)
                                   :host       host
                                   :attr       a
                                   :val        v
                                   :tx         t
                                   :add?       add?}
        (when (and ref-val? (neg-int? ref-val))
          #:datomish.sync.host-datom{:host-eid   v
                                     :host       host
                                     :client-eid (get tempids ref-val)})])]))

(defn keep-max-tx-of-identical-host-meta-entities
  "Keep only the highest tx-id from similar host-meta entities."
  [tx-data]
  (->> tx-data
       (group-by
        (juxt :datomish.sync.host-datom/host
              :datomish.sync.host-datom/host-eid
              :datomish.sync.host-datom/attr))
       (mapcat (fn [[[host] tx]]
                 (if host
                   (if (= 1 (count tx))
                     tx
                     [(apply max-key :datomish.sync.host-datom/tx tx)])
                   tx)))))

(defn transact-host-tick!
  "Transact an update received from remote host."
  [dbc host dfilter host-datoms]
  (let [db @dbc]
    (let [tx-host-meta-builder-pairs
          (->> host-datoms
               (map #(digest-host-datom % db host (partial hid->cid db host)
                                        (d/schema dbc)))
               (remove nil?))
          host-tx (->> tx-host-meta-builder-pairs (map first))]
      (let [{:keys [tempids]}
            (d/transact! dbc host-tx {::host-tick true})
            tx-data (->> tx-host-meta-builder-pairs
                         (mapcat (fn [[_ b]] (b tempids)))
                         (remove nil?)
                         keep-max-tx-of-identical-host-meta-entities)]
        (d/transact! dbc
                     tx-data
                     {::host-tick true})))))

;; -------------------------------------------------- ;;
;;;; Connection
;; A connection keeps track of syncing-db, write or read,
;; such that we may known where message coming in is going, and
;; message going out coming from.

;; Below is the structure of the internal registries:

;; Write Registry:
;; #atom{<db>          {:filters    #atom#{<datom-filter> +}
;;                      :listen-key <key>}
;;       ...           ...}

;; Read Registry:
;; #atom{<datom-filter> #{[<db> <on-failure-fn>] +}
;;       ...           ...}

;; - We'll remove any read-filter with empty db set
;; and any db with empty write-filter set, along with the db listener.
;; - For each connection, one listen-key is registered for each db.
;; - If a connection is closed, we'll keep pending write thunk around
;; to invoke when it's re-opened.

(defn host-error? [m]
  (and (map? m) (:datomish.sync.host/error? m)))

(defn dispatch-host-tick
  "Dispatch on a host-tick.
  The host-tick is a map of :data and :filter.
  Data may represent error or tx-data.
  Either updates each db registered to the filter with tx-data and meta-info,
  or invokes on-failure."
  [{:as tick :keys [data dfilter]} {:as connection :keys [read-reg host]}]
  (doseq [[dbc on-failure] (get @read-reg dfilter)]
    (if-not (host-error? data)
      (transact-host-tick! dbc host dfilter data)
      (on-failure data))))

(defn host-listen!
  "Listen for a host tick."
  [ch-recv conn]
  (a/go-loop []
    (let [{:keys [event]} (a/<! ch-recv)]
      (let [[sente-id [ev-id data]] event]
        (case ev-id
          :host/tick (dispatch-host-tick data conn)
          nil)))
    (recur)))

(defn connect
  "Connect to a host's connector.
  Return a connection to be used for syncing."
  ([uri csrf] (connect uri csrf {}))
  ([uri csrf {:keys [sente-config]}]
   (let [{:keys [send-fn state ch-recv chsk]}
         (sente/make-channel-socket! uri csrf sente-config)
         r-reg   (atom {})
         w-reg   (atom {})
         pending (atom #queue [])
         conn    {:host          uri
                  :send-fn       send-fn
                  :chsk          chsk
                  :state         state
                  :write-reg     w-reg
                  :read-reg      r-reg
                  :pending-write pending
                  }]
     (host-listen! ch-recv conn)
     (add-watch state ::write-queue
                (fn [key ref old new]
                  (when (and  (:open? new)
                              (not (empty? @pending)))
                    (pop-thunks-atom! pending))))
     conn)))

;; -------------------------------------------------- ;;
;;;; Sync-read
;; I.e. pulling data from a host.

(defn sync-read
  "<filter-id> is any value matching one declared on the host, i.e. an endpoint.
  <on-failure> accepts any error-map the host returns."
  ([conn dbc filter-id] (sync-read conn dbc filter-id {}))
  ([{:keys [state send-fn host read-reg]}
    dbc filter-id
    {:keys [initial-load?
            load-only-difference?
            ping-on-open?
            on-failure
            timeout]
     :or   {initial-load?         true
            load-only-difference? true
            on-failure            (constantly nil)
            ping-on-open?         true
            timeout               8000}}]
   (let [db @dbc]
     (let [params (merge {:filter filter-id}
                         (when initial-load?
                           {:reply-with-load? initial-load?})
                         (when load-only-difference?
                           {:last-sync-tx (last-sync-tx db host filter-id)}))]
       (when ping-on-open?
         (add-watch state ::open-ping
                    (fn [key ref old new]
                      (when (:open? new)
                        (send-fn [:client/tick-ping {:filter filter-id}])))))
       (send-fn [:client/subscribe params] timeout
                ;; tick-data is the same as :data of host-tick map.
                (fn [tick-data]
                  ;; If not initial-load?, tick-data is host-error
                  ;; or whatever (not necessary tx-data).
                  (when (sente/cb-success? tick-data)
                    (if (host-error? tick-data)
                      (on-failure tick-data)
                      (do
                        (when initial-load?
                          (transact-host-tick! dbc host filter-id tick-data))
                        (swap! read-reg update filter-id
                               (fnil conj #{}) [dbc on-failure]))))))))))

;; -------------------------------------------------- ;;
;;;; Datom Reversion
;; In case of failure to write on the remote host,
;; we can revert the effect on the client.

;; We cannot look at one datom at a time in a tx-data:
;; consider that a retract to add would result in an add if db
;; has already retracted the add.
;; We have to find one datom that gives the state of the db after tx-data,
;; and only consider to reverse the effect of that datom.

;; This could easily be avoided if we have last retracting
;; datom (and therefore value and tx-id to compare with our retract).
;; With history, we could just ask if current-datom (add or retract) has our tx-id.

(defn effective-datoms
  "Returns datoms that are the state of a db when applying these datoms."
  [datoms]
  (->> datoms
       (group-by (juxt first second third))
       (map (fn [[[e a] datoms]]
              ;; eliminate same-value retract and add:
              (let [distinct-datoms (remove-non-distinct third datoms)]
                (if (< 1 (count distinct-datoms))
                  ;; pick add over retract:
                  (first (filter #(fifth %) distinct-datoms))
                  (first distinct-datoms)))))
       (remove nil?)))

(defn still-datom? [db [e a v t add?]]
  (if-let [[_ _ v1 t1] (ea db e a)]
    (= [v t] [v1 t1])
    (not add?) ; a retract should leave no datom
    ))

(defn maybe-reverse
  "Derives original datoms from db-before and tx-data,
  if and only if the db still holds datom of tx-data."
  [db {:keys [tx-data db-before]}]
  (->> (for [[e a v tx add? :as datom] (effective-datoms tx-data)]
         (when (still-datom? db datom)
           [(if add? :db/retract :db/add) e a v]
           #_(if-let [[e0 a0 v0] (ea db-before e a)]
               [:db/add e0 a0 v0]
               ;; Maybe some other tx retract it?
               ;; Can't know without history:
               [:db/retract e a])))
       (remove nil?)
       ))

;; -------------------------------------------------- ;;
;;;; Sync-write
;; i.e. pushing updates to remote host.

(defn cid->hid
  "Resolves a host-eid of a host to a client-eid."
  ([db client-eid]
   (d/q
    '[:find ?host-eid .
      :in $ ?client-eid
      :where
      [?e :datomish.sync.host-datom/host-eid ?host-eid]
      [?e :datomish.sync.host-datom/client-eid ?client-eid]]
    db client-eid))
  ([db host client-eid]
   (first (d/q
           '[:find [?host-eid (max ?t)]
             :in $ ?host ?client-eid
             :where
             [?e :datomish.sync.host-datom/host ?host ?t]
             [?e :datomish.sync.host-datom/host-eid ?host-eid]
             [?e :datomish.sync.host-datom/client-eid ?client-eid]]
           db host client-eid))))

(defn spit-host-datom
  "Spits a datom to be handed to a host,
  where eid is either the host's or temp-id,
  tx is either zero or host's last-sync tx-id."
  [db host [e a v t add?] schema]
  (let [v (if (ref? schema a)
            (or (cid->hid db host v)
                (- v))
            v)]
    (if-let [[he ha hv ht hadd?] (ea->host-datom db host [e a])]
      [he a v ht add?]
      [(or (cid->hid db host e)
           (- e))
       a v 0 add?])))

(defn host-tempids->cid-resolver [db host m]
  (let [temp (->> m
                  (map (fn [[n h]]
                         (when (neg-int? n)
                           [h (- n)])))
                  (into {}))]
    (fn [h]
      (or (hid->cid db host h)
          (get temp h)))))

(defn host-reply-confirm->tx-data [reply host db schema]
  (->> (:tx-data reply)
       (filter meta-datom?)
       (map (fn [d]
              (let [[_ b]
                    (digest-host-datom d db host
                                       (host-tempids->cid-resolver
                                        db host (:tempids reply))
                                       schema)]
                (b {}))))
       (remove nil?)
       (apply concat)))

(defn write-and-handle!
  [{:as conn :keys [host send-fn report-fn open-send-queue]}
   dbc {:as report :keys [tx-meta db-after]}
   datom-filter data]
  (let [db @dbc]
    (send-fn [:client/write data]
             20000 ; How do we get sente's indefinite timeout?
             (fn [reply]
               (when (sente/cb-success? reply)
                 (if (:datomish.sync.host/error? reply)
                   (do (when-let [fail-fn (::on-failure tx-meta)]
                         (fail-fn host datom-filter reply))
                       (when-let [tx (not-empty (maybe-reverse @dbc report))]
                         (d/transact! dbc tx {::reverse-effect? true})))
                   (do (d/transact! dbc (host-reply-confirm->tx-data
                                         reply host db
                                         (d/schema dbc)))
                       (when-let [success-fn (::on-success tx-meta)]
                         (success-fn host datom-filter reply)))))))))

;; We'll assume fail-reversion should not happen if new edit has been made.
(defn sync-write
  "<datom-filter> is any predicate-function of datom.
  Provide <key> in config-map to avoid duplicated syncs.

  When transacting to the db, optionally include the tx-meta:
  :datomish.sync.client/on-failure (fn [host filter error-map] ...)
  :datomish.sync.client/on-success (fn [filter conn] ...)
  :datomish.sync.client/fail-reverse? <boolean>"
  ([conn dbc datom-filter] (sync-write conn dbc datom-filter {}))
  ([{:as conn :keys [write-reg host]}
    dbc datom-filter
    {:keys [initial-write? key]
     :or   {initial-write? true
            key            (gensym)}}]
   (if-let [{:keys [filters]} (get @write-reg dbc)]
     (swap! filters assoc key datom-filter)
     (let [listen-key (gensym)
           filters    (atom {key datom-filter})]
       (do (swap! write-reg assoc dbc {:filters filters :listen-key listen-key})
           (d/listen
            dbc listen-key
            (fn [{:as report :keys [tx-data db-before db-after tx-meta]}]
              ;; flag reverse-effect? to avoid sending client's reversing datoms.
              (when-not (or (::reverse-effect? tx-meta)
                            (::host-tick tx-meta)
                            (::dont-send tx-meta))
                (doseq [[key datom-filter] @filters]
                  (when-let [data
                             (->> tx-data
                                  (filter #(datom-filter db-after %))
                                  (filter meta-datom?)
                                  (map #(spit-host-datom db-after host %
                                                         (d/schema dbc)))
                                  (remove nil?)
                                  not-empty)]
                    (if-not (:open? @(:state conn))
                      (swap! (:pending-write conn)
                             conj #(write-and-handle!
                                    conn dbc report datom-filter data))
                      (write-and-handle!
                       conn dbc report datom-filter data))))))))))))

;; ---------------------------------------- ;;
;;;; Easy Sync

(defn sync
  "Sync a db with a host's db on a url.
  Push and pull all datoms by default."
  [dbc url {:keys [csrf filter-id write-filter sente-config]
            :or   {filter-id    :any
                   write-filter (constantly true)}}]
  (let [conn (connect url csrf {:sente-config sente-config})]
    (add-watch (:state conn) :sync
               (fn [key ref old {o? :open?}]
                 (when o?
                   (sync-read conn dbc filter-id)
                   (sync-write conn dbc write-filter)
                   (remove-watch ref key))))
    ;; we'll opt for one sync per dbc for now.
    (alter-meta! dbc #(merge % {::conn conn}))
    conn))

(defn close!
  "Terminate a connection."
  [conn]
  (some-> conn :chsk sente/chsk-disconnect!))
