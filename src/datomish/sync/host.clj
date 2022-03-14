(ns datomish.sync.host
  (:require [datomish.api :as d]
            [taoensso.sente :as sente]
            [taoensso.timbre :as log]
            [clojure.set :as set]
            [slingshot.slingshot :refer [try+]]
            [clojure.core.async :as a]
            [clojure.edn :as edn]
            [datomish.sync.common :refer [meta-datom?]]
            ))

;; ------------------------------------------
;;;; Utils

(defn map-or-nil
  "Return nil if x is not a map."
  [x]
  (when (map? x) x))

(defn edn-readble?
  "Test whether x would be readable as edn."
  [x]
  (try+ (edn/read-string (pr-str x)) true
        (catch Object e false)))

(defn datom->list-tx
  "Make a single list-transaction from a given datom."
  [[e a v t a?]]
  [(if a? :db/add :db/retract) e a v])

(defn datom->seq
  "Return datom as a seq.
  The given datom should implement Iterable."
  [d]
  (seq d))

;; ------------------------------------------
;;;; Client Message handler

(defmulti handle-message
  "Handle remote client's message."
  :id)
(defmethod handle-message :default [_] nil)

(defn conn->message-handler
  "Make a message handling function from a connector."
  [conn]
  (fn [msg]
    (let [context (-> msg
                      (set/rename-keys {:?data :params
                                        :?reply-fn :reply})
                      (merge conn))]
      (handle-message context))))

;; ------------------------------------------
;;;; Request to Write Receive

(defn try-write
  "Try writing deltas to the database.

  (<write-auth> <uid> <db> <datoms>)  => true | <map>
  A map returned-value indicates a failure.

  Upon failure, return the map merged with:
  {:datomish.sync.host/error?       true
   :datomish.sync.host/input-deltas <deltas>}"
  [dbc deltas write-auth uid]
  (if-let [error (map-or-nil (write-auth uid @dbc deltas))]
    ;; single auth fail means complete write failure.
    (assoc error ::error? true ::input-deltas deltas)
    (try+ (let [report (d/transact dbc
                         (map datom->list-tx deltas)
                         {::writer uid})]
            (-> report
                (select-keys [:tempids :tx-data])
                (update :tx-data #(map datom->seq %))))
          (catch Object e
            (assoc e ::error? true ::input-deltas deltas)))))

(defmethod handle-message :client/write
  [{:keys [dbc params reply write-auth uid]}]
  (let [datoms params db @dbc]
    ;; the client should look for
    ;; :datomish.sync.host/error? key to identify error map.
    (a/thread (reply (try-write dbc datoms write-auth uid)))))

;; ------------------------------------------
;;;; Subscription

;; <subscription-registers> := {<dfilter-identifier>
;;                               {:uids #{<uid> ...}
;;                                :dfilter-fn <datoms-predicate-function>}
;;                              ... ...}
;; <dfilter> := [<dfilter-identifier> <datoms-predicate-function>]
;; <dfilter-identifier> := <keyword> | <quoted-list>

(defn make-default-sub-regs []
  {:any {:dfilter-fn (constantly true)}})

(defn filter-pre-send
  "Filter out-going datoms.
  Remove syncing-helper datoms,
  and convert edn-non-conformant values/objects to strings."
  [datoms]
  (->> datoms
       ;; (filter domain-datom?)
       (map (fn [[e a v t add?]]
              (let [v (if-not (edn-readble? v)
                        (pr-str v) v)]
                [e a v t add?])))
       (remove (fn [[e a v]] (nil? v)))))

(defn subscribe
  "Register new uid for a given dfilter."
  [sub-regs dfilter-id uid]
  (update-in sub-regs [dfilter-id :uids]
             (fnil conj #{}) uid))

;; Handle client subscription:
;; - register uid
;; - reply with initial load if requested
;; Or reply with error-map when:
;; - no matching requested filter-id.
;; - pre-read authentication fail
;; - post-read authentication fail (for the intial load,
;; but still register uid for update)

;; I.e. various read auths are semipredicates, with maps indicating failure.
;; (<pre-read-auth>  <uid> <db> <dfilter-id>)  => true | <info-map>
;; (<post-read-auth> <uid> <db> <datoms>)  => true | <info-map>
;; Pre-read auth rejects a uid subscription based on uid and dfilter-id,
;; while post-read auth aborts a particular update sending
;; based on uid and the updating datoms.

(defmethod handle-message :client/subscribe
  [{:as ctx :keys
    [dbc db-history?
     params subscriptions uid reply
     pre-read-auth post-read-auth]}]
  (let [{:keys [reply-with-load? last-sync-tx]} params
        db (d/datoms @dbc :eavt)
        dfilter-id (:filter params)
        dfilter (get @subscriptions dfilter-id)]
    (if-not dfilter
      (reply {:filter dfilter-id
              :reason "Requested filter not found."})
      (if-let [error (map-or-nil (pre-read-auth uid db dfilter-id))]
        (reply (assoc error :filter-id dfilter
                      ::error? true))
        (do (swap! subscriptions subscribe dfilter-id uid)
            (if-not reply-with-load?
              (reply [])
              (let [loads (->> db
                               (filter (:dfilter-fn dfilter))
                               #_(if (and last-sync-tx db-history?)
                                   (difference-datoms db dfilter last-sync-tx)
                                   (filter (eval dfilter) db))
                               filter-pre-send)]
                (if-let [error (map-or-nil (post-read-auth uid db loads))]
                  (reply (assoc error :filter dfilter
                                ::error? true))
                  (reply (map seq loads))))))))))

(defn unsubscribe
  "Unsubscribe a uid from a dfilter-id."
  [sub-regs dfilter-id uid]
  (update-in sub-regs [dfilter-id :uids] disj uid))

(defmethod handle-message :client/unsubscribe
  [{:as ctx :keys [params subscriptions uid]}]
  (let [{dfilter-id :filter} params]
    (swap! subscriptions unsubscribe dfilter-id uid)))

(defn start-subscription-loop!
  "Start a datoms broadcaster,
  sending initial loads and listening for changes to be submitted
  to each registering clients for each dfilter.

  Each update has the form: [:host/tick <data>],
  where <data> is either:
  {:data <datoms> :dfilter dfilter-id}
  or
  {:datom.sync.host/error? true ... ...} indicating a failure."
  [{:as conn :keys [dbc post-read-auth subscriptions connections]}
   send-fn key]
  (d/listen
   dbc key
   (fn [{:keys [tx-data db-after tx-meta]}]
     (a/thread
       (when-not (::dont-send tx-meta)
         (let [alive-con (:any @connections)]
           (doall ;; forces pmap
            (->>
             @subscriptions
             (pmap
              (fn [[dfilter-id {:keys [dfilter-fn uids]}]]
                (when-let
                    [datoms (not-empty (->> tx-data
                                            (filter dfilter-fn)
                                            filter-pre-send))]
                  (doseq [uid
                          (->> uids
                               (filter #(not= (::writer tx-meta) %)))]
                    ;; Dead subscription is cleared when we're trying to push to it.
                    (if-not (alive-con uid)
                      ;; Possibility: Check all dfilters to find dead client.
                      (swap! subscriptions unsubscribe dfilter-id uid)
                      (if-let [error (map-or-nil
                                      (post-read-auth uid db-after datoms))]
                        (send-fn uid
                                 [:host/tick
                                  (assoc error
                                         ::error? true
                                         ::dfilter dfilter-id)])
                        (do (send-fn uid
                                     [:host/tick
                                      {:dfilter dfilter-id
                                       :data (map seq datoms)}]))))))))))))))))

;; A client can 'ping' to get all datoms of dfilters it is registering to.
(defmethod handle-message :client/tick-ping
  [{:as ctx :keys [uid params subscriptions dbc post-read-auth send-fn]}]
  (let [dfilter (:dfilter params)]
    (when-let [m (get @subscriptions dfilter)]
      (let [{:keys [uids filter-fn]} m]
        (when (get uids uid)
          (let [db (d/datoms @dbc :eavt)]
            (when-let [datoms (not-empty
                               (->> db
                                    (filter filter-fn)
                                    filter-pre-send))]
              (if-let [error (map-or-nil (post-read-auth uid db datoms))]
                (send-fn uid [:host/tick
                              (assoc error ::error? true ::input-dfilter dfilter)])
                (send-fn uid [:host/tick
                              {:dfilter dfilter
                               :tx-data (map seq datoms)}])))))))))
;; ------------------------------------------
;;;; Base Write Auth

(defn matching-latest-datom
  "Find latest datom matching given [<eid> <attr>]."
  [[entity attr] db]
  (d/q '[:find [?e ?a ?v ?t ?b]
         :in $ ?e ?a
         :where [?e ?a ?v ?t ?b]]
       db entity attr))

(defn datom-tx
  "Get tx-id of a datom."
  [datom] (nth 3))

(defn tx-match-last-tx?
  "Test whether a tx's tx-id matches the latest similar tx of a db.
  Two txs are similar if their eids and attrs matched."
  [datom db]
  (= (some->> db (matching-latest-datom datom) datom-tx)
     (datom-tx datom)))

(defn only-matching-last-tx
  "Write-auth wrapper which allows writing only if a client
  has the matching last tx-id about the target datoms."
  [write-auth]
  (fn [uid db datoms]
    (if (every? #(or #_(same-author-as-last-tx? uid % db)
                     (tx-match-last-tx? db %))
                datoms)
      (write-auth uid db datoms)
      {:status :fail
       :reason "Host has different tx of a datom than client's last sync."
       ;; :data {:host-tx (host-last-tx) :client-last-tx client-last-tx :datom datom}
       })))

;; ------------------------------------------
;;;; Connector

(defn sente->ring-handler
  "Make a ring-handler from a sente channel socket."
  [{:as sente :keys [ajax-post-fn ajax-get-or-ws-handshake-fn]}]
  (fn [{:as req :keys [request-method]}]
    (case request-method
      :post (ajax-post-fn req)
      :get  (ajax-get-or-ws-handshake-fn req)
      (throw (ex-info "Request Method Not Supported" {:method request-method})))))

(defn ring-req->client+uid
  [req] [(-> req :session :uid) (:client-id req)])

(def default-write-auth
  "Extensible (by wrapping) default write-auth."
  (-> (constantly true) only-matching-last-tx))

(def default-connector-config
  {:pre-read-auth  (constantly true)
   :post-read-auth (constantly true)
   :write-auth     default-write-auth
   :conn-auth      (constantly true)
   :uid-fn         ring-req->client+uid
   :db-history?    false ; IDEA: infer from the db-type.
   })

(defn connector
  "Make a connector: a Sente's host ring-handler.
  Config requires Sente-adapter, matching intended http-server.

  <dbc> refers to database connection.
  If :db-history? is false, the host will not attempt
  to compute transaction difference from <last-sync-tx>
  subscribing parameter.

  <write-auth> and <post-read-auth> are functions of uid, db, and datoms:
  return true or a map representing error message.
  <pre-read-auth> returns the same thing but takes uid, db, and <dfilter>.

  <conn-auth> and <uid-fn> are functions of <ring-req>. See Sente.
  "
  ([dbc] (connector dbc {}))
  ([dbc {:as config}]
   (let [{:as   config
          :keys [write-auth pre-read-auth
                 post-read-auth conn-auth uid-fn db-history?
                 sente-adapter sente-config]}
         ;; Using :or to supply default values
         ;; would leave config-map as provided.
         ;; Since we want config-map to include default values,
         ;; we'll merge it ourselves.
         (merge default-connector-config config)
         {:as sente :keys [ch-recv connected-uids send-fn]}
         (sente/make-channel-socket! sente-adapter
                                     (merge
                                      {:authorized?-fn conn-auth
                                       :user-id-fn     uid-fn}
                                      sente-config))
         listen-key (gensym "datomish.sync.host#")
         conn       (merge {:connections   connected-uids
                            :subscriptions (atom (make-default-sub-regs))
                            :dbc           dbc
                            :db-listen-key listen-key
                            :sente         sente}
                           config)]
     (let [stop-fn (sente/start-chsk-router! ch-recv (conn->message-handler conn))]
       (start-subscription-loop! conn send-fn listen-key)
       (with-meta (sente->ring-handler sente)
         (assoc conn :router-stop-fn stop-fn))))))

(defn kill!
  "Stop a connector."
  [connector]
  (let [{:keys [dbc db-listen-key
                router-stop-fn]}
        (meta connector)]
    (try+ (do (d/unlisten dbc db-listen-key)
              (router-stop-fn))
          (catch Object e nil))))
