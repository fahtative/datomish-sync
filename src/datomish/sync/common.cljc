(ns datomish.sync.common)

(defn meta-datom?
  "Test wheter a datom is a meta datom,
  i.e. datom that facilitates this library or
  is part of the schema."
  [[_ attr v]]
  (let [[ns n] ((juxt namespace name) attr)]
    (cond (and (= ns "db") (= n "ident")
               (and (keyword? v)
                    (not (re-find #"db" (namespace v)))))
          true
          :else (-> ns
                    #{"db"
                      ;; "common.datomish.sync.client"
                      ;; "common.datomish.sync.host"
                      "datomish.sync.host-datom"
                      "datomish.sync.sync-filter"}
                    not))))
