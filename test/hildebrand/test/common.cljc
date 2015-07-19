(ns hildebrand.test.common
  (:require [hildebrand.core :as h]
            [plumbing.core :refer [map-keys]]
            #?@ (:clj
                 [[glossop.core :refer [<? <?! go-catching]]
                  [clojure.core.async :as async]]
                 :cljs
                 [[cljs.core.async :as async]]))
  #? (:cljs
      (:require-macros [glossop.macros :refer [go-catching <?]])))

(defn env [s & [default]]
  #? (:clj
      (get (System/getenv) s default)
      :cljs
      (or (aget js/process "env" s) default)))

(def creds
  {:access-key (env "AWS_ACCESS_KEY")
   :secret-key (env "AWS_SECRET_KEY")})

(def local-dynamo-url (env "LOCAL_DYNAMO_URL"))

(defn with-items!
  ([specs f]
   (with-items! creds specs f))
  ([creds specs f]
   (go-catching
     (let [table-name->keys (map-keys :table specs)]
       (<? (h/batch-write-item! creds {:put table-name->keys}))
       (try
         (<? (f))
         (finally
           (<? (h/batch-write-item!
                creds {:delete
                       (into {}
                         (for [[{:keys [keys table]} items] specs]
                           [table (map #(select-keys % keys) items)]))}))))))))

(def table :hildebrand-test-table)

(def create-table-default
  {:table table
   :throughput {:read 1 :write 1}
   :attrs {:name :string}
   :keys  [:name]})

(def indexed-table :hildebrand-test-table-indexed)
(def local-index   :hildebrand-test-table-indexed-local)
(def global-index  :hildebrand-test-table-indexed-global)
(def create-global-index
  {:name global-index
   :keys [:game-title :timestamp]
   :project [:keys-only]
   :throughput {:read 1 :write 1}})

(def create-table-indexed
  {:table indexed-table
   :throughput {:read 1 :write 1}
   :attrs {:user-id :string :game-title :string :timestamp :number}
   :keys  [:user-id :game-title]
   :indexes
   {:local
    [{:name local-index
      :keys [:user-id :timestamp]
      :project [:include [:data]]}]
    :global
    [create-global-index]}})

;; The tests could just use one table.  This is only really a burden with the
;; few tests which use Dynamo remotely (due to expectations about capacity
;; consumption being returned, which doesn't happen with local Dynamo).  This is
;; super slow remotely.
(defn reset-tables! [creds tables]
  (go-catching
    (try
      (<? (glossop.util/into []
            (async/merge (for [{:keys [table]} tables]
                           (h/delete-table! creds table)))))
      (catch #? (:clj Exception :cljs js/Error) _))
    (<? (glossop.util/into []
          (async/merge (for [{:keys [table]} tables]
                         (h/await-status! creds table nil)))))
    (<? (glossop.util/into []
          (async/merge (for [create tables]
                         (h/ensure-table! creds create)))))))

(def default-tables [create-table-default create-table-indexed])

(defn with-local-dynamo!
  ([f] (with-local-dynamo! default-tables f))
  ([tables+items f]
   (go-catching
     (if-let [url (not-empty local-dynamo-url)]
       (let [tables (cond-> tables+items (map? tables+items) keys)
             creds (assoc creds :endpoint url)]
         (<? (reset-tables! creds tables))
         (if (map? tables+items)
           (<? (with-items! creds tables+items (partial f creds)))
           (<? (f creds))))
       (println "Warning: Skipping local test due to unset LOCAL_DYNAMO_URL")))))

(defn with-remote-dynamo!
  ([f] (with-remote-dynamo! default-tables f))
  ([tables+items f]
   (go-catching
     (if (not-empty (:secret-key creds))
       (let [tables (cond-> tables+items (map? tables+items) keys)]
         (<? (reset-tables! creds tables))
         (if (map? tables+items)
           (<? (with-items! creds tables+items (partial f creds)))
           (<? (f creds))))
       (println "Warning: Skipping remote test due to unset AWS_SECRET_KEY")))))

