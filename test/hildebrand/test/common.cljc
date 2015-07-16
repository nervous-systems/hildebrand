(ns hildebrand.test.common
  (:require [hildebrand.core :as h]
            [plumbing.core :refer [map-keys]]
            #? (:clj
                [glossop.core :refer [<? <?! go-catching]]
                :cljs
                [cljs.core.async]))
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

(defn with-items! [specs f]
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
                          [table (map #(select-keys % keys) items)]))})))))))

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
