(ns hildebrand.test-util
  (:require [hildebrand :as h]
            [plumbing.core :refer [map-keys]]))

(def creds
  {:access-key (get (System/getenv) "AWS_ACCESS_KEY")
   :secret-key (get (System/getenv) "AWS_SECRET_KEY")})

(defn with-tables* [specs f]
  (doseq [{:keys [table] :as spec} specs]
    (h/ensure-table!! creds spec))
  (f))

(defmacro with-tables [specs & body]
  `(with-tables* ~specs
     (fn [] ~@body)))

(defn with-items* [specs f]
  (let [table-name->keys (map-keys :table specs)]
    (with-tables* (keys specs)
      (fn []
        (h/batch-write-item!! creds {:put table-name->keys})
        (f)))))

(defmacro with-items [specs & body]
  `(with-items* ~specs
     (fn [] ~@body)))

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
