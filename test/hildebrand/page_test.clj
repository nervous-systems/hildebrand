(ns hildebrand.page-test
  (:require
   [hildebrand :as h]
   [clojure.test :refer :all]
   [clojure.core.async :as async]
   [glossop :refer :all]
   [hildebrand.test-util :refer :all]
   [hildebrand.page :as page]))

(def chan->vec!! (fn->> (async/into []) async/<!! (mapv throw-err)))

(defn greedy-paginate!! [f & args]
  (chan->vec!! (apply f creds indexed-table args)))

(def greedy-query!! (partial greedy-paginate!! page/query!))

(def items (for [i (range 5)] {:user-id "page-test" :game-title (str i)}))

(deftest query+
  (with-items {create-table-indexed items}
    (is (= items (greedy-query!! {:user-id [:= "page-test"]}
                                 {:limit 1})))))

(deftest query+maximum
  (with-items {create-table-indexed items}
    (is (= (take 1 items)
           (greedy-query!!
            {:user-id [:= "page-test"]}
            {:limit 1}
            {:maximum 1})))))

(def greedy-scan!! (partial greedy-paginate!! page/scan!))

(deftest scan+
  (with-items {create-table-indexed items}
    (is (= items (greedy-scan!!
                  {:limit 1
                   :filter [:= [:user-id] "page-test"]})))))
