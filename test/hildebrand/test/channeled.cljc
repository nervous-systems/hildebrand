(ns hildebrand.test.channeled
  (:require
   [glossop.util]
   #?@ (:clj
        [[clojure.core.async :as async :refer [alt!]]
         [glossop.core :refer [go-catching <?]]
         [clojure.test :refer [is]]
         [hildebrand.test.async :refer [deftest]]]
        :cljs
        [[cemerick.cljs.test]
         [cljs.core.async :as async]])
   [hildebrand.test.common :as test.common :refer
    [create-table-indexed indexed-table with-local-dynamo!]]
   [hildebrand.channeled :as channeled]
   [hildebrand.core :as h])
  #? (:cljs (:require-macros [glossop.macros :refer [<? go-catching]]
                             [hildebrand.test.async.macros :refer [deftest]]
                             [cljs.core.async.macros :refer [alt!]]
                             [cemerick.cljs.test :refer [is]])))


(defn greedy-paginate! [f & args]
  (glossop.util/into [] (apply f args)))

(def greedy-query! (partial greedy-paginate! channeled/query!))

(def items
  (for [i (range 5)]
    {:user-id "page-test" :game-title (str i)}))

(deftest query
  (with-local-dynamo! {create-table-indexed items}
    (fn [creds]
      (go-catching
        (is (= items (<? (greedy-query!
                          creds
                          indexed-table
                          {:user-id [:= "page-test"]}
                          {:limit 1}))))))))

(deftest query+maximum
  (with-local-dynamo! {create-table-indexed items}
    (fn [creds]
      (go-catching
        (is (= (take 1 items)
               (<? (greedy-query!
                    creds
                    indexed-table
                    {:user-id [:= "page-test"]}
                    {:limit 1}
                    {:maximum 1}))))))))

(def greedy-scan! (partial greedy-paginate! channeled/scan!))

(deftest scan
  (with-local-dynamo! {create-table-indexed items}
    (fn [creds]
      (go-catching
        (is (= items (<? (greedy-scan!
                          creds
                          indexed-table
                          {:limit 1
                           :filter [:= [:user-id] "page-test"]}))))))))

(deftest batching-puts
  (with-local-dynamo! [create-table-indexed]
    (fn [creds]
      (go-catching
        (let [{:keys [error-chan]}
              (channeled/batching-puts
               {:creds creds
                :table indexed-table
                :batch-opts {:in-chan (async/to-chan
                                       [{:user-id "batching-puts" :game-title "1"}
                                        {:user-id "batching-puts" :game-title "2"}])}})]
          (alt!
            (async/timeout 1000) (is false "Timeout!")
            error-chan
            ([v]
             (is (nil? v) "Error?")
             (is (= 2 (<? (h/query-count!
                           creds indexed-table
                           {:user-id [:= "batching-puts"]})))))))))))
