(ns hildebrand.test.channeled
  (:require  #?@ (:clj
                  [[clojure.core.async :as async :refer [alt!]]
                   [glossop.core :refer [go-catching <?]]
                   [clojure.test :refer [is]]
                   [hildebrand.test.async :refer [deftest]]]
                  :cljs
                  [[cemerick.cljs.test]
                   [cljs.core.async :as async]])
             [hildebrand.test.common :as test.common :refer
              [create-table-indexed indexed-table with-local-dynamo! greedy-paginate!]]
             [hildebrand.channeled :as channeled]
             [hildebrand.core :as h])
  #? (:cljs (:require-macros [glossop.macros :refer [<? go-catching]]
                             [hildebrand.test.async.macros :refer [deftest]]
                             [cljs.core.async.macros :refer [alt!]]
                             [cemerick.cljs.test :refer [is]])))

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

(deftest list-tables
  (with-local-dynamo!
    (fn [creds]
      (go-catching
        (is (< 1 (count
                  (<? (greedy-paginate!
                       channeled/list-tables!
                       creds
                       {:limit 1})))))))))

;; These are more examples than tests

(deftest batching-puts
  (with-local-dynamo! [create-table-indexed]
    (fn [creds]
      (go-catching
        (let [{:keys [error-chan]}
              (channeled/batching-puts
               creds
               indexed-table
               {:in-chan
                (async/to-chan
                 [{:user-id "batching-puts" :game-title "1"}
                  {:user-id "batching-puts" :game-title "2"}])})]
          (alt!
            (async/timeout 1000) (is false "Timeout!")
            error-chan
            ([v]
             (is (nil? v))
             (is (= 2 (<? (h/query-count!
                           creds indexed-table
                           {:user-id [:= "batching-puts"]})))))))))))

(deftest batching-deletes
  (with-local-dynamo! {create-table-indexed
                       [{:user-id "moea" :game-title "Super Mario"}
                        {:user-id "moea" :game-title "Super Metroid"}
                        {:user-id "moea" :game-title "Craps"}]}
    (fn [creds]
      (let [query-chan (channeled/query!
                        creds
                        indexed-table
                        {:user-id [:= "moea"]
                         :game-title [:begins-with "Super"]}
                        {:limit 10}
                        {:chan (async/chan 1 (map (fn [x] [indexed-table x])))})
            {delete-chan :in-chan
             error-chan :error-chan}
            (channeled/batching-deletes creds)]
        (go-catching
          (async/pipe query-chan delete-chan true)
          (alt!
            (async/timeout 1000) (is false "Timeout!")
            error-chan
            ([v]
             (is (nil? v))
             (is (= 1 (<? (h/query-count!
                           creds indexed-table {:user-id [:= "moea"]})))))))))))
