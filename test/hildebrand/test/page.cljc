(ns hildebrand.test.page
  (:require
   #?@ (:clj
        [[clojure.core.async :as async]
         [glossop.core :refer [go-catching <?]]
         [clojure.test :refer [is]]
         [hildebrand.test.async :refer [deftest]]]
        :cljs
        [[cemerick.cljs.test]
         [cljs.core.async :as async]])
   [hildebrand.test.common :as test.common :refer [creds with-items!]]
   [hildebrand.page :as page]
   [glossop.core :refer [throw-err]])
  #? (:cljs (:require-macros [glossop.macros :refer [<? go-catching]]
                             [hildebrand.test.async.macros :refer [deftest]]
                             [cemerick.cljs.test :refer [is]])))

(defn chan->vec! [ch]
  (go-catching
    (mapv throw-err (<? (async/into [] ch)))))

(defn greedy-paginate! [f & args]
  (chan->vec! (apply f creds test.common/indexed-table args)))

(def greedy-query! (partial greedy-paginate! page/query!))

(def items
  (for [i (range 5)]
    {:user-id "page-test" :game-title (str i)}))

(deftest query
  (with-items! {test.common/create-table-indexed items}
    #(go-catching
       (is (= items (<? (greedy-query!
                         {:user-id [:= "page-test"]}
                         {:limit 1})))))))

(deftest query+maximum
  (with-items! {test.common/create-table-indexed items}
    #(go-catching
       (is (= (take 1 items)
              (<? (greedy-query!
                   {:user-id [:= "page-test"]}
                   {:limit 1}
                   {:maximum 1})))))))

(def greedy-scan! (partial greedy-paginate! page/scan!))

(deftest scan
  (with-items! {test.common/create-table-indexed items}
    #(go-catching
       (is (= items (<? (greedy-scan!
                         {:limit 1
                          :filter [:= [:user-id] "page-test"]})))))))
