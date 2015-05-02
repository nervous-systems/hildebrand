(ns hildebrand.page-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :as async]
            [glossop :refer :all]
            [hildebrand.test-util :refer :all]
            [hildebrand.page :as page]))

(defn greedy-query!! [& args]
  (<?! (async/into [] (apply page/query! creds args))))

(deftest query+
  (let [items (for [i (range 5)]
                {:user-id "query+" :game-title (str i)})]
    (with-items {create-table-indexed items}
      (is (= items (greedy-query!!
                    indexed-table 
                    {:user-id [:eq "query+"]} {:limit 1}))))))

