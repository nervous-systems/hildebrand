(ns hildebrand.test.internal.response
  (:require #? (:clj
                [clojure.test :refer [deftest is]]
                :cljs
                [cljs.test :refer-macros [deftest is]])
            [hildebrand.internal.response :refer [restructure-response]]))

(def table-description-out
  {:table-description
   {:table-name :users
    :provisioned-throughput {:read 5 :write 1}
    :key-schema [{:attribute-name :age  :key-type :range}
                 {:attribute-name :name :key-type :hash}]
    :attribute-definitions
    [{:attribute-name :name :attribute-type :S}
     {:attribute-name :age  :attribute-type :N}]
    :global-secondary-indexes
    [{:index-name :gs-index
      :key-schema [{:attribute-name :age :key-type :hash}
                   {:attribute-name :something :key-type :range}]
      :projection {:projection-type :include
                   :non-key-attributes [:something-else]}
      :provisioned-throughput {:read 1 :write 1}}]}})

(def table-description-in
  {:table :users
   :attrs {:name :string :age :number}
   :keys [:name :age]
   :throughput {:read 5 :write 1}
   :indexes
   {:global [{:name :gs-index
              :keys [:age :something]
              :project [:include [:something-else]]
              :throughput {:read 1 :write 1}}]}})

(deftest create-table+
  (is (= table-description-in
         (restructure-response
          :create-table table-description-out))))

(deftest update-table+
  (is (= table-description-in
         (restructure-response
          :update-table table-description-out))))

(deftest get-item
  (is (= {:name "Joseph"}
         (restructure-response
          :get-item {:item {:name {:S "Joseph"}}}))))


(deftest batch-get-item
  (is (= {:hildebrand-test-table [{:name "Moe"}]}
         (restructure-response
          :batch-get-item
          {:responses {:hildebrand-test-table [{:name {:S "Moe"}}]}
           :unprocessed-keys {}}))))

(deftest query
  (is (= [{:name "Mephistopheles"}]
         (restructure-response
          :query
          {:count 1
           :items [{:name {:S "Mephistopheles"}}]
           :scanned-count 1}))))
