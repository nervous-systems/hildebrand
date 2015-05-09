(ns hildebrand.request-test
  (:require [clojure.test :refer :all]
            [hildebrand.request :refer :all]))

(def gs-index-out
  {:index-name :users-by-id
   :key-schema [{:attribute-name "id" :key-type :hash}]
   :projection {:projection-type :include
                :non-key-attributes [:x :y]}
   :provisioned-throughput
   {:read-capacity-units 1 :write-capacity-units 5}})

(def gs-index-in
  {:name :users-by-id
   :keys [:id]
   :project [:include [:x :y]]
   :throughput {:read 1 :write 5}})

(deftest create-table+
  (is (= {:table-name :users
          :attribute-definitions
          [{:attribute-name "email" :attribute-type :S}
           {:attribute-name "id" :attribute-type :S}
           {:attribute-name "timestamp" :attribute-type :N}]
          :key-schema [{:attribute-name "email" :key-type :hash}]
          :global-secondary-indexes
          [gs-index-out]
          :local-secondary-indexes
          [{:index-name :users-by-timestamp
            :key-schema
            [{:attribute-name "email" :key-type :hash}
             {:attribute-name "timestamp" :key-type :range}]
            :projection {:projection-type :keys-only}}]
          :provisioned-throughput {:read-capacity-units 5
                                   :write-capacity-units 1}}
         (restructure-request
          :create-table
          {:table :users
           :attrs {:id :string :email :S :timestamp :number}
           :keys [:email]
           :indexes
           {:global [gs-index-in]
            :local [{:name :users-by-timestamp
                     :keys [:email :timestamp]
                     :project [:keys-only]}]}
           :throughput {:read 5 :write 1}}))))

(deftest batch-write-item+
  (is (= {:return-consumed-capacity true
          :request-items
          {:table-one [{:delete-request {:key  {"user" {:N "1"}}}}]
           :table-two [{:put-request    {:item {"user" {:N "2"}}}}]}}
         (restructure-request
          :batch-write-item
          {:capacity true
           :delete {:table-one [{:user 1}]}
           :put    {:table-two [{:user 2}]}}))))

(deftest batch-get-item+
  (is (= {:request-items
          {:table-one {:consistent-read true
                        :expression-attribute-names {:#a "a"}
                        :projection-expression [:#a]
                        :keys [{"user" {:N "1"}}]}}}
         (restructure-request
          :batch-get-item
          {:items {:table-one
                   {:consistent true
                    :keys [{:user 1}]
                    :attrs [:#a]}}}))))


;; Don't test any expression-related stuff, since we're not in a
;; position to verify it without getting awkwardly specific.

(deftest query+
  (is (= {:table-name :users
          :key-conditions
          {:user-id
           {:attribute-value-list [{:N "5"}]
            :comparison-operator :eq}}
          :projection-expression [:#name :age]
          :expression-attribute-names {:#name "name"}
          :scan-index-forward true
          :consistent-read true}
         (restructure-request
          :query
          {:table :users
           :where {:user-id [:eq 5]}
           :attrs  [:#name :age]
           :consistent true
           :sort :asc}))))

(deftest delete-item+
  (is (= {:table-name :users
          :key {"x" {:S "y"}}
          :return-values :all-old}
         (restructure-request
          :delete-item
          {:table :users :key {:x "y"} :return :all-old}))))

(deftest update-item+
  (is (= {:table-name :users
          :return-values :all-new
          :key {"user-id" {:S "Moe"}}
          :return-item-collection-metrics true}
         (restructure-request
          :update-item
          {:table :users
           :key {:user-id "Moe"}
           :return :all-new
           :metrics true}))))

(deftest get-item+
  (is (= {:table-name :users
          :key {"foo" {:S "bar"}}
          :consistent-read true}
         (restructure-request
          :get-item
          {:table :users
           :key {:foo "bar"}
           :consistent true}))))

(deftest update-table+
  (is (= {:table-name :users
          :attribute-definitions [{:attribute-name "x"
                                   :attribute-type :S}]
          :global-secondary-index-updates
          #{{:create gs-index-out}
            {:delete {:index-name :amazing-index}}
            {:update {:index-name :other-index
                      :provisioned-throughput
                      {:read-capacity-units 1
                       :write-capacity-units 1}}}}
          :provisioned-throughput
          {:read-capacity-units 5 :write-capacity-units 3}}
         (-> (restructure-request
              :update-table
              {:attrs {:x :string}
               :table :users
               :indexes {:create [gs-index-in]
                         :delete [{:name :amazing-index}]
                         :update [{:name :other-index
                                   :throughput {:read 1 :write 1}}]}
               :throughput {:read 5 :write 3}})
             (update-in [:global-secondary-index-updates] (partial into #{}))))))

(deftest describe-table+
  (is (= {:table-name :users}
         (restructure-request
          :describe-table
          {:table :users}))))
