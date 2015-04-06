(ns hildebrand.dynamo-test
  (:require [hildebrand.dynamo]
            [hildebrand.util :refer :all]
            [glossop :refer [<?! <? go-catching]]
            [eulalie]
            [eulalie.dynamo]
            [clojure.test :refer :all]
            [clojure.core.async :as async]))

(def creds
  {:access-key (get (System/getenv) "AWS_ACCESS_KEY")
   :secret-key (get (System/getenv) "AWS_SECRET_KEY")})

(defn issue! [target content]
  (go-catching
    (branch->
      (<? (hildebrand.dynamo/issue-request!
           creds
           {:target target :max-retries 0 :body content}))
      :error (-> :error pr-str Exception. throw))))

;; move this into the project proper
(defn await-status! [table status]
  (go-catching
    (loop []
      (let [status' (-> (issue! :describe-table {:table table}) <? :status)]
        (cond (nil? status')     nil
              (= status status') status'
              :else (do
                      (<? (async/timeout 1000))
                      (recur)))))))

(def issue!!        (comp <?! issue!))
(def await-status!! (comp <?! await-status!))

(def put-item (partial issue!! :put-item))
(def get-item (partial issue!! :get-item))

(def table :hildebrand-test-table)
(def create-table-default
  {:table table
   :throughput {:read 1 :write 1}
   :attrs {:name :S}
   :keys  {:name :hash}})

(defn with-tables [specs f]
  (doseq [{:keys [table] :as spec} specs]
    (when (empty? (issue!! :describe-table {:table table}))
      (issue!! :create-table spec)
      (await-status!! table :active)))
  (f))

(deftest put+get
  (with-tables [create-table-default]
    (fn []
      (put-item {:table table :item {:name "Mephistopheles" :age 33}})
      (is (= 33
             (:age (get-item
                    {:table table :key {:name "Mephistopheles"}})))))))
