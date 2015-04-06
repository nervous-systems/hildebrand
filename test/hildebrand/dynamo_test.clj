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
(def del-item (partial issue!! :delete-item))

(def table :hildebrand-test-table)
(def create-table-default
  {:table table
   :throughput {:read 1 :write 1}
   :attrs {:name :S}
   :keys  {:name :hash}})

(defn with-tables* [specs f]
  (doseq [{:keys [table] :as spec} specs]
    (when (empty? (issue!! :describe-table {:table table}))
      (issue!! :create-table spec)
      (await-status!! table :active)))
  (f))

(defmacro with-tables [specs & body]
  `(with-tables* ~specs
     (fn [] ~@body)))

(defn with-items* [specs f]
  (with-tables* (keys specs)
    (fn []
      (doseq [[{:keys [table]} items] specs]
        ;; batches, rather
        (doseq [item items]
          (put-item {:table table :item item})))
      (f))))

(defmacro with-items [specs & body]
  `(with-items* ~specs
     (fn [] ~@body)))

(def item {:name "Mephistopheles"})

(deftest put+get
  (with-tables [create-table-default]
    (is (empty? (put-item {:table table :item (assoc item :age 33)})))
    (is (= 33
           (:age (get-item
                  {:table table
                   :key item
                   :consistent true}))))))

(deftest delete
  (with-items {create-table-default [item]}
    (is (empty? (del-item {:table table :key item})))))

(deftest delete-cc
  (with-items {create-table-default [item]}
    (is (= table
           (->
            (del-item {:table table :key item :capacity :total})
            :capacity
            :table)))))
