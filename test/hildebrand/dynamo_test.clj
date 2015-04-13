(ns hildebrand.dynamo-test
  (:require
   [hildebrand.dynamo.expr :refer [let-expr]]
   [hildebrand.dynamo]
   [slingshot.slingshot :refer [throw+ try+]]
   [hildebrand.util :refer :all]
   [glossop :refer [<?! <? go-catching]]
   [eulalie]
   [eulalie.dynamo]
   [clojure.test :refer :all]
   [plumbing.core :refer :all]
   [clojure.core.async :as async]))

(def creds
  {:access-key (get (System/getenv) "AWS_ACCESS_KEY")
   :secret-key (get (System/getenv) "AWS_SECRET_KEY")})

(defn issue! [target content & [{:keys [throw] :or {throw true}}]]
  (go-catching
    (let [{:keys [hildebrand/error] :as resp}
          (<? (hildebrand.dynamo/issue-request!
               creds
               {:target target :max-retries 0 :body content}))]
      (if (and throw error)
        (throw+ error)
        resp))))

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
(def update-item (partial issue!! :update-item))

(def table :hildebrand-test-table)
(def create-table-default
  {:table table
   :throughput {:read 1 :write 1}
   :attrs {:name :S}
   :keys  {:name :hash}})

(defn with-tables* [specs f]
  (doseq [{:keys [table] :as spec} specs]
    (let [{:keys [hildebrand/error]} (issue!! :describe-table {:table table} {:throw false})]
      (cond (= (:type error) :resource-not-found-exception)
            (do
              (issue!! :create-table spec)
              (await-status!! table :active))
            error (throw+ error)
            :else nil)))
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

(deftest list-tables
  (with-tables [create-table-default
                (assoc create-table-default
                       :table :hildebrand-test-table-list-tables)]
    (let [tables (issue!! :list-tables {:limit 1})]
      (is (= 1 (count tables)))
      (is (-> tables meta :start-table)))))

(deftest put+get
  (with-tables [create-table-default]
    (is (empty? (put-item {:table table :item (assoc item :age 33)})))
    (is (= 33
           (:age (get-item
                  {:table table
                   :key item
                   :consistent true}))))))

(deftest put-returning
  (with-tables [create-table-default]
    (let [item' (assoc item :old "put-returning")]
      (is (empty?  (put-item {:table table :item item'})))
      (is (= item' (put-item {:table table :item item :return :all-old}))))))

(deftest put-meta
  (with-tables [create-table-default]
    (let [item (put-item {:table table :item item :capacity :total})]
      (is (empty? item))
      (is (= table (-> item meta :capacity :table))))))

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

(deftest delete-expected-expr
  (with-items {create-table-default [(assoc item :age 33 :hobby "Strolling")]}
    (is (empty?
         (del-item
          {:table table :key item :condition
           {:values {:min 30 :max 34 :prefix1 "St" :prefix2 "Tro"}
            :expr   '(and (<= :min age)
                          (<= age :max)
                          (or (begins-with hobby :prefix1)
                              (begins-with hobby :prefix2)))}})))))

(deftest delete-expected-expr-neg
  (with-items {create-table-default [(assoc item :age 33)]}
    (is (= :conditional-check-failed-exception
           (-> (del-item
                {:table table :key item :condition
                 {:values {:min1 10 :max1 30 :min2 33 :max2 40}
                  :expr   '(and (or
                                 (between age :min1 :max1)
                                 (between age :min2 :max2))
                                (exists garbage))}}
                {:throw false})
               :hildebrand/error
               :type)))))

(defn update-test [attrs-in updates attrs-out]
  (let [keyed-item (merge item attrs-in)
        expected   (merge item attrs-out)]
    (with-items {create-table-default [keyed-item]}
      (is (= expected
             (update-item
              {:table table :key item :update updates :return :all-new}))))))

(deftest update-item-only
  (update-test
   {:hobbies #{"eating" "sleeping"}
    :bad     "good"}
   {:nick    [:set "Rodrigo"]
    :hobbies [:add #{"dreaming"}]
    :bad     [:remove]}
   {:nick    "Rodrigo"
    :hobbies #{"eating" "sleeping" "dreaming"}}))

(deftest update-item-list
  (update-test
   {:x [1 2 3]}
   {:x [:append ["4"]]}
   {:x [1 2 3 "4"]}))

(deftest update-item-concat
  (update-test
   {:x [1] :y #{1}}
   {:x [:concat [2]] :y [:concat #{2}]}
   {:x [1 2] :y #{1 2}}))

(deftest update-item-init
  (update-test
   {:y 5}
   {:x [:init 6] :y [:init 1]}
   {:y 5 :x 6}))

(deftest update-item-remove
  (update-test
   {:y 5 :z 5}
   {:y [:remove] :z [:remove]}
   {}))

(deftest update-item-nested-set
  (update-test
   {:a [:b]}
   {:a {0 [:set "c"]}}
   {:a ["c"]}))

(deftest update-item-deeply-nested-set
  (update-test
   {:a ["b" "c" {:d ["e"]}]}
   {:a {2 {:d {0 [:set "f"]}}}}
   {:a ["b" "c" {:d ["f"]}]}))

(deftest update-item-nested-init
  (update-test
   {:a {:b "c"}}
   {:a {:b [:init "d"] :B [:init "D"]}}
   {:a {:b "c" :B "D"}}))

(deftest update-item-nested-append
  (update-test
   {:a {:b [["c"]]}}
   {:a {:b {0 [:concat ["d"]]}}}
   {:a {:b [["c" "d"]]}}))

(deftest update-item-nested-remove
  (update-test
   {:a {:b [{:c :d}]}}
   {:a {:b {0 [:remove]}}}
   {:a {:b []}}))

(def items
  (for [i (range 5)]
    (assoc item :name (str "batch-write-" i))))

(deftest batch-write
  (is (empty? (issue!! :batch-write-item {:put {table items}}))))

(deftest batch-write+get
  (issue!! :batch-write-item {:put {table items}})
  (let [responses (issue!!
                   :batch-get-item
                   {table {:consistent true :keys items}})]
    (is (= (into #{} items) (into #{} (responses table))))))
