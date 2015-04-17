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
   [clojure.core.async :as async]
   [clojure.walk :as walk]))

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

(def put-item       (partial issue!! :put-item))
(def get-item       (partial issue!! :get-item))
(def del-item       (partial issue!! :delete-item))
(def update-item    (partial issue!! :update-item))
(def batch-write    (partial issue!! :batch-write-item))
(def query          (partial issue!! :query))
(def describe-table (partial issue!! :describe-table))
(def update-table   (partial issue!! :update-table))

(def table :hildebrand-test-table)
(def create-table-default
  {:table table
   :throughput {:read 1 :write 1}
   :attrs {:name :string}
   :keys  [:name]})

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
      (batch-write {:put (map-keys :table specs)})
      (f))))

(defmacro with-items [specs & body]
  `(with-items* ~specs
     (fn [] ~@body)))

(def item {:name "Mephistopheles"})

(deftest list-tables+
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
                  {:table table :key item :consistent true}))))))

(deftest put+conditional
  (is (= :conditional-check-failed-exception
         (-> (put-item {:table table
                        :item item
                        :when [:not-exists :#name]} {:throw false})
             :hildebrand/error
             :type))))

(deftest put+returning
  (with-tables [create-table-default]
    (let [item' (assoc item :old "put-returning")]
      (is (empty?  (put-item {:table table :item item'})))
      (is (= item' (put-item {:table table :item item :return :all-old}))))))

(deftest put+meta
  (with-tables [create-table-default]
    (let [item (put-item {:table table :item item :capacity :total})]
      (is (empty? item))
      (is (= table (-> item meta :capacity :table))))))

(deftest delete+
  (with-items {create-table-default [item]}
    (is (empty? (del-item {:table table :key item})))))

(deftest delete+cc
  (with-items {create-table-default [item]}
    (is (= table
           (->
            (del-item {:table table :key item :capacity :total})
            meta
            :capacity
            :table)))))

(deftest delete+expected-expr
  (with-items {create-table-default [(assoc item :age 33 :hobby "Strolling")]}
    (is (empty?
         (del-item
          {:table table :key item :condition
           [:and
            [:<= 30 :#age]
            [:<= :#age 34]
            [:or
             [:begins-with :#hobby "St"]
             [:begins-with :#hobby "Tro"]]]})))))

(deftest delete+expected-expr-neg
  (with-items {create-table-default [(assoc item :age 33)]}
    (is (= :conditional-check-failed-exception
           (-> (del-item
                {:table table :key item :condition
                 [:and
                  [:or
                   [:between :#age 10 30]
                   [:between :#age 33 40]]
                  [:exists :#garbage]]}
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

(deftest update-item+
  (update-test
   {:hobbies #{"eating" "sleeping"}
    :bad     "good"}
   {:nick    [:set "Rodrigo"]
    :hobbies [:add #{"dreaming"}]
    :bad     [:remove]}
   {:nick    "Rodrigo"
    :hobbies #{"eating" "sleeping" "dreaming"}}))

(deftest update-item+list
  (update-test
   {:x [1 2 3]}
   {:x [:append ["4"]]}
   {:x [1 2 3 "4"]}))

(deftest update-item+concat
  (update-test
   {:x [1] :y #{1}}
   {:x [:concat [2]] :y [:concat #{2}]}
   {:x [1 2] :y #{1 2}}))

(deftest update-item+init
  (update-test
   {:y 5}
   {:x [:init 6] :y [:init 1]}
   {:y 5 :x 6}))

(deftest update-item+remove
  (update-test
   {:y 5 :z 5}
   {:y [:remove] :z [:remove]}
   {}))

(deftest update-item+nested-set
  (update-test
   {:a [:b]}
   {:a {0 [:set "c"]}}
   {:a ["c"]}))

(deftest update-item+deeply-nested-set
  (update-test
   {:a ["b" "c" {:d ["e"]}]}
   {:a {2 {:d {0 [:set "f"]}}}}
   {:a ["b" "c" {:d ["f"]}]}))

(deftest update-item+nested-init
  (update-test
   {:a {:b "c"}}
   {:a {:b [:init "d"] :B [:init "D"]}}
   {:a {:b "c" :B "D"}}))

(deftest update-item+nested-append
  (update-test
   {:a {:b [["c"]]}}
   {:a {:b {0 [:concat ["d"]]}}}
   {:a {:b [["c" "d"]]}}))

(deftest update-item+nested-remove
  (update-test
   {:a {:b [{:c :d}]}}
   {:a {:b {0 [:remove]}}}
   {:a {:b []}}))

(def items
  (for [i (range 5)]
    (assoc item :name (str "batch-write-" i))))

(deftest batch-write+
  (with-tables [create-table-default]
    (is (empty? (issue!! :batch-write-item {:put {table items}})))))

(deftest batch-write+get
  (with-tables [create-table-default]
    (issue!! :batch-write-item {:put {table items}})
    (let [responses (issue!!
                     :batch-get-item
                     {:items {table {:consistent true :keys items}}})]
      (is (= (into #{} items) (into #{} (responses table)))))))

(deftest query+
  (with-items {create-table-default [{:name "Mephistopheles"}]}
    (is (= [{:name "Mephistopheles"}]
           (map #(select-keys % #{:name})
                (:items (query {:table table
                                :where {:name [:eq "Mephistopheles"]}})))))))

(def indexed-table :hildebrand-test-table-indexed)
(def local-index   :hildebrand-test-table-indexed-local)
(def global-index  :hildebrand-test-table-indexed-global)
(def create-global-index
  {:name global-index
   :keys [:game-title :timestamp]
   :project [:keys-only]
   :throughput {:read 1 :write 1}})

(def create-table-indexed
  {:table indexed-table
   :throughput {:read 1 :write 1}
   :attrs {:user-id :string :game-title :string :timestamp :number}
   :keys  [:user-id :game-title]
   :indexes
   {:local
    [{:name local-index
      :keys [:user-id :timestamp]
      :project [:include [:data]]}]
    :global
    [create-global-index]}})

(def ->game-item (partial zipmap [:user-id :game-title :timestamp :data]))
(def indexed-items (map ->game-item [["moe" "Super Metroid" 1 "great"]
                                     ["moe" "Wii Fit" 2]]))

(deftest query+local-index
  (with-items {create-table-indexed indexed-items}
    (is (= [(first indexed-items)]
           (:items (query {:table indexed-table
                           :index local-index
                           :where {:user-id   [:= "moe"]
                                   :timestamp [:< 2]}}))))))

(deftest query+filter
  (with-items {create-table-indexed indexed-items}
    (is (= [(first indexed-items)]
           (:items (query {:table indexed-table
                           :where {:user-id [:= "moe"]}
                           :filter [:< :#timestamp 2]}))))))

(deftest query+global-index
  (with-items {create-table-indexed indexed-items}
    (is (= [(-> indexed-items first (dissoc :data))]
           (:items (query {:table indexed-table
                           :index global-index
                           :where {:game-title [:= "Super Metroid"]
                                   :timestamp  [:< 2]}}))))))

(def cleanup-description
  (partial
   walk/postwalk
   (fn [x]
     (cond
       (map?  x) (dissoc x :items :size :status :created :decreases :count)
       (coll? x) (vec x)
       :else     x))))

(deftest describe-complex-table
  (with-tables [create-table-indexed]
    (is (= create-table-indexed
           (-> (describe-table {:table indexed-table})
               cleanup-description)))))
