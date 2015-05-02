(ns hildebrand-test
  (:require [clojure.test :refer [deftest is]]
            [clojure.walk :as walk]
            [hildebrand :as h]
            [plumbing.core :refer [map-keys dissoc-in]]
            [hildebrand.test-util :refer :all])
  (:import (clojure.lang ExceptionInfo)))

(def item {:name "Mephistopheles"})

(deftest list-tables+
  (with-tables [create-table-default
                (assoc create-table-default
                       :table :hildebrand-test-table-list-tables)]
    (let [{:keys [tables] :as r} (h/list-tables!! creds {:limit 1})]
      (is (= 1 (count tables)))
      (is (-> r meta :end-table)))))

(deftest put+get
  (with-tables [create-table-default]
    (is (empty? (h/put-item!! creds table (assoc item :age 33))))
    (is (= 33 (:age (h/get-item!! creds table item {:consistent true}))))))

(deftest put+conditional
  (with-items {create-table-default [item]}
    (is (= :conditional-check-failed-exception
           (try
             (h/put-item!! creds table item {:when [:not-exists :#name]})
             (catch ExceptionInfo e
               (-> e ex-data :type)))))))

(deftest put+returning
  (with-tables [create-table-default]
    (let [item' (assoc item :old "put-returning")]
      (is (empty?  (h/put-item!! creds table item')))
      (is (= item' (h/put-item!! creds table item {:return :all-old}))))))

(deftest put+meta
  (with-tables [create-table-default]
    (let [item (h/put-item!! creds table item {:capacity :total})]
      (is (empty? item))
      (is (= table (-> item meta :capacity :table))))))

(deftest delete+
  (with-items {create-table-default [item]}
    (is (empty? (h/delete-item!! creds table item)))))

(deftest delete+cc
  (with-items {create-table-default [item]}
    (is (= table
           (->
            (h/delete-item!! creds table item {:capacity :total})
            meta
            :capacity
            :table)))))

(deftest delete+expected-expr
  (with-items {create-table-default [(assoc item :age 33 :hobby "Strolling")]}
    (is (empty?
         (h/delete-item!!
          creds table item
          {:when
           [:and
            [:<= 30 :#age]
            [:<= :#age 34]
            [:or
             [:begins-with :#hobby "St"]
             [:begins-with :#hobby "Tro"]]]})))))

(deftest delete+expected-expr-neg
  (with-items {create-table-default [(assoc item :age 33)]}
    (is (= :conditional-check-failed-exception
           (try
             (h/delete-item!!
              creds table item
              {:when
               [:and
                [:or
                 [:between :#age 10 30]
                 [:between :#age 33 40]]
                [:exists :#garbage]]})
             (catch ExceptionInfo e
               (-> e ex-data :type)))))))

(defn update-test [attrs-in updates attrs-out]
  (let [keyed-item (merge item attrs-in)
        expected   (merge item attrs-out)]
    (with-items {create-table-default [keyed-item]}
      (is (= expected
             (h/update-item!!
              creds table item updates {:return :all-new}))))))

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
    (is (empty? (h/batch-write-item!! creds {:put {table items}})))))

(deftest batch-write+get
  (with-tables [create-table-default]
    (h/batch-write-item!! creds {:put {table items}})
    (let [responses (h/batch-get-item!!
                     creds {table {:consistent true :keys items}})]
      (is (= (into #{} items)
             (into #{} (responses table)))))))

(deftest query+
  (with-items {create-table-default [{:name "Mephistopheles"}]}
    (is (= [{:name "Mephistopheles"}]
           (map #(select-keys % #{:name})
                (:items (h/query!! creds table {:name [:eq "Mephistopheles"]})))))))

(def ->game-item (partial zipmap [:user-id :game-title :timestamp :data]))
(def indexed-items (map ->game-item [["moe" "Super Metroid" 1 "great"]
                                     ["moe" "Wii Fit" 2]]))

(deftest query+local-index
  (with-items {create-table-indexed indexed-items}
    (is (= [(first indexed-items)]
           (:items (h/query!!
                    creds indexed-table {:user-id [:= "moe"] :timestamp [:< 2]}
                    {:index local-index}))))))

(deftest query+filter
  (with-items {create-table-indexed indexed-items}
    (is (= [(first indexed-items)]
           (:items (h/query!!
                    creds indexed-table {:user-id [:= "moe"]}
                    {:filter [:< :#timestamp 2]}))))))

(deftest query+global-index
  (with-items {create-table-indexed indexed-items}
    (is (= [(-> indexed-items first (dissoc :data))]
           (:items (h/query!!
                    creds indexed-table {:game-title [:= "Super Metroid"]
                                         :timestamp  [:< 2]}
                    {:index global-index}))))))

(def cleanup-description
  (partial
   walk/postwalk
   (fn [x]
     (cond
       (map?  x) (dissoc x :items :size :status :created :decreases :count :backfilling)
       (coll? x) (vec x)
       :else     x))))

(deftest describe-complex-table
  (with-tables [create-table-indexed]
    (is (= create-table-indexed
           (-> (h/describe-table!! creds indexed-table)
               cleanup-description)))))

(deftest scan+
  (let [items (for [i (range 5)]
                {:name     (str "scan-test-" i)
                 :religion "scan-test"})]
    (with-items {create-table-default items}
      (is (= (into #{} items)
             (into #{} (:items
                        (h/scan!! creds table
                                  {:filter [:= :#religion "scan-test"]}))))))))
