(ns hildebrand.test.core
  (:require [clojure.walk :as walk]
            [hildebrand.core :as h]
            [glossop.util]
            [glossop.core #? (:clj :refer :cljs :refer-macros) [go-catching <?]]
            [hildebrand.test.common :as test.common
             :refer [table create-table-default creds
                     namespaced-table create-table-namespaced
                     with-local-dynamo! with-remote-dynamo!]]
            [#? (:clj clojure.core.async :cljs cljs.core.async) :as async]
            [hildebrand.test.util
             #? (:clj :refer :cljs :refer-macros) [deftest is]])
  #? (:clj
      (:import (clojure.lang ExceptionInfo))
      :cljs
      (:require-macros [hildebrand.test.core :refer [update-test]])))

(defn ba->seq [x]
  #? (:clj
      (seq x)
      :cljs
      (for [i (range (aget x "length"))]
        (.readInt8 x i))))

(deftest binary-roundtrip
  (with-local-dynamo!
    (fn [creds]
      (let [in #? (:clj
                   (.getBytes "\u00a5123Hello" "utf8")
                   :cljs
                   (js/Buffer. "\u00a5123Hello" "utf8"))]
        (go-catching
          (<? (h/put-item! creds table {:name "binary-roundtrip" :attr in}))
          (is (= (ba->seq in)
                 (-> (h/get-item! creds table {:name "binary-roundtrip"})
                     <?
                     :attr
                     ba->seq))))))))

(def item {:name "Mephistopheles"})

(def namespaced-item {:namespaced/name "Mephistopheles"})

(deftest list-tables
  (with-local-dynamo!
    (fn [creds]
      (go-catching
        (let [tables (<? (h/list-tables! creds {:limit 1}))]
          (is (= 1 (count tables)))
          (is (-> tables meta :start-table)))))))

(deftest get+nonexistent
  (with-local-dynamo!
    (fn [creds]
      (go-catching
        (is (nil? (<? (h/get-item! creds table {:name "rofl"}))))))))

(deftest put+get
  (with-local-dynamo!
    (fn [creds]
      (go-catching
        (is (empty? (<? (h/put-item! creds table (assoc item :age 33)))))
        (is (= 33 (<? (h/get-item!
                       creds table item
                       {:consistent true}
                       {:chan (async/chan 1 (map :age))}))))))))

(deftest namespaced-put+get
  (let [val (assoc namespaced-item :namespaced/age 33)]
    (with-local-dynamo! {create-table-namespaced [val]}
      (fn [creds]
        (go-catching
         (is (= val (<? (h/get-item! creds namespaced-table namespaced-item {:consistent true})))))))))

(deftest put+conditional
  (with-local-dynamo! {create-table-default [item]}
    (fn [creds]
     (go-catching
       (is (= :conditional-failed
              (try
                (<? (h/put-item! creds table item {:when [:not-exists [:name]]}))
                (catch #? (:clj ExceptionInfo :cljs js/Error) e
                  (-> e ex-data :type)))))))))

(deftest put+returning
  (with-local-dynamo!
    (fn [creds]
      (go-catching
        (let [item' (assoc item :old "put-returning")]
          (is (empty?  (<? (h/put-item! creds table item'))))
          (is (= item' (<? (h/put-item! creds table item {:return :all-old})))))))))

(deftest put+meta
  (with-remote-dynamo! [create-table-default]
    (fn [creds]
      (go-catching
        (<? (h/put-item! creds table item {:capacity :total}))
        (let [item (<? (h/put-item! creds table item
                                    {:capacity :total :return :all-old}))]
          (is (= table (-> item meta :capacity :table))))))))

(deftest delete
  (with-local-dynamo! {create-table-default [item]}
    (fn [creds]
      (go-catching
        (is (empty? (<? (h/delete-item! creds table item))))))))

(deftest delete+cc
  (with-remote-dynamo! {create-table-default [item]}
    (fn [creds]
      (go-catching
        (is (= table
               (->
                (h/delete-item! creds table item
                                {:capacity :total :return :all-old})
                <?
                meta
                :capacity
                :table)))))))

(deftest delete+expected-expr
  (with-local-dynamo! {create-table-default
                       [(assoc item :age 33 :hobby "Strolling")]}
    (fn [creds]
      (go-catching
        (is (empty?
             (<? (h/delete-item!
                  creds table item
                  {:when
                   [:and
                    [:<= 30 ^:hildebrand/path [:age]]
                    [:<= [:age] 34]
                    [:in [:age] [30 33 34]]
                    [:not [:in [:age] #{30 34}]]
                    [:or
                     [:begins-with [:hobby] "St"]
                     [:begins-with [:hobby] "Tro"]]]}))))))))

(defn update-test [attrs-in updates attrs-out]
  (let [keyed-item (merge item attrs-in)
        exp        (merge item attrs-out)]
    (with-local-dynamo! {create-table-default [keyed-item]}
      (fn [creds]
        (go-catching
          (is (= exp
                 (<? (h/update-item!
                      creds table item updates {:return :all-new})))))))))

(deftest update-item
  (update-test
   {:hobbies #{:eating :sleeping}
    :bad     "good"}
   {:nick    [:set "Rodrigo"]
    :hobbies [:add #{"dreaming"}]
    :bad     [:remove]}
   {:nick    "Rodrigo"
    :hobbies #{:eating :sleeping :dreaming}}))

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

(deftest update-item+inc+dec
  (update-test
   {:a 5 :b 4}
   {:a [:inc 4]
    :b [:dec 4]}
   {:a 9 :b 0}))

(deftest update-item+nested-reserved-names
  (update-test
   {:deterministic {:except {:for "deterministic,"}}}
   {:deterministic {:except {:for [:set "deterministic!"]}}}
   {:deterministic {:except {:for "deterministic!"}}}))

(deftest update-item+refer
  (update-test
   {:please {:init "ialize"} :me ", boss!"}
   {:please
    {:init [:set  (with-meta [:me] {:hildebrand/path true})]}
     :and  [:init (with-meta [:me] {:hildebrand/path true})]}
   {:please {:init ", boss!"} :and ", boss!" :me ", boss!"}))

(deftest update-item+nested-refer
  (update-test
   {:irish {:set ["ter, "]} :norfolk "terrier"}
   {:norfolk [:set (with-meta [:irish :set 0] {:hildebrand/path true})]}
   {:irish {:set ["ter, "]} :norfolk "ter, "}))

(def items
  (for [i (range 5)]
    (assoc item :name (str "batch-write-" i))))

(deftest batch-write
  (with-local-dynamo!
    (fn [creds]
      (go-catching
        (is (empty? (<? (h/batch-write-item! creds {:put {table items}}))))))))

(deftest batch-write+get
  (with-local-dynamo!
    (fn [creds]
      (go-catching
        (<? (h/batch-write-item! creds {:put {table items}}))
        (let [responses
              (<? (h/batch-get-item!
                   creds {table {:consistent true :keys items}}))]
          (is (= (into #{} items)
                 (into #{} (responses table)))))))))

(deftest query
  (with-local-dynamo! {create-table-default [{:name "Mephistopheles"}]}
    (fn [creds]
      (go-catching
        (is (= [{:name "Mephistopheles"}]
               (map #(select-keys % #{:name})
                    (<? (h/query! creds table
                                  {:name [:= "Mephistopheles"]})))))))))

(def ->game-item (partial zipmap [:user-id :game-title :timestamp :data]))
(def indexed-items
  (map ->game-item
       [["moe" "Super Metroid" 1 "great"]
        ["moe" "Wii Fit" 2]]))

(deftest query+local-index
  (with-local-dynamo! {test.common/create-table-indexed indexed-items}
    (fn [creds]
      (go-catching
        (is (= [(first indexed-items)]
               (<? (h/query!
                    creds
                    test.common/indexed-table
                    {:user-id [:= "moe"] :timestamp [:< 2]}
                    {:index test.common/local-index}))))))))

(deftest query+filter
  (with-local-dynamo! {test.common/create-table-indexed indexed-items}
    (fn [creds]
      (go-catching
        (is (= [(first indexed-items)]
               (<? (h/query!
                    creds
                    test.common/indexed-table
                    {:user-id [:= "moe"]}
                    {:filter [:< [:timestamp] 2]}))))))))

(deftest scan
  (let [items (for [i (range 5)]
                {:name     (str "scan-test-" i)
                 :religion "scan-test"})]
    (with-local-dynamo! {create-table-default items}
      (fn [creds]
        (go-catching
          (is (= (into #{} items)
                 (into #{} (<? (h/scan! creds table
                                        {:filter [:= [:religion] "scan-test"]}))))))))))
