(ns hildebrand.test.core
  (:require [clojure.walk :as walk]
            [hildebrand.core :as h]
            [plumbing.core :refer [map-keys dissoc-in]]
            [hildebrand.test.common :as test.common :refer
             [table create-table-default creds]]
            #?@ (:clj
                 [[clojure.core.async :as async]
                  [clojure.test :refer [is]]
                  [hildebrand.test.async :refer [deftest]]
                  [glossop.core :refer [go-catching <?]]]
                 :cljs
                 [[cemerick.cljs.test]
                  [cljs.core.async :as async]]))
  #? (:clj
      (:import (clojure.lang ExceptionInfo))
      :cljs
      (:require-macros [glossop.macros :refer [<? go-catching]]
                       [hildebrand.test.async.macros :refer [deftest]]
                       [cemerick.cljs.test :refer [is]]
                       [hildebrand.test.core :refer [update-test]])))

(def item {:name "Mephistopheles"})

(deftest list-tables
  (go-catching
    (let [tables (<? (h/list-tables! creds {:limit 1}))]
      (is (= 1 (count tables)))
      (is (-> tables meta :end-table)))))

(deftest get+nonexistent
  (go-catching
    (is (nil? (<? (h/get-item! creds table {:name "rofl"}))))))

(deftest put+get
  (go-catching
    (is (empty? (<? (h/put-item! creds table (assoc item :age 33)))))
    (is (= 33 (:age (<? (h/get-item! creds table item {:consistent true})))))))

(deftest put+conditional
  (test.common/with-items! {create-table-default [item]}
    #(go-catching
       (is (= :conditional-failed
              (try
                (<? (h/put-item! creds table item {:when [:not-exists [:name]]}))
                (catch #? (:clj ExceptionInfo :cljs js/Error) e
                  (-> e ex-data :type))))))))

(deftest put+returning
  (go-catching
    (let [item' (assoc item :old "put-returning")]
      (is (empty?  (<? (h/put-item! creds table item'))))
      (is (= item' (<? (h/put-item! creds table item {:return :all-old})))))))

(deftest put+meta
  (go-catching
    (let [item (<? (h/put-item! creds table item {:capacity :total}))]
      (is (empty? item))
      (is (= table (-> item meta :capacity :table))))))

(deftest delete
  (test.common/with-items! {create-table-default [item]}
    #(go-catching
       (is (empty? (<? (h/delete-item! creds table item)))))))

(deftest delete+cc
  (test.common/with-items! {create-table-default [item]}
    #(go-catching
       (is (= table
              (->
               (h/delete-item! creds table item {:capacity :total})
               <?
               meta
               :capacity
               :table))))))

(deftest delete+expected-expr
  (test.common/with-items!
    {create-table-default [(assoc item :age 33 :hobby "Strolling")]}
    #(go-catching
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
                    [:begins-with [:hobby] "Tro"]]]})))))))

(deftest delete+expected-expr-neg
  (test.common/with-items! {create-table-default [(assoc item :age 33)]}
    #(go-catching
       (is (= :conditional-failed
              (try
                (<? (h/delete-item!
                     creds table item
                     {:when
                      [:and
                       [:or
                        [:between [:age] 10 30]
                        [:between [:age] 33 40]]
                       [:exists [:garbage]]]}))
                (catch #? (:clj ExceptionInfo :cljs js/Error) e
                       (-> e ex-data :type))))))))




;; long story
#? (:clj
    (defmacro update-test [attrs-in updates attrs-out & [ctx]]
      (let [keyed-item (merge item attrs-in)
            exp        (merge item attrs-out)
            act       `(h/update-item! ~creds ~table ~item ~updates
                                       {:return :all-new})]
        (if (:ns &env)
          `(test.common/with-items! ~{create-table-default [keyed-item]}
             #(glossop.macros/go-catching
                (cemerick.cljs.test/is (= ~exp (glossop.macros/<? ~act)))))
          `(test.common/with-items! ~{create-table-default [keyed-item]}
             #(go-catching (is (= ~exp (<? ~act)))))))))

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
  (go-catching
    (is (empty? (<? (h/batch-write-item! creds {:put {table items}}))))))

(deftest batch-write+get
  (go-catching
    (<? (h/batch-write-item! creds {:put {table items}}))
    (let [responses
          (<? (h/batch-get-item!
               creds {table {:consistent true :keys items}}))]
      (is (= (into #{} items)
             (into #{} (responses table)))))))

(deftest query
  (test.common/with-items! {create-table-default [{:name "Mephistopheles"}]}
    (fn []
      (go-catching
        (is (= [{:name "Mephistopheles"}]
               (map #(select-keys % #{:name})
                    (<? (h/query! creds table
                                  {:name [:= "Mephistopheles"]})))))))))

(def ->game-item (partial zipmap [:user-id :game-title :timestamp :data]))
(def indexed-items (map ->game-item
                        [["moe" "Super Metroid" 1 "great"]
                         ["moe" "Wii Fit" 2]]))


(deftest query+local-index
  (test.common/with-items! {test.common/create-table-indexed indexed-items}
    #(go-catching
       (is (= [(first indexed-items)]
              (<? (h/query!
                   creds
                   test.common/indexed-table
                   {:user-id [:= "moe"] :timestamp [:< 2]}
                   {:index test.common/local-index})))))))

(deftest query+filter
  (test.common/with-items! {test.common/create-table-indexed indexed-items}
    #(go-catching
       (is (= [(first indexed-items)]
              (<? (h/query!
                   creds
                   test.common/indexed-table
                   {:user-id [:= "moe"]}
                   {:filter [:< [:timestamp] 2]})))))))

(deftest query+global-index
  (test.common/with-items! {test.common/create-table-indexed indexed-items}
    #(go-catching
       (is (= [(-> indexed-items first (dissoc :data))]
              (<? (h/query!
                   creds
                   test.common/indexed-table
                   {:game-title [:= "Super Metroid"]
                    :timestamp  [:< 2]}
                   {:index test.common/global-index})))))))

(deftest scan
  (let [items (for [i (range 5)]
                {:name     (str "scan-test-" i)
                 :religion "scan-test"})]
    (test.common/with-items! {create-table-default items}
      #(go-catching
         (is (= (into #{} items)
                (into #{} (<? (h/scan! creds table
                                       {:filter [:= [:religion] "scan-test"]})))))))))
