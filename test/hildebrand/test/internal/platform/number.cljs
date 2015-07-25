(ns hildebrand.test.internal.platform.number
  (:require [hildebrand.internal.platform.number :as n]
            [cljs.nodejs :as nodejs]
            [cljs.test :refer-macros [deftest is]]))

(def BigNumber (nodejs/require "bignumber.js"))

(def max+1 "9007199254740993")

(deftest string->number
  (is (.eq (BigNumber. max+1) (n/string->number max+1))))

(deftest ddb-num?
  (is (n/ddb-num? (BigNumber. max+1)))
  (is (= :error
         (try
           (n/ddb-num? (.pow (BigNumber. max+1) 100))
           (catch :default _
               :error)))))



