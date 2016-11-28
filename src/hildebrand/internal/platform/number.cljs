(ns hildebrand.internal.platform.number
  (:require [cljs.nodejs :as nodejs])
  (:refer-clojure :exclude [boolean?]))

(def BigNumber (nodejs/require "bignumber.js"))

(defn- assert-precision [n]
  (assert (<= (.precision n) 38)
          (str "DynamoDB numbers have <= 38 digits of precision."))
  true)

(defn ddb-num?
  "Is `x` a number type natively storable by DynamoDB? Note that DDB stores
  _all_ numbers as exact-value strings with <= 38 digits of precision.

  Ref. http://goo.gl/jzzsIW"
  [x]
  (or (number? x)
      (and (instance? BigNumber x) (assert-precision x))))

(defn string->number [s]
  (let [v (BigNumber. s)]
    (if (and (= -1 (.indexOf s ".")) (<= (.precision v) 15))
      (js/parseInt s)
      v)))

(defn boolean? [x]
  (= (type x) (type true)))
