(ns hildebrand.dynamo.util
  (:require [plumbing.core :refer [for-map]])
  (:import [clojure.lang BigInt]))

;; Taken from Faraday
(defn- assert-precision [x]
  (let [^BigDecimal dec (if (string? x) (BigDecimal. ^String x) (bigdec x))]
    (assert (<= (.precision dec) 38)
	    (str "DynamoDB numbers have <= 38 digits of precision."))
    true))

;; Taken from Faraday
(defn ddb-num? [x]
  "Is `x` a number type natively storable by DynamoDB? Note that DDB
  stores _all_ numbers as exact-value strings with <= 38 digits of
  precision.

  Ref. http://goo.gl/jzzsIW"
  (or (instance? Long    x)
      (instance? Double  x)
      (instance? Integer x)
      (instance? Float   x)
      ;;; High-precision types:
      (and (instance? BigInt     x) (assert-precision x))
      (and (instance? BigDecimal x) (assert-precision x))
      (and (instance? BigInteger x) (assert-precision x))))

(defn string->number [^String s]
  (if (.contains s ".")
    (BigDecimal. s)
    (bigint (BigInteger. s))))

(def boolean? (partial instance? Boolean))

(defn throw-empty [x]
  (if (empty? x)
    (throw (Exception. "Empty values disallowed"))
    x))

(def type-aliases-out
  {:string     :S
   :number     :N
   :list       :L
   :binary     :B
   :number-set :NS
   :string-set :SS
   :binary-set :BS
   :map        :M
   :null       :NULL
   :boolean    :BOOL})

(def type-aliases-in
  (for-map [[k v] type-aliases-out]
    v k))
