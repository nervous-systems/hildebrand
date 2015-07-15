(ns hildebrand.internal.platform.number
  (:import (clojure.lang BigInt)))

(defn- assert-precision [x]
  (let [^BigDecimal dec (if (string? x) (BigDecimal. ^String x) (bigdec x))]
    (assert (<= (.precision dec) 38)
            (str "DynamoDB numbers have <= 38 digits of precision."))
    true))

;; Faraday
(defn ddb-num?
  "Is `x` a number type natively storable by DynamoDB? Note that DDB stores
  _all_ numbers as exact-value strings with <= 38 digits of precision.

  Ref. http://goo.gl/jzzsIW"
  [x]
  (or (instance? Long    x)
      (instance? Double  x)
      (instance? Integer x)
      (instance? Float   x)
      ;; High-precision types:
      (and (instance? BigInt     x) (assert-precision x))
      (and (instance? BigDecimal x) (assert-precision x))
      (and (instance? BigInteger x) (assert-precision x))))

(defn string->number [^String s]
  (if (.contains s ".")
    (BigDecimal. s)
    (bigint (BigInteger. s))))

(def boolean? (partial instance? Boolean))
