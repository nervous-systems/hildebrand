(ns hildebrand.util
  (:require [clojure.set :as set]
            [plumbing.core :refer :all]
            [clojure.walk :as walk]))

;; Experiments, mostly

(defmacro if-> [test val then else]
  `(let [val# ~val]
     (if ~test
       (-> val# ~then)
       (-> val# ~else))))

(defmacro branch-> [x & conds]
  "Short-circuiting cond-> with anaphoric predicates.
   (branch-> x p? y) -> (cond-> (-> x p?) y)"
  (let [g     (gensym)
        conds (mapcat (fn [[l r]] `((-> ~g ~l) ~r)) (partition 2 conds))]
    `(let [~g ~x]
       (cond-> ~g ~@conds))))

(defmacro branch->> [x & conds]
  (let [g     (gensym)
        conds (mapcat (fn [[l r]] `((->> ~g ~l) ~r)) (partition 2 conds))]
    `(let [~g ~x]
       (cond->> ~g ~@conds))))

(defmacro branch [x & conds]
  (let [g     (gensym)
        else  (when (odd? (count conds)) (last conds))
        conds (mapcat (fn [[l r]] `((~l ~x) ~r)) (partition 2 conds))]
    `(let [~g ~x]
       (cond ~@conds :else ~else))))

(defn key-renamer [spec]
  (fn [m] (set/rename-keys m spec)))

(defn transform-map [spec m]
  (into {}
    (for [[k v] m]
      (if-let [n+t (spec k)]
        (cond
          (keyword? n+t) [n+t v]
          (fn?      n+t) [k   (n+t v)]
          :else
          (let [[new-name transform] n+t]
            [new-name (transform v)]))
        [k v]))))

(defn map-transformer [spec]
  (partial transform-map spec))

(defn mapper [& args]
  (apply partial map args))

(defn reducer [& args]
  (apply partial reduce args))

(defn defmulti-dispatch [method v->handler]
  (doseq [[v handler] v->handler]
    (defmethod method v [& args] (apply handler args))))
