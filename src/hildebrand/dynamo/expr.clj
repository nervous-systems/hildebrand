(ns hildebrand.dynamo.expr
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [hildebrand.util :refer [map-transformer]]
            [clojure.walk :as walk]
            [plumbing.core :refer :all]
            [plumbing.map]))

(defn group [& rest]
  (str "(" (str/join " " rest) ")"))

(defn arglist [xs]
  (str/join ", " xs))

(defmulti flatten-expr (fn [{:keys [hildebrand/type]}] type))

(def functions
  '{exists      attribute_exists
    not-exists  attribute_not_exists
    begins-with begins_with
    contains    contains})

(defmethod flatten-expr :condition-expression [{:keys [hildebrand/expr hildebrand/env]}]
  (walk/postwalk
   (fn [l]
     (if (coll? l)
       (let [[op & args] l]
         (cond (functions  op) (str (functions op)  (group (arglist args)))
               (= 'between op) (let [[x y z] args]  (group x op y 'and z))
               (= 'in op)      (let [[x & xs] args] (group x op (group (arglist xs))))
               :else (apply group (interpose op args))))
       (env l l)))
   ;; There's a single toplevel expression in this case
   (first expr)))

(defn build-condition-expr [{:keys [expr values]}]
  (walk/postwalk
   (fn [l]
     (cond
       (coll? l)
       (let [[op & args] l]
         (cond (functions  op) (str (functions op)  (group (arglist args)))
               (= 'between op) (let [[x y z] args]  (group x op y 'and z))
               (= 'in op)      (let [[x & xs] args] (group x op (group (arglist xs))))
               :else (apply group (interpose op args))))
       (keyword? l) (str l)
       :else l))
   ;; There's a single toplevel expression in this case
   expr))

(def update-ops '{del DELETE rem REMOVE})

(defmethod flatten-expr :update-expression [{:keys [hildebrand/expr hildebrand/env]}]
  (->> expr
       (map (fn [[op & [x y :as args]]]
              (if (= op 'set)
                (str "set " (env x x) " = " (env y y))
                (str (update-ops op op) " "  (str/join " " (map #(env % %) args))))))
       (str/join " ")))

(defn unprefix-col [x]
  (when (keyword? x)
    (let [col (name x)]
      (when (= "#" (subs col 0 1))
        (subs col 1)))))

(def process-arglist
  (partial
   reduce
   (fn [{:keys [values args] :as m} arg]
     (merge-with conj m (if (keyword? arg)
                          {:args (name arg)}
                          (let [g (->> (gensym) name (str ":"))]
                            {:values [g arg] :args g}))))
   {:args [] :values {}}))

(defmulti  rewrite-funcall (fn [{:keys [op]}] op))
(defmethod rewrite-funcall :default [m]  m)
(defmethod rewrite-funcall :set [m]
  (update m :args (fn [[x & xs]] (into [x '=] xs))))

(defn flatten-funcall [{:keys [op args]}]
  (into [(name op)] args))

(def build-update-expr
  (partial
   reduce
   (fn [{:keys [values exprs]} [attr [op & args]]]
     (let [args     (into [attr] args)
           {values' :values args :args :as m} (process-arglist args)
           funcall  {:args args :op (case op :rem :remove :del :delete op)}]
       {:values (into values values')
        :exprs  (conj exprs (-> funcall
                                rewrite-funcall
                                flatten-funcall))}))
   {:values {} :exprs []}))

(defn flatten-update-expr [es]
  (str/join " " (map (partial str/join " ") es)))

(defn prepare-expr [expr & [{:keys [values attrs] :or {values {} attrs {}}}]]
  (let [prefix-keys (fn [prefix m]
                      (map
                       (fn [k] [(symbol (name k)) (str prefix (name k))])
                       (keys m)))]

    {:attrs  (map-keys (fn->> name (str "#")) attrs)
     :values (map-keys (fn->> name (str ":")) values)
     :hildebrand/expr expr
     :hildebrand/env
     (into {}
       (concat (prefix-keys "#" attrs)
               (prefix-keys ":" values)))}))

(defmacro bindings->map*
  ([values]
   `(bindings->map* ~values []))
  ([values attrs]
   (let [->map #(into {} (map (fn [[k v]] `['~k ~v]) (partition 2 %)))]
     `{:values ~(->map values) :attrs ~(->map attrs)})))

(defmacro let-expr [& rest]
  (let [[bindings expr] (split-with vector? rest)]
    `(prepare-expr (quote ~expr) (bindings->map* ~@bindings))))
