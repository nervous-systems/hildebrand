(ns hildebrand.dynamo.expr
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [hildebrand.util :refer [map-transformer transform-map]]
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

(defn alias-col [x]
  (let [x (name x)]
    (if (= "#" (subs x 0 1))
      [x (subs x 1)]
      [(str "#" x) x])))


(defn squish-operation [{:keys [op args]}]
  (reduce
   (fn [{:keys [values args attrs] :as m} arg]
     (merge-with
      conj m
      (cond
        (keyword? arg) (let [[alias arg] (alias-col arg)]
                         {:args alias :attrs [alias arg]})
        :else (let [g (->> (gensym) name (str ":"))]
                {:values [g arg]
                 :args   g}))))
   {:args [] :values {} :attrs {} :op op}
   args))

(defn flatten-funcall [{:keys [op args]}]
  (into [(name op)] args))

(defn normalize-op-name [op]
  (case op :rem :remove :del :delete op))

(defn merge-concats [{:keys [concat] :as by-op} values]
  (dissoc
   (reduce
    (fn [by-op [_ r :as args]]
      (let [k (if (-> r values set?) :add :append)]
        (update by-op k conj args)))
    by-op concat)
   :concat))

(defn args->call [fn-name [l r]]
  [l (format "%s(%s, %s)" fn-name l r)])

(defn merge-into-attr-set [k fn-name by-op]
  (let [ops (by-op k)]
    (-> by-op
        (update :set concat (map (partial args->call fn-name) ops))
        (dissoc k))))

(def merge-appends (partial merge-into-attr-set :append 'list_append))
(def merge-inits   (partial merge-into-attr-set :init 'if_not_exists))

(defn merge-ops [by-op values]
  (str/join
   " "
   (for [[op op-arglists] (-> by-op merge-inits (merge-concats values) merge-appends)]
     (str/join
      " "
      [(name (normalize-op-name op))
       (str/join ", "
                 (cond
                   (= op :set) (map (partial str/join " = ") op-arglists)
                   (= op :rem) (flatten op-arglists)
                   :else       (map (partial str/join " ") op-arglists)))]))))

(defn build-update-expr [m]
  (let [{:keys [calls values] :as result}
        (reduce
         (fn [{:keys [values calls attrs] :as m} [attr [op & args]]]
           (let [args     (into [attr] args)
                 {:keys [args op] :as out} (squish-operation {:op op :args args})]
             {:values (into values (:values out))
              :calls  (update calls op conj args)
              :attrs  (into attrs (:attrs out))}))
         {:values {} :calls {} :attrs {}}
         m)]
    (transform-map
     {:calls [:exprs #(merge-ops % values)]}
     result)))

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
