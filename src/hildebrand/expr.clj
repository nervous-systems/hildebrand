(ns hildebrand.expr
  (:require [clojure.string :as str]
            [clojure.walk :as walk]
            [hildebrand.util :refer :all]
            [plumbing.core :refer :all]
            [plumbing.map :refer [keyword-map]]))

(defn ->hildebrand-path [x]
  (if (coll? x)
    (with-meta x (assoc (meta x) :hildebrand/path true))
    x))

(def path-reader
  "Abstraction!"
  ->hildebrand-path)

(defn hildebrand-path?  [x]
  (and (coll? x) (:hildebrand/path (meta x))))

(defn aliased-col? [x]
  (and (str-or-keyword? x)
       (-> x name (subs 0 1) (= "#"))))

(defn alias-col [x segment->alias]
  (let [n (name x)]
    (cond->> n
      (not (aliased-col? n)) segment->alias)))

(defn alias-col+path [segments segment->alias]
  (map #(cond-> %
          (str-or-keyword? %)
          (alias-col segment->alias))
       segments))

(defn unalias-col [x]
  (let [n (name x)]
    (cond-> n
      (= "#" (subs n 0 1)) (subs 1))))

(def path->col (fn-> name (str/split #"\." 2) first))

(defn flatten-update-ops [m & [{:keys [path] :or {path []}}]]
  (reduce
   (fn [acc [k v]]
     (let [path (conj path k)]
       (cond
         (map? v)    (into acc (flatten-update-ops v {:path path}))
         (vector? v) (let [[op & args] v]
                       (conj acc (into [op path] args))))))
   []  m))

(defn col+path->string [[col & path]]
  (reduce
   (fn [acc part]
     (if (integer? part)
       (str acc "[" part "]")
       (str acc "." (name part))))
   (str col) path))

(defn path->attrs [segments segment->alias]
  (into {}
    (for [segment segments :when (str-or-keyword? segment)]
      [(segment->alias (name segment)) (name segment)])))

(defn parameterize-op [{:keys [op-name col+path arg] :as op} segment->alias]
  (let [{aliased-col+path :col+path :as op}
        (update op :col+path alias-col+path segment->alias)
        attrs (path->attrs col+path segment->alias)]
    (cond
      (hildebrand-path? arg)
      (let [aliased-arg-path (alias-col+path arg segment->alias)]
        [(assoc op :arg (col+path->string aliased-arg-path))
         {:attrs (merge attrs (path->attrs arg segment->alias))}])

      :else
      (let [g (->> (gensym) name (str ":"))]
        [(assoc op :arg g)
         {:attrs attrs
          :values {g arg}}]))))

(defn explode-op [[op-name col+path arg :as op]]
  (keyword-map op-name col+path arg))

(defmulti  arg->call (fn [fn-name col arg] fn-name))
(defmethod arg->call :default [fn-name col arg]
  (format "%s(%s, %s)" (name fn-name) col arg))
(defmethod arg->call :+ [_ col arg]
  (format "%s + %s" col arg))
(defmethod arg->call :- [_ col arg]
  (format "%s - %s" col arg))

(defn new-op-name [op op-name]
  (assoc op :op-name op-name))

(defn op->set [fn-name {:keys [col+path arg] :as op} params]
  (let [col+path (col+path->string col+path)]
    [(assoc op
            :op-name :set
            :arg (arg->call fn-name col+path arg))
     params]))

(defmulti  rewrite-op (fn [{:keys [op-name]} params] op-name))
(defmethod rewrite-op :default [op params] [op params])
(defmulti-dispatch
  rewrite-op
  {:append (partial op->set :list_append)
   :init   (partial op->set :if_not_exists)
   :inc    (partial op->set :+)
   :dec    (partial op->set :-)})

(defmethod rewrite-op :concat [{:keys [arg] :as op} {:keys [values] :as params}]
  (if (set? (values arg))
    [(new-op-name op :add) params]
    (-> op (new-op-name :append) (rewrite-op params))))

(defmethod rewrite-op :remove [{:keys [arg] :as op} params]
  [(assoc op :arg :hildebrand/no-arg) (dissoc-in params [:values arg])])

(defn parameterize-ops [ops segment->alias]
  (let [[ops param-maps]
        (->> ops
             (map (fn [op] (apply rewrite-op
                                  (parameterize-op (explode-op op)
                                                   segment->alias))))
             (apply map vector))]
    [ops (apply merge-with into param-maps)]))

(defn op->vector [{:keys [op-name col+path arg]}]
  (cond-> [(name op-name) (col+path->string col+path)]
    (not= arg :hildebrand/no-arg) (conj arg)))

(defmulti  op->string (fn [op-name op] op-name))
(defmethod op->string :default [_ [op-name col+path arg]]
  (str/join " " [col+path arg]))
(defmethod op->string :remove [_ [op-name col+path]]
  col+path)

(defmulti  op-group->string (fn [op-name ops] op-name))
(defmethod op-group->string :default [op-name ops]
  (str (name op-name) " "
       (str/join ", " (map (fn->> op->vector (op->string op-name)) ops))))
(defmethod op-group->string :set [op-name ops]
  (str "set "
       (str/join ", "
                 (for [op ops :let [[_ col+path arg] (op->vector op)]]
                   (str col+path " = " arg)))))

(defn ops->string [by-op]
  (str/join
   " "
   (for [[op ops] by-op]
     (op-group->string op ops))))

(defn update-ops->statement [m]
  (let [[ops {:keys [attrs values] :as params}]
        (-> m
            flatten-update-ops
            (parameterize-ops (memoized-fn _ [_] (str "#" (gensym)))))
        ops (group-by :op-name ops)]
    (assoc params :expr (ops->string ops))))

(defn parameterize-arg [arg segment->alias]
  (if (hildebrand-path? arg)
    {:args  [(col+path->string (alias-col+path arg segment->alias))]
     :attrs (path->attrs arg segment->alias)}
    (let [g (name (gensym))]
      {:args [(str ":" g)] :values {(str ":" g) arg}})))

(defmulti  parameterize-args (fn [op segment->alias] (:op-name op)))
(defmethod parameterize-args :default
  [{:keys [op-name args] :as op} segment->alias]
  (let [args+attrs (apply merge-with into
                          (map #(parameterize-arg % segment->alias) args))]
    (assoc args+attrs :op-name op-name)))

;; XXX in

(def logical-ops #{:and :or :not})

(def structural-ops
  "These can accept sequences as their second parameter, as opposed to,
  e.g. less than / greater than, for which we will always assume a vector
  represents an attribute path"
  #{:= :contains :in})

(defn resolve-args [{:keys [args] :as op}]
  (if (structural-ops op)
    (update-in op [:args 0] ->hildebrand-path)
    (assoc op :args (map ->hildebrand-path args))))

(defn parameterize-expr [[op & args] segment->alias]
  (if (logical-ops op)
    (-> (reduce
         (fn [{:keys [args values attrs]} arg]
           (let [{args' :args vals' :values attrs' :attrs op :op-name}
                 (parameterize-expr arg segment->alias)]
             {:args   (conj args (into [op] args'))
              :values (merge values vals')
              :attrs  (merge attrs attrs')}))
         {}
         args)
        (assoc :op-name op))
    (-> {:op-name op :args (vec args)}
        resolve-args
        (parameterize-args segment->alias))))

(defn group [& rest]
  (str "(" (str/join " " rest) ")"))

(defn arglist [xs]
  (str/join ", " xs))

(def prefix-ops
  {:begins-with :begins_with
   :not-exists  :attribute_not_exists
   :exists      :attribute_exists
   :contains    :contains})

(defmulti  cond-expr-op->string (fn [[op & args]] op))
(defmethod cond-expr-op->string :default [[op & args]]
  (apply group (interpose (name op) args)))
(defmethod cond-expr-op->string :between [[op x y z]]
  (group x (name op) y 'and z))
(defmethod cond-expr-op->string :in [[op x xs]]
  (group x (name op) (group (arglist xs))))
(defmethod cond-expr-op->string :not [[op arg]]
  (group 'not arg))
(defmulti-dispatch cond-expr-op->string
  (for-map [[in-op out-op] prefix-ops]
    in-op (fn [[_ & args]]
            (str (name out-op) (group (arglist args))))))

(defn cond-expr->string [expr]
  (walk/postwalk
   (fn [form]
     (cond-> form
       (coll? form) cond-expr-op->string))
   expr))

(defn cond-expr->statement [expr]
  (let [{:keys [op-name args values attrs] :as params}
        (parameterize-expr expr (memoized-fn _ [_] (str "#" (gensym))))]
    {:attrs  attrs
     :values values
     :expr   (cond-expr->string (into [op-name] args))}))
