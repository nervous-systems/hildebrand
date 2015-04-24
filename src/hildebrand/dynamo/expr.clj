(ns hildebrand.dynamo.expr
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [hildebrand.util :refer :all]
            [clojure.walk :as walk]
            [plumbing.core :refer :all]
            [plumbing.map :refer [keyword-map]]))

(defn aliased-col? [x]
  (and (or (keyword? x) (string? x))
       (-> x name (subs 0 1) (= "#"))))

(defn alias-col [x]
  (let [n (name x)]
    (cond->> n
      (not (aliased-col? n)) (str "#"))))

(defn unalias-col [x]
  (let [n (name x)]
    (cond-> n
      (= "#" (subs n 0 1)) (subs 1))))

(defn flatten-update-ops [m & [{:keys [path acc] :or {path [] acc []}}]]
  (loop [[[k v :as kv] & m] (seq m) acc acc]
    (let [path (conj path k)]
      (cond
        (nil? kv)   acc
        (map? v)    (recur m (flatten-update-ops v {:path path :acc acc}))
        (vector? v) (let [[op & args] v]
                      (recur m (conj acc (into [op path] args))))))))

(defn parameterize-op [{:keys [op-name col path arg] :as op}]
  (let [{aliased-col :col :as op} (update op :col alias-col)
        attrs {aliased-col (unalias-col col)}]
    (if (aliased-col? arg)
      (let [arg (name arg)] 
        [(assoc op :arg arg)
         {:attrs (assoc attrs arg (unalias-col arg))}])
      (let [g (->> (gensym) name (str ":"))]
        [(assoc op :arg g)
         {:attrs attrs
          :values {g arg}}]))))

(defn explode-op [[op-name [col & path] arg :as op]]
  (keyword-map op-name col path arg))

(defn col+path->string [[col & path]]
  (reduce
   (fn [acc part]
     (if (integer? part)
       (str acc "[" part "]")
       (str acc "." (name part))))
   (str col) path))

(defn arg->call [fn-name col arg]
  (format "%s(%s, %s)" fn-name col arg))

(defn new-op-name [op op-name]
  (assoc op :op-name op-name))

(defn op->set [fn-name {:keys [col path] :as op} params]
  (let [col+path (col+path->string (into [col] path))]
    [(-> op
         (new-op-name :set)
         (update :arg (partial arg->call fn-name col+path))) params]))

(defmulti  rewrite-op (fn [{:keys [op-name]} params] op-name))
(defmethod rewrite-op :default [op params] [op params])
(defmulti-dispatch
  rewrite-op
  {:append (partial op->set 'list_append)
   :init   (partial op->set 'if_not_exists)})

(defmethod rewrite-op :concat [{:keys [arg] :as op} {:keys [values] :as params}]
  (if (set? (values arg))
    [(new-op-name op :add) params]
    (-> op (new-op-name :append) (rewrite-op params))))

(defmethod rewrite-op :remove [{:keys [arg] :as op} params]
  [(assoc op :arg :hildebrand/no-arg) (dissoc-in params [:values arg])])

(defn parameterize-ops [ops]
  (let [[ops param-maps]
        (->> ops
             (map (fn->> explode-op parameterize-op (apply rewrite-op)))
             (apply map vector))]
    [ops (apply merge-with into param-maps)]))

(defn op->vector [{:keys [op-name col path arg]}]
  (cond-> [(name op-name) (col+path->string (into [col] path))]
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
            parameterize-ops)
        ops (group-by :op-name ops)]
    (assoc params :expr (ops->string ops))))

(def logical-ops #{:and :or :not})

(def prefix-ops
  {:begins-with :begins_with
   :not-exists  :attribute_not_exists
   :exists      :attribute_exists
   :contains    :contains})

(defn column-arg? [a]
  (and (keyword? a) (-> a name (subs 0 1) (= "#"))))

(defn parameterize-arg [arg]
  (if (column-arg? arg)
    {:args [(name arg)] :attrs {(name arg) (unalias-col arg)}}
    (let [g (->> (gensym) name (str ":"))]
      {:args [g] :values {g arg}})))

(defn map-merge [f v]
  (->> v (map f) (apply merge-with into)))

(defn parameterize-args [args]
  (let [m (map-merge parameterize-arg args)]
    [(:args m) (dissoc m :args)]))

(defn parameterize-expr [[op & args]]
  (if (logical-ops op)
    (let [[sub-ops param-maps] (apply map vector (map parameterize-expr args))
          params               (apply merge-with into param-maps)]
      [(into [op] sub-ops) params])
    (let [[args params] (parameterize-args args)]
      [(into [op] args) params])))

(defn group [& rest]
  (str "(" (str/join " " rest) ")"))

(defn arglist [xs]
  (str/join ", " xs))

(defmulti  cond-expr-op->string (fn [[op & args]] op))
(defmethod cond-expr-op->string :default [[op & args]]
  (apply group (interpose (name op) args)))
(defmethod cond-expr-op->string :between [[op x y z]]
  (group x (name op) y 'and z))
(defmethod cond-expr-op->string :in [[op x & xs]]
  (group x (name op) (group (arglist xs))))
(defmethod cond-expr-op->string :not [[op arg]]
  (group 'not arg))
(defmulti-dispatch cond-expr-op->string
  (for-map [[in-op out-op] prefix-ops]
    in-op (fn [[_ & args]] (str (name out-op) (group (arglist args))))))

(defn cond-expr->string [expr]
  (walk/postwalk
   (fn [form]
     (cond-> form
       (coll? form) cond-expr-op->string))
   expr))

(defn cond-expr->statement [expr]
  (let [[expr params] (parameterize-expr expr)]
    (assoc params :expr (cond-expr->string expr))))
