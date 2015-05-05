(ns hildebrand.expr
  (:require [clojure.string :as str]
            [clojure.walk :as walk]
            [hildebrand.util :refer :all]
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

(defmulti  arg->call (fn [fn-name col arg] fn-name))
(defmethod arg->call :default [fn-name col arg]
  (format "%s(%s, %s)" (name fn-name) col arg))
(defmethod arg->call :+ [_ col arg]
  (format "%s + %s" col (or arg 1)))
(defmethod arg->call :- [_ col arg]
  (format "%s - %s" col (or arg 1)))

(defn new-op-name [op op-name]
  (assoc op :op-name op-name))

(defn op->set [fn-name {:keys [col path arg] :as op} params]
  (let [col+path (col+path->string (into [col] path))]
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
  (let [g (name (gensym))]
    (if (column-arg? arg)
      (let [col (path->col arg)]
        {:args  [(str "#" g)]
         :attrs {(str "#" g) (unalias-col col)}})
      {:args [(str ":" g)] :values {(str ":" g) arg}})))

(defn parameterize-args [args]
  (let [{:keys [args] :as m}
        (apply merge-with into (map parameterize-arg args))]
    [args (dissoc m :args)]))

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
    in-op (fn [[_ & args]]
            (str (name out-op) (group (arglist args))))))

(defn cond-expr->string [expr]
  (walk/postwalk
   (fn [form]
     (cond-> form
       (coll? form) cond-expr-op->string))
   expr))

(defn cond-expr->statement [expr]
  (let [[expr params] (parameterize-expr expr)]
    (assoc params :expr (cond-expr->string expr))))
