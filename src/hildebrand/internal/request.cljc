(ns hildebrand.internal.request
  (:require [glossop.misc :refer [stringy?]]
            [clojure.set :as set]
            [eulalie.platform :refer [byte-array? ba->b64-string]]
            [hildebrand.internal.expr :as expr]
            [hildebrand.internal.platform.number :refer [boolean? ddb-num?]]
            [hildebrand.internal.util
             :refer [type-aliases-out throw-empty defmulti-dispatch]]
            [plumbing.core :refer [map-vals #?@ (:clj [for-map])]])
  #? (:cljs
      (:require-macros [plumbing.core :refer [for-map map-vals]])))

(defn to-set-attr [v]
  (let [v (throw-empty v)]
    (cond
      (every? stringy?    v)  {:SS (map name           v)}
      (every? ddb-num?    v)  {:NS (map str            v)}
      (every? byte-array? v)  {:BS (map ba->b64-string v)}
      :else (assert false "Invalid set type"))))

(declare to-attr-value)

(defn item-key [k]
  (if (and (keyword? k) (namespace k))
    (str (namespace k) "/" (name k))
    (name k)))

(defn ->item [m]
  (for-map [[k v] m]
    (item-key k) (to-attr-value v)))

(defn to-attr-value [v]
  (let [v (cond-> v (keyword? v) name)]
    (cond
      (string?     v) {:S (throw-empty v)}
      (nil?        v) {:NULL true}
      (boolean?    v) {:BOOL v}
      (ddb-num?    v) {:N (str v)}
      (vector?     v) {:L (map to-attr-value v)}
      (map?        v) {:M (->item v)}
      (byte-array? v) {:B (ba->b64-string v)}
      (set?        v) (to-set-attr v)

      (expr/hildebrand-literal? v) v
      :else (assert false (str "Invalid value " (type v))))))

(def comparison-ops {:< :lt :<= :le := :eq :> :gt :>= :ge :begins-with :begins_with})

(defn ->key-conds [conds]
  (for-map [[col [op & args]] conds]
    col {:attribute-value-list (map to-attr-value args)
         :comparison-operator  (comparison-ops op op)}))

(defn ->attr-value [[k v]]
  {:attribute-name (name k)
   :attribute-type (type-aliases-out v v)})

(defn lift-projection-expression [m k v]
  (if (not-empty v)
    (let [alias->col (for-map [col v]
                       (str "#" (name (gensym))) col)]
      (assoc m
             :expression-attribute-names alias->col
             :projection-expression (keys alias->col)))
    m))

(defn lift-expression [req v out-key]
  (if-let [{:keys [expr values attrs]} (map-vals not-empty v)]
    (cond-> req
      values (update :expression-attribute-values merge (->item values))
      attrs  (update :expression-attribute-names  merge attrs)
      expr   (assoc out-key expr))
    req))

(defn lift-condition-expression
  [req k v & [{:keys [out-key]
               :or {out-key :condition-expression}}]]
  (lift-expression req (expr/cond-expr->statement v) out-key))

(defn lift-update-expression [req k v]
  (lift-expression req (expr/update-ops->statement v) :update-expression))

(defn ->projection [[tag & [arg]]]
  (cond-> {:projection-type tag}
    arg (assoc :non-key-attributes arg)))

(defmulti transform-request-kv (fn [m k v parent-k] k))

(def renames
  {:index       :index-name
   :table       :table-name
   :capacity    :return-consumed-capacity
   :metrics     :return-item-collection-metrics
   :return      :return-values
   :start-table :exclusive-start-table-name
   :start-key   :exclusive-start-key
   :consistent  :consistent-read
   :where       :key-conditions
   :sort        :scan-index-forward
   :throughput  :provisioned-throughput
   :segments    :total-segments})

(defmethod transform-request-kv :default [m k v target] (assoc m k v))

(defmethod transform-request-kv :attrs [m k v target]
  (if (#{:create-table :update-table} target)
    (assoc m :attribute-definitions (map ->attr-value v))
    (lift-projection-expression m k v)))

(defn ->keys [v]
  (map
   (fn [t attr]
     {:attribute-name (name attr)
      :key-type t})
   [:hash :range] v))

(defmethod transform-request-kv :keys [m k v target]
  (if (= target :get-item)
    (assoc m k (map ->item v))
    (assoc m :key-schema (->keys v))))

(defn restructure-request [target body]
  (set/rename-keys
   (reduce
    (fn [acc [k v]]
      (transform-request-kv acc k v target))
    nil body)
   renames))

(def ->throughput
  #(set/rename-keys
    %
    {:read :read-capacity-units
     :write :write-capacity-units}))

(defn restructure-index
  [{:keys [name throughput keys] [tag & [arg]] :project :as m}]
  (cond-> {:index-name name}
    keys (assoc :key-schema (->keys keys))
    arg  (assoc-in [:projection :non-key-attributes] arg)
    tag  (assoc-in [:projection :projection-type] tag)
    throughput (assoc :provisioned-throughput
                      (->throughput throughput))))

(defmethod transform-request-kv :indexes [m k v parent]
  (case parent
    :create-table
    (let [{:keys [global local]} v]
      (cond-> m
        (not-empty global) (assoc :global-secondary-indexes
                                  (map restructure-index global))
        (not-empty local)  (assoc :local-secondary-indexes
                                  (map restructure-index local))))
    :update-table
    (assoc m :global-secondary-index-updates
           (for [[op index] v]
             {op (restructure-index index)}))))

(defn ->batch-req [type m]
  (let [m (->item m)]
    (case type
      :delete {:delete-request {:key  m}}
      :put    {:put-request    {:item m}})))

(defn lift-batch-ops [m k v]
  (reduce
   (fn [m [table ops]]
     (let [ops (for [op ops] (->batch-req k op))]
       (update-in m [:request-items table] concat ops)))
   (dissoc m k) v))

(defmethod transform-request-kv :items [m k v target]
  (assoc m :request-items
         (for-map [[t req] v]
           t (restructure-request :get-item req))))

(defmethod transform-request-kv :filter [m k v target]
  (lift-condition-expression
   m k v {:out-key :filter-expression}))

(defmulti-dispatch transform-request-kv
  (map-vals
   (fn [handler]
     (fn [m k v _] (handler m k v)))
   {:delete  lift-batch-ops
    :put     lift-batch-ops
    :project lift-projection-expression
    :when    lift-condition-expression
    :update  lift-update-expression}))

(defmulti-dispatch transform-request-kv
  (map-vals
   (fn [handler]
     (fn [m k v _] (assoc m k (handler v))))
   {:where ->key-conds
    :item  ->item
    :key   ->item
    :throughput ->throughput
    :sort (partial = :asc)}))

