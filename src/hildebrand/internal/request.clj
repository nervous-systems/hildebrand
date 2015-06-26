(ns hildebrand.internal.request
  (:require [glossop :refer [stringy?]]
            [clojure.set :as set]
            [hildebrand.internal.expr :as expr]
            [hildebrand.util :refer
             [boolean? ddb-num? throw-empty type-aliases-out]]
            [plumbing.core :refer :all]))

(defn to-set-attr [v]
  (let [v (throw-empty v)]
    (cond
      (every? stringy? v)  {:SS (map name v)}
      (every? ddb-num? v)  {:NS (map str     v)}
      :else (throw (Exception. "Invalid set type")))))

(defn to-attr-value [v]
  (let [v (cond-> v (keyword? v) name)]
    (cond
      (string?  v) {:S (throw-empty v)}
      (nil?     v) {:NULL true}
      (boolean? v) {:BOOL v}
      (ddb-num? v) {:N (str v)}
      (vector?  v) {:L (map to-attr-value v)}
      (map?     v)     {:M (for-map [[k v'] v]
                             (name k) (to-attr-value v'))}
      (set?     v) (to-set-attr v)
      ;; This is basically a hack for binary support - you could supply
      ;; e.g. #hildebrand/literal {:BS ...}
      (expr/hildebrand-literal? v) v
      :else (throw (Exception. (str "Invalid value " (type v)))))))

(defn ->item [m]
  (for-map [[k v] m]
    (name k) (to-attr-value v)))

(def comparison-ops {:< :lt :<= :le := :eq :> :gt :>= :ge})

(defn ->key-conds [conds]
  (for-map [[col [op & args]] conds]
    col {:attribute-value-list (map to-attr-value args)
         :comparison-operator  (comparison-ops op op)}))

(def ->key-schema
  (partial
   map
   (fn [t attr]
     {:attribute-name (name attr)
      :key-type t}) [:hash :range]))

(defn ->attr-value [[k v]]
  {:attribute-name (name k)
   :attribute-type (type-aliases-out v v)})

(defn lift-projection-expression [m v]
  (cond-> m
    (not-empty v)
    (assoc :expression-attribute-names
           (into {}
             (for [col v :when (expr/aliased-col? col)]
               [col (expr/unalias-col col)]))
           :projection-expression v)))

(defn lift-expression [req v out-key]
  (if-let [{:keys [expr values attrs]} (map-vals not-empty v)]
    (cond-> req
      values (update :expression-attribute-values merge (->item values))
      attrs  (update :expression-attribute-names  merge attrs)
      expr   (assoc out-key expr))
    req))

(defn lift-condition-expression
  [req v & [{:keys [out-key]
             :or {out-key :condition-expression}}]]
  (lift-expression req (expr/cond-expr->statement v) out-key))

(defn lift-update-expression [req v]
  (lift-expression
   req (expr/update-ops->statement v) :update-expression))

(defn ->projection [[tag & [arg]]]
  (cond-> {:projection-type tag}
    arg (assoc :non-key-attributes arg)))

(defmulti  transform-request-kv (fn [k v m parent-k] k))

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

(def rename #(set/rename-keys % renames))

(defmethod transform-request-kv :default [k v m target]
  (assoc m k v))

(defmethod transform-request-kv :attrs [k v m target]
  (if (#{:create-table :update-table} target)
    (assoc m :attribute-definitions (map ->attr-value v))
    (lift-projection-expression m v)))

(defmethod transform-request-kv :key [k v m target]
  (assoc m k (->item v)))

(defn ->keys [v]
  (map
   (fn [t attr]
     {:attribute-name (name attr)
      :key-type t})
   [:hash :range] v))

(defmethod transform-request-kv :keys [k v m target]
  (if (= target :get-item)
    (assoc m k (map ->item v))
    (assoc m :key-schema (->keys v))))

(defmulti  prepare-request (fn [target body] target))
(defmethod prepare-request :default [_ body]
  body)

(defn restructure-request [target body]
  (rename
   (reduce
    (fn [acc [k v]]
      (transform-request-kv k v acc target))
    nil (prepare-request target body))))

(def ->throughput
  #(set/rename-keys
    %
    {:read :read-capacity-units
     :write :write-capacity-units}))

(defmethod transform-request-kv :throughput [k v m target]
  (assoc m k (->throughput v)))

(defn restructure-index
  [{:keys [name throughput keys] [tag & [arg]] :project :as m}]
  (cond-> {:index-name name}
    keys (assoc :key-schema (->keys keys))
    arg  (assoc-in [:projection :non-key-attributes] arg)
    tag  (assoc-in [:projection :projection-type] tag)
    throughput (assoc :provisioned-throughput
                      (->throughput throughput))))

(defmethod transform-request-kv :indexes [k v m parent]
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
           (mapcat
            (fn [[op indexes]]
              (for [index indexes]
                {op (restructure-index index)}))
            v))))

(defn ->batch-req [type m]
  (let [m (->item m)]
    (case type
      :delete {:delete-request {:key  m}}
      :put    {:put-request    {:item m}})))

(defn lift-batch-ops [k v m]
  (reduce
   (fn [m [table ops]]
     (let [ops (map (partial ->batch-req k) ops)]
       (update-in m [:request-items table] concat ops)))
   (dissoc m k) v))

(defmethod transform-request-kv :delete [k v m parent]
  (lift-batch-ops k v m))

(defmethod transform-request-kv :put [k v m parent]
  (lift-batch-ops k v m))

(defmethod transform-request-kv :project [k v m target]
  (lift-projection-expression m v))

(defmethod transform-request-kv :sort [k v m target]
  (assoc m k (= v :asc)))

(defmethod transform-request-kv :items [k v m target]
  (assoc m :request-items
         (for-map [[t req] v]
           t (restructure-request :get-item req))))

(defmethod transform-request-kv :filter [k v m target]
  (lift-condition-expression
   m v {:out-key :filter-expression}))

(defmethod transform-request-kv :where [k v m target]
  (assoc m k (->key-conds v)))

(defmethod transform-request-kv :when [k v m target]
  (lift-condition-expression m v))

(defmethod transform-request-kv :update [k v m target]
  (lift-update-expression m v))

(defmethod transform-request-kv :item [k v m target]
  (assoc m k (->item v)))
