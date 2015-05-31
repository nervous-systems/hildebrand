(ns hildebrand.response
  (:require [clojure.set :as set]
            [clojure.walk :as walk]
            [hildebrand.util :refer :all]
            [plumbing.core :refer :all]
            [clojure.tools.logging :as log]))

(defn from-attr-value [m]
  (let [[[tag value]] (seq m)]
    (condp = tag
      :S    value
      :N    (string->number value)
      :M    (map-vals from-attr-value value)
      :L    (mapv     from-attr-value value)
      :BOOL (boolean value)
      :NULL nil
      :SS   (into #{} (map keyword value))
      :NS   (into #{} (map string->number value))
      ;; XXX
      :BS   value
      ;; XXX
      :B    value)))

(defn ->attr-def [{:keys [attribute-name attribute-type]}]
  [attribute-name (type-aliases-in attribute-type attribute-type)])

(def ->key-schema
  (fn->> (sort-by :key-type) (map :attribute-name)))

(def ->throughput
  #(set/rename-keys
    %
    {:read-capacity-units :read
     :write-capacity-units :write
     :number-of-decreases-today :decreases
     :last-increase-date-time :last-increase
     :last-decrease-date-time :last-decrease}))

(defn ->projection [{attrs :non-key-attributes type :projection-type}]
  (when type
    (cond-> [type] (not-empty attrs) (conj attrs))))

(def index-renames
  {:index-name :name
   :index-size-bytes :size
   :index-status :status
   :item-count :count
   :key-schema :keys
   :projection :project
   :provisioned-throughput :throughput})

(def ->global-index
  (fn->
   (update :key-schema ->key-schema)
   (update :projection ->projection)
   (update :provisioned-throughput ->throughput)
   (set/rename-keys index-renames)))

(def ->local-index (fn-> ->global-index (dissoc :throughput)))

(def table-description-renames
  {:table-name :table
   :attribute-definitions :attrs
   :key-schema :keys
   :provisioned-throughput :throughput
   :table-status :status
   :table-size-bytes :size
   :item-count :items
   :creation-date-time :created})

(defmulti  transform-table-kv (fn [k v m] k))
(defmethod transform-table-kv :default [k v m]
  (assoc m k v))

(defmethod transform-table-kv :attribute-definitions [k v m]
  (assoc m k (into {} (map ->attr-def v))))

(defmethod transform-table-kv :key-schema [k v m]
  (assoc m k (->key-schema v)))

(defmethod transform-table-kv :provisioned-throughput [k v m]
  (assoc m k (->throughput v)))

(defmethod transform-table-kv :global-secondary-indexes [k v m]
  (update-in m [:indexes :global] into (map ->global-index v)))

(defmethod transform-table-kv :local-secondary-indexes [k v m]
  ;; These are a subset of global indexes
  (update-in m [:indexes :local] into (map ->local-index v)))

(defn ->table-description [m]
  (set/rename-keys
   (reduce
    (fn [acc [k v]]
      (transform-table-kv k v acc))
    nil m)
   table-description-renames))

(defmulti  transform-response-kv (fn [k v m target] k))
(defmethod transform-response-kv :default [k v m _] (assoc m k v))
(defmethod transform-response-kv :table-description [k v m _]
  (->table-description v))
(defmethod transform-response-kv :table [k v m _]
  (->table-description v))

(defmethod transform-response-kv :consumed-capacity [k v m _]
  (assoc m k (walk/prewalk-replace
              {:capacity-units :capacity
               :table-name     :table}
              v)))

(def ->item (partial map-vals from-attr-value))

(defmethod transform-response-kv :item [k v m _]
  (assoc m k (->item v)))

(defmethod transform-response-kv :items [k v m _]
  (assoc m k (map ->item v)))

(defmethod transform-response-kv :attributes [k v m _]
  (assoc m k (->item v)))

(defmulti  restructure-response* (fn [target m] target))
(defmethod restructure-response* :default [_ m] m)
(defmethod restructure-response* :get-item [_ {:keys [item] :as m}]
  ;; If anything except the item appears in the response (e.g. capacity, etc.)
  ;; then default to the empty map so we can associate metadata with it.
  ;; It's not ideal, but I'm not sure what is.
  (let [m    (not-empty (dissoc m :item))
        item (cond-> (not-empty item) m (or {}))]
    (when item
      (with-meta item m))))

(defmethod restructure-response* :list-tables [_ {:keys [tables end-table]}]
  (with-meta tables {:end-table end-table}))

(defmethod restructure-response* :query [_ {:keys [items] :as m}]
  (with-meta (or items []) (dissoc m :items)))

(defmethod restructure-response* :scan [_ {:keys [items] :as m}]
  (with-meta (or items []) (dissoc m :items)))

(defn error [type message & [data]]
  (assoc data :hildebrand/error {:type type :message message}))

(defn maybe-unprocessed-error [unprocessed]
  (when (not-empty unprocessed)
    (error :unprocessed-items
           (format "%d unprocessed items" (count unprocessed))
           {:unprocessed unprocessed})))

(defmethod restructure-response* :batch-get-item [_ {:keys [unprocessed responses] :as m}]
  (or (maybe-unprocessed-error unprocessed)
      (with-meta
        (for-map [[t items] responses]
          t (map ->item items))
        m)))

(defmethod restructure-response* :batch-write-item [_ {:keys [unprocessed] :as resp}]
  (or (maybe-unprocessed-error unprocessed)
      (with-meta {} resp)))

(def renames
  {:consumed-capacity :capacity
   :attributes        :item
   :table-names       :tables
   :item-collection-metrics :metrics
   :last-evaluated-table-name :end-table
   :last-evaluated-key        :end-key
   :unprocessed-items :unprocessed})

(def structure-groups
  ;; we want the same semantics for all item-returning functions, so we'll
  ;; filter target through this before restructure-response* dispatch
  {:put-item    :get-item
   :update-item :get-item
   :delete-item :get-item})

(defn restructure-response [target m]
  (let [m (reduce
           (fn [acc [k v]]
             (transform-response-kv k v acc target))
           nil m)]
    (restructure-response*
     (structure-groups target target)
     (set/rename-keys m renames))))
