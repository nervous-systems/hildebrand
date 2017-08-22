(ns hildebrand.internal.response
  (:require [clojure.set :as set]
            [clojure.walk :as walk]
            [eulalie.platform :refer [b64-string->ba]]
            [hildebrand.internal.util :refer [type-aliases-in defmulti-dispatch]]
            [hildebrand.internal.platform.number :refer [string->number]]
            [plumbing.core :refer [map-vals #?@ (:clj [fn-> fn->> for-map])]])
  #? (:cljs (:require-macros [plumbing.core :refer [fn-> fn->> for-map]])))

(defn from-attr-value [m]
  (let [[[tag value]] (seq m)]
    (case tag
      :S    value
      :N    (string->number value)
      :M    (map-vals from-attr-value value)
      :L    (mapv     from-attr-value value)
      :BOOL (boolean value)
      :NULL nil
      :SS   (into #{} (map keyword value))
      :NS   (into #{} (map string->number value))
      :BS   (into #{} (map b64-string->ba value))
      :B    (b64-string->ba value))))

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

(defmulti  transform-table-kv (fn [m k v] k))
(defmethod transform-table-kv :default [m k v]
  (assoc m k v))

(defmulti-dispatch transform-table-kv
  (map-vals
   (fn [handler]
     (fn [m k v] (assoc m k (handler v))))
   {:attribute-definitions #(into {} (map ->attr-def %))
    :key-schema ->key-schema
    :provisioned-throughput ->throughput
    :creation-date-time #(Math/round ^Double (* 1000 %))}))

(defmethod transform-table-kv :global-secondary-indexes [m k v]
  (update-in m [:indexes :global] into (map ->global-index v)))

(defmethod transform-table-kv :local-secondary-indexes [m k v]
  (update-in m [:indexes :local] into (map ->local-index v)))

(defmulti  transform-response-kv
  (fn [m k v target]
    (keyword "hildebrand.response-key" (name k))))
(defmethod transform-response-kv :default [m k v _]
  (assoc m (keyword (name k)) v))

(derive :hildebrand.response-key/table :hildebrand.response-key/table-description)

(defmethod transform-response-kv
  :hildebrand.response-key/table-description [m k v _]
  (set/rename-keys
   (reduce
    (fn [acc [k v]]
      (transform-table-kv acc k v))
    nil v)
   table-description-renames))

(defmethod transform-response-kv
  :hildebrand.response-key/consumed-capacity [m k v _]
  (assoc m k (walk/prewalk-replace
              {:capacity-units :capacity
               :table-name     :table}
              v)))

(derive :hildebrand.response-key/items      :hildebrand.response-key/item)
(derive :hildebrand.response-key/attributes :hildebrand.response-key/item)

(defmethod transform-response-kv :hildebrand.response-key/item [m k v _]
  (assoc m k (if (map? v) (->item v) (map ->item v))))

(defn process-unprocessed [[table items]]
  (reduce
   (fn [acc i]
     (cond
       (:put-request i) (update-in acc [:put table] #(conj % (-> i :put-request :item ->item)))
       (:delete-request i)  (update-in acc [:delete table] #(conj % (-> i :delete-request :key ->item)))
       :else acc))
   nil items))

(defmethod transform-response-kv :hildebrand.response-key/unprocessed [m k v _]
  (update m k #(merge-with conj % (process-unprocessed v))))

(defmulti  restructure-response*
  (fn [target m] (keyword "hildebrand.response" (name target))))
(defmethod restructure-response* :default [_ m] m)

(derive :hildebrand.response/put-item    :hildebrand.response/get-item)
(derive :hildebrand.response/update-item :hildebrand.response/get-item)
(derive :hildebrand.response/delete-item :hildebrand.response/get-item)

(defmethod restructure-response* :hildebrand.response/get-item
  [_ {:keys [item] :as m}]
  (when item
    (with-meta item (dissoc m :item))))

(defmethod restructure-response* :hildebrand.response/list-tables
  [_ {:keys [tables] :as m}]
  (with-meta (or tables []) (dissoc m :tables)))

(derive :hildebrand.response/scan :hildebrand.response/query)

(defmethod restructure-response* :hildebrand.response/query
  [_ {:keys [items] :as m}]
  (with-meta (or items []) (dissoc m :items)))

(defn error [type message & [data]]
  {:hildebrand/error (assoc data :type type :message message)})

(defn maybe-unprocessed-error [unprocessed & [result]]
  (when (not-empty unprocessed)
    (error :unprocessed-items
           (str (count unprocessed) "unprocessed items")
           {:unprocessed unprocessed
            :result result})))

(defmethod restructure-response* :hildebrand.response/batch-get-item
  [_ {:keys [unprocessed responses] :as m}]
  (let [result (with-meta
                 (for-map [[t items] responses]
                   t (map ->item items))
                 m)]
    (or (maybe-unprocessed-error unprocessed result)
        result)))

(defmethod restructure-response* :hildebrand.response/batch-write-item
  [_ {:keys [unprocessed] :as resp}]
  (let [result (with-meta
                 (reduce
                  (fn [acc m]
                    (transform-response-kv acc :unprocessed m :batch-write-item))
                  nil unprocessed)
                 resp)]
    result))

(def renames
  {:consumed-capacity :capacity
   :attributes        :item
   :table-names       :tables
   :item-collection-metrics :metrics
   :last-evaluated-table-name :start-table
   :last-evaluated-key        :start-key
   :unprocessed-items :unprocessed})

(defn restructure-response [target m]
  (let [m (reduce
           (fn [acc [k v]]
             (transform-response-kv acc k v target))
           nil m)]
    (restructure-response*
     target
     (set/rename-keys m renames))))
