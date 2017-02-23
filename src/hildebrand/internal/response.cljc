(ns hildebrand.internal.response
  (:require [clojure.set :as set]
            [clojure.walk :as walk]
            ;; [eulalie.platform :refer [b64-string->ba]]
            [hildebrand.internal.util :as util]
            [hildebrand.internal.platform.number :refer [string->number]]))

(defn from-attr-value [m]
  (let [[[tag value]] (seq m)]
    (case tag
      :S    value
      :N    (string->number value)
      :M    (util/map-vals from-attr-value value)
      :L    (mapv     from-attr-value value)
      :BOOL (boolean value)
      :NULL nil
      :SS   (into #{} (map keyword value))
      :NS   (into #{} (map string->number value))
      ;; :BS   (into #{} (map b64-string->ba value)) ;; TODO
      ;; :B    (b64-string->ba value)
      )))

(defn ->attr-def [{:keys [attribute-name attribute-type]}]
  [attribute-name (util/type-aliases-in attribute-type attribute-type)])

(defn ->key-schema [k]
  (->> k (sort-by :key-type) (map :attribute-name)))

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

(defn ->global-index [index]
  (-> index
      (update :key-schema ->key-schema)
      (update :projection ->projection)
      (update :provisioned-throughput ->throughput)
      (set/rename-keys index-renames)))

(defn ->local-index [index]
  (-> index ->global-index (dissoc :throughput)))

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

(util/defmulti-dispatch transform-table-kv
  (util/map-vals
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

(def ->item (partial util/map-vals from-attr-value))

(derive :hildebrand.response-key/items      :hildebrand.response-key/item)
(derive :hildebrand.response-key/attributes :hildebrand.response-key/item)

(defmethod transform-response-kv :hildebrand.response-key/item [m k v _]
  (assoc m k (if (map? v) (->item v) (map ->item v))))

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
  (assoc data :hildebrand/error {:type type :message message}))

(defn maybe-unprocessed-error [unprocessed & [result]]
  (when (not-empty unprocessed)
    (error :unprocessed-items
           (str (count unprocessed) "unprocessed items")
           {:unprocessed unprocessed
            :result result})))

(defmethod restructure-response* :hildebrand.response/batch-get-item
  [_ {:keys [unprocessed responses] :as m}]
  (let [result (with-meta
                 (reduce (fn [acc [t items]]
                           (merge acc {t (map ->item items)}))
                         {}
                         responses)
                 m)]
    (or (maybe-unprocessed-error unprocessed result)
        result)))

(defmethod restructure-response* :hildebrand.response/batch-write-item
  [_ {:keys [unprocessed] :as resp}]
  (or (maybe-unprocessed-error unprocessed)
      (with-meta {} resp)))

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

(defn restructure-response* [target body]
  (let [resp (restructure-response target body)]
    (if (and (map? resp) (:hildebrand/error resp))
      (let [{{:keys [type] :as error} :hildebrand/error} resp]
        (ex-info (name type) error))
      resp)))
