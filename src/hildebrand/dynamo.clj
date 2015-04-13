(ns hildebrand.dynamo
  (:require
   [clojure.set :as set]
   [clojure.string :as str]
   [clojure.walk :as walk]
   [eulalie]
   [eulalie.dynamo]
   [glossop :refer :all :exclude [fn-> fn->>]]
   [hildebrand.dynamo.schema :as schema]
   [hildebrand.dynamo.expr :as expr :refer [flatten-expr]]
   [hildebrand.util :refer :all]
   [hildebrand.dynamo.util :refer :all]
   [plumbing.core :refer :all]
   [plumbing.map]))

(defn join-keywords [& args]
  (some->> args not-empty (map name) str/join keyword))

(defn from-attr-value [[tag value]]
  (condp = tag
    :S    value
    :N    (string->number value)
    :M    (map-vals (partial apply from-attr-value) value)
    :L    (mapv     (partial apply from-attr-value) value)
    :BOOL (boolean value)
    :NULL nil
    :SS   (into #{} value)
    :NS   (into #{} (map string->number value))))

(defn attr [t v] {t v})

(defn to-set-attr [v]
  (let [v (throw-empty v)]
    (cond
      (every? string? v)  {:SS (map fq-name v)}
      (every? ddb-num? v) {:NS (map str     v)}
      :else (throw (Exception. "Invalid set type")))))

(defn to-attr-value [v]
  (let [v (branch-> v keyword? fq-name)]
    (branch v
      string?  {:S (throw-empty v)}
      nil?     {:NULL true}
      boolean? {:BOOL v}
      ddb-num? {:N (str v)}
      vector?  {:L (map to-attr-value v)}
      map?     {:M (for-map [[k v'] v]
                     (name k) (to-attr-value v'))}
      set?     (to-set-attr v)
      (throw (Exception. (str "Invalid value " (type v)))))))

(defn key-schema-item [[k v]]
  {:attribute-name (name k)
   :key-type       v})

(def default-throughput {:read 1 :write 1})

(def ->throughput
  (key-renamer {:read  :read-capacity-units
                :write :write-capacity-units}))

(defn ->attr-value [[k v]]
  {:attribute-name (name k)
   :attribute-type v})

(def ->create-table
  (fn->>
   (transform-map
    {:table [:table-name name]
     :throughput [:provisioned-throughput
                  (fn->> (merge default-throughput) ->throughput)]
     :attrs [:attribute-definitions (partial map ->attr-value)]
     :keys [:key-schema (partial map key-schema-item)]})
   (schema/conforming schema/CreateTable*)))

(def ->delete-table
  (fn->> (transform-map {:table [:table-name name]})
         (schema/conforming schema/DeleteTable*)))

(def ->describe-table
  (fn->> (transform-map {:table [:table-name name]})
         (schema/conforming schema/DescribeTable*)))

(defn ->item-spec [m]
  (for-map [[k v] m]
    (name k) (to-attr-value v)))

(defn raise-condition-expression [{:keys [condition] :as req}]
  (let [{:keys [expr values attrs] :as condition}
        (if (map? condition)
          condition
          {:exprs condition})]
    (cond-> (dissoc req :condition)
      (not-empty values) (update :expression-attribute-values
                                 merge (for-map [[k v] values]
                                         (str k) (to-attr-value v)))
      (not-empty expr)  (assoc :condition-expression
                               (expr/build-condition-expr condition)))))

(defn raise-update-expression [{update' :update :as req}]
  (let [{:keys [values exprs attrs]}
        (map-vals not-empty (expr/build-update-expr update'))]
    (cond-> (dissoc req :update)
      values (update
              :expression-attribute-values
              merge (map-vals to-attr-value values))
      attrs  (update :expression-attribute-names merge attrs)
      exprs  (assoc :update-expression exprs))))

(defn ->batch-req [type m]
  (let [m (->item-spec m)]
    (case type
      :delete {:delete-request {:key  m}}
      :put    {:put-request    {:item m}})))

(defn raise-batch-ops [k m]
  (reduce
   (fn [m [table ops]]
     (let [ops (map (partial ->batch-req k) ops)]
       (update-in m [:request-items table] concat ops)))
   (dissoc m k) (m k)))

(def ->batch-write
  (fn->>
   (raise-batch-ops :delete)
   (raise-batch-ops :put)))

(def ->put-item
  (fn->>
   (transform-map
    {:table [:table-name name]
     :item  [:item ->item-spec]
     :return :return-values
     :capacity :return-consumed-capacity})
   (schema/conforming schema/PutItem*)))

(def get-item-common
  {:capacity :return-consumed-capacity
   :attrs [:projection-expression (fn->> (map name) (str/join " "))]
   :consistent :consistent-read})

(def ->get-item
  (fn->>
   (transform-map
    (merge get-item-common
           {:table [:table-name name]
            :key    ->item-spec}))
   (schema/conforming schema/GetItem*)))

(defn ->batch-get [m]
  {:request-items
   (for-map [[table req] m]
     table (transform-map
            (assoc get-item-common
                   :keys (mapper ->item-spec))
            req))})

(def ->update-item
  (fn->>
   (transform-map
    {:table [:table-name name]
     :key   ->item-spec
     :return :return-values})
   raise-update-expression
   raise-condition-expression))

(def ->delete-item
  (fn->>
   (transform-map
    {:table [:table-name name]
     :key     ->item-spec
     :capacity :return-consumed-capacity
     :consistent :consistent-read})
   raise-condition-expression
   (schema/conforming schema/DeleteItem*)))

(defmulti transform-request :target)
(defmulti-dispatch
  transform-request
  (map-vals
   (fn [f] (fn-> :body f))
   {:create-table   ->create-table
    :put-item       ->put-item
    :delete-table   ->delete-table
    :get-item       ->get-item
    :delete-item    ->delete-item
    :describe-table ->describe-table
    :update-item    ->update-item
    :batch-write-item ->batch-write
    :batch-get-item   ->batch-get}))

(def <-attr-def (juxt :attribute-name :attribute-type))

(def <-key-schema
  (partial keyed-map :attribute-name :key-type))

(def <-throughput
  (key-renamer
   {:read-capacity-units :read
    :write-capacity-units :write
    :number-of-decreases-today :decreases
    :last-increase-date-time :last-increase
    :last-decrease-date-time :last-decrease}))

(def <-projection
  (key-renamer
   {:non-key-attributes :attrs
    :projection-type    :type}))

(def <-global-index
  (map-transformer
   {:index-name :name
    :index-size-bytes :size
    :index-status :status
    :item-count :count
    :key-schema [:keys <-key-schema]
    :projection <-projection
    :provisioned-throughput [:throughput <-throughput]}))

;; that bidirectional transformation function though

(def <-table-description-body
  (map-transformer
   {:attribute-definitions [:attrs (fn->> (map <-attr-def) (into {}))]
    :key-schema [:keys <-key-schema]
    :provisioned-throughput [:throughput <-throughput]
    :table-status :status
    :table-size-byes :size
    :creation-date-time :created
    :global-secondary-indexes [:global-indexes (mapper <-global-index)]}))

(def <-create-table
  (fn->> :table-description <-table-description-body))

(def <-consumed-capacity
  (map-transformer
   {:consumed-capacity
    [:capacity (partial
                walk/prewalk-replace
                {:capacity-units :capacity
                 :table-name     :table})]}))

(defn <-get-item [{:keys [item] :as resp}]
  (let [resp (<-consumed-capacity resp)]
    (with-meta
      (map-vals (partial apply from-attr-value) item)
      (dissoc resp :item))))

(defn <-put-item [{:keys [attributes] :as resp}]
  (let [resp (<-consumed-capacity resp)]
    (with-meta
      (map-vals (partial apply from-attr-value) attributes)
      (dissoc resp :attributes))))

(defn <-update-item [{:keys [attributes] :as resp}]
  (let [resp (<-consumed-capacity resp)]
    (with-meta
      (map-vals (partial apply from-attr-value) attributes)
      (dissoc resp :attributes))))

(defn error [type message & [data]]
  (assoc data
         :hildebrand/error
         {:type type :message message}))

(defn <-batch-write [{:keys [unprocessed-items] :as resp}]
  (if (not-empty unprocessed-items)
    (error :unprocessed-items
           (format "%d unprocessed items" (count unprocessed-items))
           {:unprocessed unprocessed-items})
    (let [resp (<-consumed-capacity resp)]
      (with-meta {} resp))))

(defn <-batch-get [{:keys [unprocessed-items] :as resp}]
  (if (not-empty unprocessed-items)
    (error :unprocessed-items
           (format "%d unprocessed items" (count unprocessed-items))
           {:unprocessed unprocessed-items})
    (let [resp (<-consumed-capacity resp)]
      (with-meta {} resp))))

(defn <-delete-item [resp]
  (<-consumed-capacity resp))

(defmulti  transform-response :target)
(defmethod transform-response :default [{:keys [body]}] body)
(defmulti-dispatch
  transform-response
  (map-vals
   #(comp % :body)
   {:batch-write-item  <-batch-write
    :batch-get-item    <-batch-get
    :create-table      <-create-table
    :delete-table      <-create-table
    :get-item          <-get-item
    :put-item          <-put-item
    :delete-item       <-delete-item
    :update-item       <-update-item
    :describe-table  (fn-> :table <-table-description-body)}))

(defmulti  transform-error
  (fn [{target :target {:keys [type]} :hildebrand/error}] [target type]))

(defmethod transform-error :default [m]
  (set/rename-keys m {:error :hildebrand/error}))

(defmulti-dispatch
  transform-error
  {[:describe-table :resource-not-found-exception]
   (fn-> (assoc :body {}) (dissoc :hildebrand/error))})

(defn issue-request! [creds {:keys [target] :as req}]
  (clojure.pprint/pprint (transform-request req))
  (let [req (assoc req :creds creds)] ;; TODO move this into eulalie
    (go-catching
      (let [resp (-> (eulalie/issue-request!
                      eulalie.dynamo/service
                      creds
                      (assoc req :content (transform-request req)))
                     <?
                     (assoc :target target
                            :hildebrand/request req)
                     (set/rename-keys {:error :hildebrand/error}))]
        (if (:hildebrand/error resp)
          (do
            ;; TODO response?
            (clojure.pprint/pprint (-> resp :response :body))
            (transform-error resp))
          (let [resp (transform-response resp)]
            (branch-> resp :hildebrand/error transform-error)))))))

(def issue-request!! (comp <?! issue-request!))
