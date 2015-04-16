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

(defn ->projection [[tag & [arg]]]
  (cond-> {:projection-type tag}
    arg (assoc :non-key-attributes arg)))

(def ->key-schema
  (fn->>
   (sort-by val) ;; :hash, then :range
   (map key-schema-item)))

(def index-common
  {:name :index-name
   :keys [:key-schema ->key-schema]
   :project [:projection ->projection]})

(def ->gs-index
  (map-transformer
   (assoc index-common
          :throughput [:provisioned-throughput
                       (fn->> (merge default-throughput) ->throughput)])))

(def ->ls-index
  (map-transformer index-common))

(defn raise-create-indexes [{{:keys [global local]} :indexes :as req}]
  (cond-> (dissoc req :indexes)
    (not-empty global) (assoc
                        :global-secondary-indexes
                        (map ->gs-index global))
    (not-empty local) (assoc
                       :local-secondary-indexes
                       (map ->ls-index local))))

(def ->create-table
  (fn->>
   (transform-map
    {:table [:table-name name]
     :throughput [:provisioned-throughput
                  (fn->> (merge default-throughput) ->throughput)]
     :attrs [:attribute-definitions (partial map ->attr-value)]
     :keys [:key-schema ->key-schema]})
   raise-create-indexes
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

(defn ->item [m]
  (for-map [[k v] m]
    (name k) (to-attr-value v)))

(defn raise-condition-expression
  [req & [{:keys [out-key in-key] :or {out-key :condition-expression
                                       in-key  :condition}}]]
  (if-let [{:keys [expr values attrs]}
           (some-> in-key req not-empty expr/cond-expr->statement)]
    (cond-> (-> req (dissoc in-key) (assoc out-key expr))
      (not-empty values) (update
                          :expression-attribute-values
                          merge (->item values))
      (not-empty attrs)  (update
                          :expression-attribute-names
                          merge attrs))
    req))

(defn raise-update-expression [{update' :update :as req}]
  (if (not-empty update')
    (let [{:keys [values expr attrs]}
          (map-vals not-empty (expr/update-ops->statement update'))]
      (cond-> (dissoc req :update)
        values (update
                :expression-attribute-values
                merge (map-vals to-attr-value values))
        attrs  (update :expression-attribute-names merge attrs)
        expr   (assoc :update-expression expr)))
    req))

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

(def comparison-ops {:< :lt :<= :le := :eq :> :gt :>= :ge})

(defn ->key-conds [conds]
  (for-map [[col [op & args]] conds]
    col {:attribute-value-list (map to-attr-value args)
         :comparison-operator (comparison-ops op op)}))

(defn ->query [m]
  (let [m (transform-map
           {:index :index-name
            :table :table-name
            :where [:key-conditions ->key-conds]
            :attrs [:projection-expression (fn->> (map name) (str/join " "))]
            :consistent :consistent-read
            :sort [:scan-index-forward (partial = :asc)]}
           m)]
    (raise-condition-expression
     m
     {:in-key :filter
      :out-key :filter-expression})))

(defn ->put-item [m]
  (let [m
        (transform-map
         {:table [:table-name name]
          :item  [:item ->item-spec]
          :return :return-values
          :capacity :return-consumed-capacity} m)]
    (raise-condition-expression
     m
     {:in-key :condition
      :out-key :condition-expression})))

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

(defn ->index-updates [updates]
  (for [[tag m] updates]
    {tag (->gs-index m)}))

(def ->update-table
  (fn->>
   (transform-map
    {:table :table-name
     :attrs [:attribute-definitions (mapper ->attr-value)]
     :throughput [:provisioned-throughput ->throughput]
     :indexes [:global-secondary-index-updates ->index-updates]})))

(def ->list-tables
  (map-transformer
   {:start-table [:exclusive-start-table-name]}))

(defmulti transform-request :target)
(defmulti-dispatch
  transform-request
  (map-vals
   (fn [f] (fn-> :body f))
   {:query          ->query
    :list-tables    ->list-tables
    :create-table   ->create-table
    :put-item       ->put-item
    :delete-table   ->delete-table
    :get-item       ->get-item
    :delete-item    ->delete-item
    :describe-table ->describe-table
    :update-item    ->update-item
    :batch-write-item ->batch-write
    :batch-get-item   ->batch-get
    :update-table     ->update-table}))

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

(defn <-projection [{attrs :non-key-attributes type :projection-type}]
  (cond-> [type]
    (not-empty attrs) (conj attrs)))

(def <-global-index
  (map-transformer
   {:index-name :name
    :index-size-bytes :size
    :index-status :status
    :item-count :count
    :key-schema [:keys <-key-schema]
    :projection [:project <-projection]
    :provisioned-throughput [:throughput <-throughput]}))

;; that bidirectional transformation function though

(defn raise-indexes [{:keys [global-indexes local-indexes] :as req}]
  (-> req
      (dissoc :global-indexes :local-indexes)
      (assoc :indexes {:global global-indexes
                       :local  local-indexes})))

(def <-table-description-body
  (fn->>
   (transform-map
    {:table-name :table
     :attribute-definitions [:attrs (fn->> (map <-attr-def) (into {}))]
     :key-schema [:keys <-key-schema]
     :provisioned-throughput [:throughput <-throughput]
     :table-status :status
     :table-size-bytes :size
     :item-count :items
     :creation-date-time :created
     :global-secondary-indexes [:global-indexes (mapper <-global-index)]
     :local-secondary-indexes  [:local-indexes  (mapper <-global-index)]})
   raise-indexes))

(def <-create-table
  (fn->> :table-description <-table-description-body))

(def <-consumed-capacity
  (map-transformer
   {:consumed-capacity
    [:capacity (partial
                walk/prewalk-replace
                {:capacity-units :capacity
                 :table-name     :table})]}))

(def <-item (partial map-vals (partial apply from-attr-value)))

(defn <-wrapped-item [item-k resp]
  (let [resp (<-consumed-capacity resp)]
    (with-meta
      (-> resp item-k <-item)
      (dissoc resp item-k))))

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

(defn <-batch-get [{:keys [unprocessed-items responses] :as resp}]
  (if (not-empty unprocessed-items)
    (error :unprocessed-items
           (format "%d unprocessed items" (count unprocessed-items))
           {:unprocessed unprocessed-items})
    (let [resp (<-consumed-capacity resp)]
      (with-meta (for-map [[t items] responses]
                   t (map <-item items))
        resp))))

(defn <-delete-item [resp]
  (<-consumed-capacity resp))

(defn <-list-tables [{:keys [last-evaluated-table-name table-names]}]
  (with-meta table-names {:start-table last-evaluated-table-name}))

(defn <-query [resp]
  (update resp :items (mapper <-item)))

(defmulti  transform-response :target)
(defmethod transform-response :default [{:keys [body]}] body)
(defmulti-dispatch
  transform-response
  (map-vals
   #(comp % :body)
   {:list-tables       <-list-tables
    :batch-write-item  <-batch-write
    :batch-get-item    <-batch-get
    :create-table      <-create-table
    :delete-table      <-create-table
    :get-item          (partial <-wrapped-item :item)
    :put-item          (partial <-wrapped-item :attributes)
    :update-item       (partial <-wrapped-item :attributes)
    :delete-item       (partial <-wrapped-item :attributes)
    :describe-table    (fn-> :table <-table-description-body)
    :update-table      (fn-> :table-description <-table-description-body)
    :query             <-query}))

(defmulti  transform-error
  (fn [{target :target {:keys [type]} :hildebrand/error}] [target type]))

(defmethod transform-error :default [m]
  (set/rename-keys m {:error :hildebrand/error}))

(defn issue-request! [creds {:keys [target] :as req}]
  (clojure.pprint/pprint (transform-request req))
  (let [req (assoc req :creds creds)] ;; TODO move this into eulalie
    (go-catching
      (let [resp (-> (eulalie/issue-request!
                      eulalie.dynamo/service
                      creds
                      (assoc req :content (transform-request req)))
                     <?
                     (dissoc :request)
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
