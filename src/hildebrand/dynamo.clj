(ns hildebrand.dynamo
  (:require
   [clojure.set :as set]
   [clojure.string :as str]
   [clojure.walk :as walk]
   [clojure.core.async :as async]
   [eulalie]
   [eulalie.dynamo]
   [glossop :refer :all :exclude [fn-> fn->>]]
   [hildebrand.dynamo.schema :as schema]
   [hildebrand.util :refer :all]
   [hildebrand.dynamo.util :refer :all]
   [hildebrand.dynamo.request :refer [restructure-request]]
   [plumbing.core :refer :all]
   [plumbing.map]))

(defmulti  rewrite-table-out identity)
(defmethod rewrite-table-out :default [x] x)

(defmulti  rewrite-table-in identity)
(defmethod rewrite-table-in :default [x] x)

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

(defn <-attr-def [{:keys [attribute-name attribute-type]}]
  [attribute-name (type-aliases-in attribute-type attribute-type)])

(def <-key-schema
  (fn->> (sort-by :key-type)
	 (map :attribute-name)))

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
   {:index-name [:name rewrite-table-in]
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
    {:table-name [:table rewrite-table-in]
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
		   (rewrite-table-in t) (map <-item items))
	resp))))

(defn <-delete-item [resp]
  (<-consumed-capacity resp))

(defn <-list-tables [{:keys [last-evaluated-table-name table-names]}]
  (with-meta {:tables (map rewrite-table-in table-names)}
    {:start-table (some-> last-evaluated-table-name rewrite-table-in)}))

(defn <-query [resp]
  (update resp :items (mapper <-item)))

(defmulti  transform-response (fn [target body] target))
(defmethod transform-response :default [_ body] body)
(defmulti-dispatch
  transform-response
  (map-vals
   (fn [f] #(f %2))
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

(defn issue-request! [{:keys [target] :as req}]
  (go-catching
    (let [resp (-> req
		   (assoc :service :dynamo)
		   (update :body (partial restructure-request target))
		   eulalie/issue-request!
		   <?
		   (set/rename-keys {:error :hildebrand/error}))]
      (if (:hildebrand/error resp)
	resp
	(transform-response target (:body resp))))))

(def issue-request!! (comp <?! issue-request!))

(defmulti  error->throwable :type)
(defmethod error->throwable :default [{:keys [type message] :as error}]
  (ex-info (name type) error))

(defn issue-targeted-request! [target creds request & [mangle]]
  (go-catching
    (let [{:keys [hildebrand/error] :as resp}
	  (<? (issue-request! {:target target :creds creds :body request}))]
      (if error
	(error->throwable error)
	(cond-> resp mangle mangle)))))

(defmacro defissuer [target-name args & [mangle]]
  (let [fname!  (-> target-name name (str "!"))
	fname!! (str fname! "!")
	args'   (into '[creds] (conj args '& '[extra]))
	body  `(issue-targeted-request!
		~(keyword target-name) ~'creds
		(merge (plumbing.map/keyword-map ~@args) ~'extra)
		~(or mangle identity))]
    `(do
       (defn ~(symbol fname!)  ~args' ~body)
       (defn ~(symbol fname!!) ~args' (<?! ~body)))))

(defissuer get-item    [table key])
(defissuer update-item [table key update])
(defissuer delete-item [table key])
(defissuer put-item    [table item])
(defissuer describe-table [table])
(defissuer update-table   [table])
(defissuer list-tables    [])
(defissuer create-table   [])
(defissuer query          [table where])
(defissuer delete-table   [table])

(defn table-status! [creds table]
  (go-catching
    (try
      (-> (describe-table! creds table) <? :status)
      (catch clojure.lang.ExceptionInfo e
	(when-not (= :resource-not-found-exception (-> e ex-data :type))
	  (throw e))))))

(def table-status!! (comp <?! table-status!))

(defn await-status! [creds table status]
  (go-catching
    (loop []
      (let [status' (<? (table-status! creds table))]
	(cond (nil? status')     nil
	      (= status status') status'
	      :else (do
		      (<? (async/timeout 1000))
		      (recur)))))))

(def await-status!! (comp <?! await-status!))

(defn ensure-table! [creds {:keys [table] :as create}]
  (go-catching
    (let [status (<? (table-status! creds table))]
      (when-not status
	(<? (create-table! creds create)))
      (when-not (= :active status)
	(<? (await-status! creds table :active))))))

(def ensure-table!! (comp <?! ensure-table!))
