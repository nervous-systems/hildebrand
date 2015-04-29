(ns hildebrand
  (:require
   [clojure.set :as set]
   [clojure.string :as str]
   [clojure.walk :as walk]
   [clojure.core.async :as async]
   [eulalie]
   [eulalie.dynamo]
   [glossop :refer :all :exclude [fn-> fn->>]] 
   [hildebrand.util :refer :all]
   [hildebrand.util :refer :all]
   [hildebrand.request :refer [restructure-request]]
   [hildebrand.response :refer [restructure-response]] 
   [plumbing.core :refer :all]
   [plumbing.map]))

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
	(restructure-response target (:body resp))))))

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
(defissuer batch-write-item [])
(defissuer batch-get-item [])

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
