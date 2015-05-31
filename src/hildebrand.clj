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
   [hildebrand.request :refer [restructure-request]]
   [hildebrand.response :refer [restructure-response]]
   [plumbing.core :refer :all]
   [plumbing.map]))

(def aws-error->hildebrand
  {:resource-not-found-exception :resource-not-found
   :conditional-check-failed-exception :conditional-failed})

(defn rename-error [{{:keys [type] :as error} :error :as r}]
  (let [r (dissoc r :error)]
    (if error
      (assoc r :hildebrand/error
             (update error :type aws-error->hildebrand error))
      r)))

(defn issue-request! [{:keys [target] :as req}]
  (go-catching
    (let [resp (-> req
                   (assoc :service :dynamo)
                   (update :body (partial restructure-request target))
                   eulalie/issue-request!
                   <?
                   rename-error)]
      (if (:hildebrand/error resp)
        resp
        (restructure-response target (:body resp))))))

(def issue-request!! (comp <?! issue-request!))

(defmulti  error->throwable :type)
(defmethod error->throwable :default [{:keys [type message] :as error}]
  (ex-info (name type) error))

(defn issue-targeted-request! [target creds request]
  (go-catching
    (let [resp (<? (issue-request! {:target target :creds creds :body request}))]
      (if-let [error (when (map? resp) (:hildebrand/error resp))]
        (error->throwable error)
        resp))))

(defmacro defissuer [target-name args & [doc]]
  (let [fname!  (-> target-name name (str "!") symbol)
        fname!! (-> target-name name (str "!!") symbol)
        args'   (into '[creds] (conj args '& '[extra]))
        body  `(issue-targeted-request!
                ~(keyword target-name) ~'creds
                (merge (plumbing.map/keyword-map ~@args) ~'extra))
        md     (cond-> (meta target-name)
                 doc (assoc :doc doc))]
    `(do
       (defn ~(with-meta fname!  md) ~args' ~body)
       (defn ~(with-meta fname!! md) ~args' (<?! ~body)))))

(defissuer get-item [table key]
  "`key` is a map containing enough keys (either hash, or hash+range) to
uniquely identify an item in this table/index.")
(defissuer update-item [table key update]
  "`key` is a map containing enough keys (either hash, or hash+range) to
uniquely identify an item in this table.

`update` is a map of item attribute names to attribute values.

Returns an empty map unless `:return` is set to a reasonable value.")
(defissuer delete-item [table key]
  "`key` is a map containing enough keys (either hash, or hash+range) to
uniquely identify an item in this table.

Returns an empty map unless `:return` is set to to `:all-old`.")
(defissuer put-item [table item]
  "`item` is a map of attribute names to attribute values.

Returns an empty map unless `:return` is set to `:all-old`, and the put
overwrites an existing item." )
(defissuer describe-table [table])
(defissuer update-table   [table attrs]
  "`attrs` is a map of attribute names to symbolic types, describing the
attributes of the table." )
(defissuer list-tables    [])
(defissuer create-table   [])
(defissuer query          [table where]
  "`where` is a map of keys (either hash, or hash+range) to tagged comparison
 instructions, e.g. `{:hash [:= \"Hello\"] :range [:< 5]}`")
(defissuer delete-table   [table])
(defissuer batch-write-item []
  "Accepts `:put` and `:delete` keys, each optionally being set to a map of
table names to either lists of items to insert, or keys identifying items to
delete." )
(defissuer batch-get-item [items]
  "`items` is a map of table names to get requests, each a map containing at
 least `keys`, a list of keys identifying items to retrieve." )
(defissuer scan [table])

(defn table-status! [creds table]
  (go-catching
    (try
      (-> (describe-table! creds table) <? :status)
      (catch clojure.lang.ExceptionInfo e
        (when-not (= :resource-not-found (-> e ex-data :type))
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
