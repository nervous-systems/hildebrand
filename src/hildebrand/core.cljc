(ns hildebrand.core
  (:require [hildebrand.internal]
            [hildebrand.internal.request]
            [hildebrand.internal.response]
            [hildebrand.internal.util :as util]
            #?@(:clj [[clojure.core.async :as async]
                      [hildebrand.internal.macros :refer [<?!]]]
                :cljs [[cljs.core.async :as async]
                       [cljs.reader :as reader]
                       [hildebrand.internal.expr :as expr]]))
  #?(:cljs (:require-macros [hildebrand.internal.macros
                             :refer [defissuer-dynamo go-catching <?]])))

#? (:cljs
    (do
      (reader/register-tag-parser! "hildebrand/path"    expr/path-reader)
      (reader/register-tag-parser! "hildebrand/literal" expr/literal-reader)))

(defissuer-dynamo get-item [table key]
  "`key` is a map containing enough keys (either hash, or hash+range) to
uniquely identify an item in this table/index.")
(defissuer-dynamo update-item [table key update]
  "`key` is a map containing enough keys (either hash, or hash+range) to
uniquely identify an item in this table.

`update` is a map of item attribute names to attribute values.

Returns an empty map unless `:return` is set to a reasonable value.")
(defissuer-dynamo delete-item [table key]
  "`key` is a map containing enough keys (either hash, or hash+range) to
uniquely identify an item in this table.

Returns an empty map unless `:return` is set to to `:all-old`.")
(defissuer-dynamo put-item [table item]
  "`item` is a map of attribute names to attribute values.

Returns an empty map unless `:return` is set to `:all-old`, and the put
overwrites an existing item." )
(defissuer-dynamo describe-table [table])
(defissuer-dynamo update-table   [table]
  "`attrs` is a map of attribute names to symbolic types, describing the
attributes of the table." )
(defissuer-dynamo list-tables    [])
(defissuer-dynamo create-table   [])
(defissuer-dynamo query          [table where]
  "`where` is a map of keys (either hash, or hash+range) to tagged comparison
 instructions, e.g. `{:hash [:= \"Hello\"] :range [:< 5]}`")
(defissuer-dynamo delete-table   [table])
(defissuer-dynamo batch-write-item []
  "Accepts `:put` and `:delete` keys, each optionally being set to a map of
table names to either lists of items to insert, or keys identifying items to
delete." )
(defissuer-dynamo batch-get-item [items]
  "`items` is a map of table names to get requests, each a map containing at
 least `keys`, a list of keys identifying items to retrieve." )
(defissuer-dynamo scan [table])

(defn table-status! [creds table]
  (go-catching
    (try
      (-> (describe-table! creds table) <? :status)
      (catch
          #? (:clj clojure.lang.ExceptionInfo :cljs js/Error) e
          (when (not= :resource-not-found (-> e ex-data :type))
            (throw e))))))

#? (:clj (def table-status!! (comp <?! table-status!)))

;; Should probably do something smarter
(defn await-status! [creds table target-status & [{:keys [timeout] :or {timeout 250}}]]
  (go-catching
    (loop []
      (let [status (<? (table-status! creds table))]
        (cond (nil? status)            nil
              (= status target-status) status
              :else (do
                      (<? (async/timeout timeout))
                      (recur)))))))

#? (:clj (def await-status!! (comp <?! await-status!)))

(defn ensure-table! [creds {:keys [table] :as create}]
  (go-catching
    (let [status (<? (table-status! creds table))]
      (when-not status
        (<? (create-table! creds create)))
      (when-not (= :active status)
        (<? (await-status! creds table :active))))))

#? (:clj (def ensure-table!! (comp <?! ensure-table!)))

(defn scan-count! [creds table & [extra {:keys [chan close?] :or {close? true}}]]
  (cond->
      (go-catching
        (-> (scan! creds table (assoc extra :select :count))
            <?
            meta
            :count))
    chan (async/pipe chan close?)))

#? (:clj (def scan-count!! (comp <?! scan-count!)))

(defn query-count! [creds table where
                    & [extra {:keys [chan close?] :or {close? true}}]]
  (cond->
      (go-catching
        (-> (query! creds table where (assoc extra :select :count))
            <?
            meta
            :count))
    chan (async/pipe chan close?)))

#? (:clj (def query-count!! (comp <?! query-count!)))
