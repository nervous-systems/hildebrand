(ns hildebrand.streams
  (:require [clojure.walk :as walk]
            [eulalie]
            [plumbing.core :refer [update]]
            [eulalie.dynamo-streams]
            [hildebrand]
            [hildebrand.internal :as i]
            [hildebrand.internal.request :as request]
            [hildebrand.internal.response :as response]
            [hildebrand.util :as util]
            [glossop :refer [<? <?! go-catching]]))

(defmacro defissuer [target-name args & [doc]]
  `(i/defissuer :dynamo-streams
     ~target-name ~args
     request/restructure-request
     response/restructure-response
     ~doc))

(defn ->shard [m]
  (update m :sequence-number-range
          (fn [{start :starting-sequence-number
                end   :ending-sequence-number}]
            [(some-> start util/string->number)
             (some-> end   util/string->number)])))

(defmethod response/restructure-response* :describe-stream
  [_ {{:keys [key-schema shards] :as m} :stream-description}]
  (assoc m
         :key-schema (response/->key-schema key-schema)
         :shards     (map ->shard shards)))

(defmethod response/restructure-response* :get-shard-iterator
  [_ {:keys [shard-iterator]}]
  shard-iterator)

(def event-name+op->value-keys
  {[:new-and-old-images :insert] [:new-image]
   [:new-and-old-images :modify] [:old-image :new-image]
   [:new-and-old-images :remove] [:old-image]
   [:old-image :insert] [:keys]
   [:new-image :remove] [:keys]})

(defn ->tagged-record [event-name {:keys [stream-view-type] :as m}]
  (let [ks (get event-name+op->value-keys
                [stream-view-type event-name]
                [stream-view-type])]
    (into [event-name]
      (for [k ks]
        (response/->item (m k))))))

(defn ->record [{:keys [dynamodb event-name] :as m}]
  (with-meta
    (->tagged-record event-name dynamodb)
    (update-in m [:dynamodb :sequence-number] util/string->number)))

(defmethod response/restructure-response* :get-records
  [_ {:keys [records] :as m}]
  (with-meta
    (map ->record records)
    (dissoc m :records)))

(defissuer describe-stream    [stream-id])
(defissuer get-records        [shard-iterator])
(defissuer get-shard-iterator [stream-id shard-id shard-iterator-type])
(defissuer list-streams       [])

(defn latest-stream-id! [creds table & args]
  (go-catching
    (let [{:keys [latest-stream-id]}
          (<? (apply hildebrand/describe-table! creds table args))]
      latest-stream-id)))
(def latest-stream-id!! (comp <?! latest-stream-id!))
