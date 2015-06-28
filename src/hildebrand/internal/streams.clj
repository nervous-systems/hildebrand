(ns hildebrand.internal.streams
  (:require [hildebrand.util :as util]
            [eulalie.dynamo-streams]
            [plumbing.core :refer [update]]
            [hildebrand.internal.response :as response]))

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
