(ns hildebrand.streams
  (:require [hildebrand.internal.streams]
            [hildebrand.internal :as i]
            [hildebrand.internal.request :as request]
            [hildebrand.internal.response :as response]
            [glossop :refer [<? <?! go-catching]]))

(defmacro defissuer [target-name args & [doc]]
  `(i/defissuer :dynamo-streams
     ~target-name ~args
     request/restructure-request
     response/restructure-response
     ~doc))

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
