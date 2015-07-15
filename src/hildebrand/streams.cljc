(ns hildebrand.streams
  (:require [eulalie.support]
            [eulalie.dynamo-streams]
            [hildebrand.core :as hildebrand]
            [hildebrand.internal.streams]
            [hildebrand.internal :as i]
            [hildebrand.internal.request :as request]
            [hildebrand.internal.response :as response]
            #?@ (:clj
                 [[glossop.core :refer [go-catching <? <?!]]
                  [clojure.core.async :as async]]
                 :cljs
                 [[cljs.core.async :as async]]))
  #? (:cljs (:require-macros [glossop.macros :refer [go-catching <?]]
                             [hildebrand.streams :refer [defissuer]])))

#? (:clj
    (defmacro defissuer [target-name args & [doc]]
      `(eulalie.support/defissuer :dynamo-streams
         ~target-name ~args
         request/restructure-request
         response/restructure-response
         ~doc)))

(defissuer describe-stream    [stream-id])
(defissuer get-records        [shard-iterator])
(defissuer get-shard-iterator [stream-id shard-id shard-iterator-type])
(defissuer list-streams       [])

(defn latest-stream-id! [creds table & args]
  (go-catching
    (let [{:keys [latest-stream-id]}
          (<? (apply hildebrand/describe-table! creds table args))]
      latest-stream-id)))
#? (:clj (def latest-stream-id!! (comp <?! latest-stream-id!)))
