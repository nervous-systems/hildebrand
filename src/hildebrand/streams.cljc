(ns hildebrand.streams
  (:require [glossop.core :as g
             #? (:clj :refer :cljs :refer-macros) [go-catching <?]]
            #? (:clj
                [clojure.core.async :as async]
                :cljs
                [cljs.core.async :as async])
            [clojure.set :as set]
            [eulalie.dynamo-streams]
            [eulalie.support]
            [hildebrand.core :as hildebrand]
            [hildebrand.internal :as i]
            [hildebrand.internal.request :as request]
            [hildebrand.internal.response :as response]
            [hildebrand.internal.streams])
  #? (:cljs (:require-macros [hildebrand.streams :refer [defissuer]])))

#? (:clj
    (defmacro defissuer [target-name args & [doc]]
      `(eulalie.support/defissuer :dynamo-streams
         ~target-name ~args
         request/restructure-request
         response/restructure-response
         ~doc)))

(defissuer describe-stream    [stream-arn])
(defissuer get-records        [shard-iterator])
(defissuer get-shard-iterator [stream-arn shard-id shard-iterator-type])
(defissuer list-streams       [])

(defn latest-stream-arn! [creds table
                          & [args {:keys [chan close?] :or {close? true}}]]
  (cond->
      (go-catching
        (-> (apply hildebrand/describe-table! creds table args)
            <?
            :latest-stream-arn))
    chan (async/pipe chan close?)))
#? (:clj (def latest-stream-arn!! (comp g/<?! latest-stream-arn!)))
