(ns hildebrand.test.streams.channeled
  (:require [hildebrand.streams.channeled :as channeled]
            [hildebrand.streams :as streams]
            [hildebrand.core :as h]
            [hildebrand.test.common :as test.common
             :refer [create-table-indexed indexed-table
                     with-local-dynamo! greedy-paginate!]]
            [hildebrand.test.util #? (:clj :refer :cljs :refer-macros) [deftest is]]
            [glossop.util]
            [glossop.core #? (:clj :refer :cljs :refer-macros) [go-catching <?]]
            #? (:clj
                [clojure.core.async :as async :refer [alt!]]
                :cljs
                [cljs.core.async :as async]))
  #? (:cljs (:require-macros [cljs.core.async.macros :refer [alt!]])))

(deftest list-streams
  (with-local-dynamo!
    (fn [creds]
      (go-catching
        (is (< 1 (count (<? (greedy-paginate!
                             channeled/list-streams!
                             creds
                             {:limit 1})))))))))

(deftest get-records
  (let [item {:user-id "moea" :game-title "Super Mario"}]
    (with-local-dynamo!
      {create-table-indexed [item]}
      (fn [creds]
        (go-catching
          (<? (h/put-item! creds indexed-table (assoc item :update "yeah")))
          (let [stream-arn (<? (streams/latest-stream-arn! creds indexed-table))
                stream     (<? (streams/describe-stream! creds stream-arn))
                {:keys [shard-id]} (last (:shards stream))
                record-chan (channeled/get-records!
                             creds stream-arn shard-id :trim-horizon {:limit 1})]
            (alt! (async/timeout 1000) (is nil "Timeout")
                  (->> record-chan
                       (async/take 2)
                       (glossop.util/into []))
                  ([results]
                   (async/close! record-chan)
                   (glossop.core/throw-err results)
                   (is (= results
                          [[:insert item]
                           [:modify item (assoc item :update "yeah")]]))))))))))
