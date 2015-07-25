(ns hildebrand.test.streams
  (:require [hildebrand.streams :as streams]
            [glossop.core #? (:clj :refer :cljs :refer-macros) [go-catching <?]]
            [hildebrand.test.common :as test.common
             :refer [create-table-indexed indexed-table with-local-dynamo!]]
            [hildebrand.test.util #? (:clj :refer :cljs :refer-macros) [deftest is]]))

(deftest list-streams
  (with-local-dynamo! [create-table-indexed]
    (fn [creds]
      (go-catching
        (let [streams (<? (streams/list-streams! creds {:limit 1}))]
          (is (some :stream-arn streams))
          (is (string? (:exclusive-start-stream-arn (meta streams)))))))))

(defn describe-stream! [creds]
  (go-catching
    (let [[{:keys [stream-arn]}]
          (<? (streams/list-streams! creds {:limit 1}))]
      (<? (streams/describe-stream! creds stream-arn)))))

(deftest describe-stream
  (with-local-dynamo! [create-table-indexed]
    (fn [creds]
      (go-catching
        (let [stream (<? (describe-stream! creds))]
          (is (:stream-arn stream)))))))

(deftest get-shard-iterator
  (with-local-dynamo! [create-table-indexed]
    (fn [creds]
      (go-catching
        (let [{stream-arn :stream-arn
               [{:keys [shard-id]}] :shards} (<? (describe-stream! creds))]
          (is (string?
               (<? (streams/get-shard-iterator!
                    creds
                    stream-arn
                    shard-id
                    :trim-horizon)))))))))
