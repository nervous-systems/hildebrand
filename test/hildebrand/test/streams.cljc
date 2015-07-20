(ns hildebrand.test.streams
  (:require [hildebrand.streams :as streams]
            [hildebrand.test.common :as test.common
             :refer [create-table-indexed indexed-table with-local-dynamo!]]
            #?@ (:clj
                 [[clojure.test :refer [is]]
                  [hildebrand.test.async :refer [deftest]]
                  [glossop.core :refer [go-catching <?]]]
                 :cljs
                 [[cemerick.cljs.test]]))
  #? (:cljs
      (:require-macros [glossop.macros :refer [<? go-catching]]
                       [hildebrand.test.async.macros :refer [deftest]]
                       [cemerick.cljs.test :refer [is]])))

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

