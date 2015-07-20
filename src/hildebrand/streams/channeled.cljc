(ns hildebrand.streams.channeled
  (:require [hildebrand.streams :as streams]
            [hildebrand.channeled :refer [paginate!]]
            [glossop.util :refer [onto-chan?]]
            #?@ (:clj
                 [[glossop.core :refer [go-catching <?]]
                  [clojure.core.async :as async]]
                 :cljs
                 [[cljs.core.async :as async]]))
  #? (:cljs (:require-macros [glossop.macros :refer [go-catching <?]])))

(defn list-streams! [creds {:keys [limit] :as extra} & [batch-opts]]
  (paginate!
   (partial streams/list-streams! creds)
   extra
   (assoc batch-opts :limit limit :start-key-name :exclusive-start-stream-arn)))

(defn get-records! [creds stream-arn shard-id iterator-type
                    & [{:keys [limit chan sequence-number]}]]
  (assert (or limit chan)
          "Please supply either a page-size (limit) or output channel")
  (let [chan (or chan (async/chan limit))]
    (go-catching
      (let [iterator
            (<? (streams/get-shard-iterator!
                 creds stream-arn shard-id iterator-type
                 {:sequence-number sequence-number}))]
        (try
          (loop [iterator iterator]
            (let [records  (<? (streams/get-records! creds iterator {:limit limit}))
                  iterator (-> records meta :next-shard-iterator)]
              (if (and (<? (onto-chan? chan records)) iterator)
                (recur iterator)
                (async/close! chan))))
          (catch #? (:clj Exception :cljs js/Error) e
            (async/>! chan e)
            (async/close! chan)))))
    chan))
