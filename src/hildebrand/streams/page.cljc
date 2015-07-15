(ns hildebrand.streams.page
  (:require [hildebrand.streams :as streams]
            [glossop.util :refer [onto-chan?]]
            #?@ (:clj
                 [[glossop.core :refer [go-catching <?]]
                  [clojure.core.async :as async]]
                 :cljs
                 [[cljs.core.async :as async]]))
  #? (:cljs (:require-macros [glossop.macros :refer [go-catching <?]])))

(defn get-records! [creds stream-id shard-id iterator-type
                    & [{:keys [limit chan sequence-number]}]]
  (assert (or limit chan)
          "Please supply either a page-size (limit) or output channel")
  (let [chan (or chan (async/chan limit))]
    (go-catching
      (let [iterator
            (<? (streams/get-shard-iterator!
                 creds stream-id shard-id iterator-type
                 {:sequence-number sequence-number}))]
        (try
          (loop [iterator iterator]
            (let [records  (<? (streams/get-records! creds iterator {:limit limit}))
                  iterator (-> records meta :next-shard-iterator)]
              (if (and (<? (onto-chan? chan records)) iterator)
                (recur iterator)
                (async/close! chan))))
          (catch Exception e
            (async/>! chan e)
            (async/close! chan)))))
    chan))
