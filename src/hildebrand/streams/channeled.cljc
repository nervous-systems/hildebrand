(ns hildebrand.streams.channeled
  (:require [hildebrand.streams :as streams]
            [hildebrand.channeled :refer [paginate!]]
            [glossop.util :refer [onto-chan?]]
            #? (:clj
                [clojure.core.async :as async]
                :cljs
                [cljs.core.async :as async])
            [glossop.core #? (:clj :refer :cljs :refer-macros) [go-catching <?]]))

(defn list-streams! [creds {:keys [limit] :as extra} & [batch-opts]]
  (paginate!
   (partial streams/list-streams! creds)
   extra
   (assoc batch-opts :limit limit :start-key-name :exclusive-start-stream-arn)))

(defn get-shards! [creds stream-arn {:keys [limit] :as extra} & [batch-opts]]
  (paginate!
   (fn [& args]
     (go-catching
       (let [{:keys [shards] :as m}
             (<? (apply streams/describe-stream! creds stream-arn args))]
         (map #(with-meta % m) shards))))
   extra
   (assoc batch-opts :limit limit :start-key-name :exclusive-start-shard-id)))

(defn get-records! [creds stream-arn shard-id iterator-type
                    {:keys [limit sequence-number]}
                    & [{:keys [chan close?] :or {close? true}}]]
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
                (when close?
                  (async/close! chan)))))
          (catch #? (:clj Exception :cljs js/Error) e
                 (async/>! chan e)
                 (when close?
                   (async/close! chan))))))
    chan))
