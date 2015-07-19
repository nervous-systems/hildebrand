(ns hildebrand.channeled
  (:require [hildebrand.core :as h]
            [glossop.util :refer [onto-chan?]]
            [eulalie.support]
            [plumbing.core :refer [grouped-map]]
            #?@ (:clj
                 [[glossop.core :refer [go-catching <?]]
                  [clojure.core.async :as async :refer [alt!]]]
                 :cljs
                 [[cljs.core.async :as async]]))
  #? (:cljs (:require-macros [glossop.macros :refer [go-catching <?]]
                             [cljs.core.async.macros :refer [alt!]])))

(defn paginate! [f input {:keys [limit maximum chan]}]
  (assert (or limit chan)
          "Please supply either a page-size (limit) or output channel")
  (let [chan (or chan (async/chan limit))]
    (go-catching
      (try
        (loop [start-key nil n 0]
          (let [items (<? (f (cond-> input start-key
                                     (assoc :start-key start-key))))
                n (+ n (count items))
                {:keys [end-key]} (meta items)]
            (if (and (<? (onto-chan? chan items))
                     end-key
                     (or (not maximum) (< n maximum)))
              (recur end-key n)
              (async/close! chan))))
        (catch #? (:clj Exception :cljs js/Error) e
          (async/>! chan e)
          (async/close! chan))))
    chan))

(defn query! [creds table where {:keys [limit] :as extra} & [{:keys [chan maximum]}]]
  (paginate!
   (partial h/query! creds table where)
   extra
   {:limit limit :maximum maximum :chan chan}))

(defn scan! [creds table {:keys [limit] :as extra} & [{:keys [chan maximum]}]]
  (paginate!
   (partial h/scan! creds table)
   extra
   {:limit limit :maximum maximum :chan chan}))


(defn- batch-send! [issue-fn batch error-chan]
  (go-catching
    (try
      (<? (issue-fn batch))
      (catch #? (:clj Exception :cljs js/Error) e
        (async/>! error-chan e)))))

(defn- batch-cleanup! [issue-fn batch error-chan]
  (go-catching
    (when (not-empty batch)
      (<? (batch-send! issue-fn batch error-chan)))
    (async/close! error-chan)))

(defn batching-channel
  [{:keys [issue-fn period-ms threshold in-chan error-chan timeout-fn]
    :or {period-ms 200 threshold 10 timeout-fn async/timeout}}]
  (let [in-chan    (or in-chan (async/chan))
        error-chan (or error-chan (async/chan))]
    (go-catching
      (loop [batch []]
        (let [msg (if (not-empty batch)
                    (alt!
                      (timeout-fn period-ms) ::timeout
                      in-chan ([v] v))
                    (<? in-chan))]
          (if (nil? msg)
            (<? (batch-cleanup! issue-fn batch error-chan))
            (let [batch (cond-> batch (not= msg ::timeout) (conj msg))]
              (if (or (= msg ::timeout) (= threshold (count batch)))
                (do (<? (batch-send! issue-fn batch error-chan))
                    (recur []))
                (recur batch)))))))
    {:in-chan in-chan :error-chan error-chan}))

(defn- batching-something [op-tag {:keys [creds table batch-opts]}]
  (batching-channel
   (assoc batch-opts
          :issue-fn
          (fn [batch]
            (h/batch-write-item!
             creds {op-tag (if table
                             {table batch}
                             (grouped-map first second batch))})))))

(defn batching-puts [{:keys [creds table batch-opts] :as arg}]
  (batching-something :put arg))

(defn batching-deletes [{:keys [creds table batch-opts] :as arg}]
  (batching-something :delete arg))
