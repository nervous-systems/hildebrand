(ns hildebrand.page
  (:require [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [hildebrand :as h]
            [glossop :refer :all]))

(defn onto-chan?
  "This is a version of onto-chan which never closes the target
  channel, and returns a boolean on its own channel, indicating
  whether all puts were completed"
  ([ch coll]
   (async/go-loop [vs (seq coll)]
     (if vs
       (when (async/>! ch (first vs))
         (recur (next vs)))
       true))))

(defn query! [creds table where & [{:keys [limit] :as extra} {:keys [chan maximum]}]]
  (assert (or limit chan)
          "Please supply either a page-size (limit) or output channel")
  (let [chan (or chan (async/chan limit))]
    (go-catching
      (try
        (loop [start-key nil n 0]
          (log/info "OK" start-key n)
          (let [{:keys [items last-evaluated-key]}
                (<? (h/query! creds table where
                              (cond-> extra start-key
                                      (assoc :start-key start-key))))
                n (+ n (count items))]
            (log/info "WRITING ONTO CHAN " items)
            (if (and (<? (onto-chan? chan items))
                     last-evaluated-key
                     (or (not maximum) (< n maximum)))
              (recur last-evaluated-key n)
              (async/close! chan))))
        (catch Exception e
          (async/>! chan e)
          (async/close! chan))))
    chan))
