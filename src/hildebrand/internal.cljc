(ns hildebrand.internal
  (:require [hildebrand.internal.util :as util]
            #?@(:clj [[clojure.core.async :as async]]
                :cljs [[cljs.core.async :as async]])
            #_[eulalie.dynamo]))

(def aws-error->hildebrand
  {:resource-not-found-exception :resource-not-found
   :conditional-check-failed-exception :conditional-failed})

(defmethod util/translate-error-type :eulalie.service/dynamo [_ error-type]
  (aws-error->hildebrand error-type error-type))

;; From eulalie.support
;; https://github.com/nervous-systems/eulalie/blob/master/src/eulalie/support.cljc
(defn issue-request! [{:keys [body target service chan close?] :as req
                       :or {close? true}} & [req-fn resp-fn]]
  (cond->
      ;; there was a go-catching wrapping this
      (let [{:keys [error body]}
            (async/<! (issue-request!
                       (-> req
                           (assoc :body (cond->> body req-fn (req-fn target)))
                           (dissoc :chan :close?))))]
        (if error
          (util/error->throwable service error)
          (cond->> body resp-fn (resp-fn target))))
    chan (async/pipe chan close?)))
