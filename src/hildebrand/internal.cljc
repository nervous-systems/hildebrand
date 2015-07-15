(ns hildebrand.internal
  (:require [eulalie.support]
            [hildebrand.internal.request]
            [hildebrand.internal.response]
            [plumbing.core :refer [update]]
            [plumbing.map :refer [keyword-map]]
            [hildebrand.error :refer [error->throwable]]
            [glossop :refer [<?! go-catching <?]]
            [eulalie]
            [eulalie.dynamo]))

(def aws-error->hildebrand
  {:resource-not-found-exception :resource-not-found
   :conditional-check-failed-exception :conditional-failed})

(defn rename-error [{{:keys [type] :as error} :error :as r}]
  (cond-> (dissoc r :error)
    error (assoc :hildebrand/error
                 (update error :type aws-error->hildebrand type))))

(defn issue-request! [service target creds body req-fn resp-fn]
  (go-catching
    (let [body (req-fn target body)
          {:keys [hildebrand/error body] :as resp}
          (-> (keyword-map service target creds body)
              eulalie/issue-request!
              <?
              rename-error)]
      (if error
        (error->throwable error)
        (resp-fn target body)))))

#? (:clj
    (defmacro defissuer [target-name args & [doc]]
      `(eulalie.support/defissuer :dynamo
         ~target-name ~args
         hildebrand.internal.request/restructure-request
         hildebrand.internal.response/restructure-response
         ~doc)))
