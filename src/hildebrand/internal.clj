(ns hildebrand.internal
  (:require [plumbing.core :refer [update]]
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

(defmacro defissuer [service target-name args req-fn resp-fn & [doc]]
  (let [fname!  (-> target-name name (str "!") symbol)
        fname!! (-> target-name name (str "!!") symbol)
        args'   (into '[creds] (conj args '& '[extra]))
        body  `(issue-request!
                ~service ~(keyword target-name) ~'creds
                (merge (plumbing.map/keyword-map ~@args) ~'extra)
                ~req-fn ~resp-fn)
        md     (cond-> (meta target-name)
                 doc (assoc :doc doc))]
    `(do
       (defn ~(with-meta fname!  md) ~args' ~body)
       (defn ~(with-meta fname!! md) ~args' (<?! ~body)))))
