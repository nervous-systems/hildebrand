(ns hildebrand.error)

(defmulti  error->throwable :type)
(defmethod error->throwable :default [{:keys [type message] :as error}]
  (ex-info (name type) error))

