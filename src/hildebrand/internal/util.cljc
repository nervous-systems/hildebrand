(ns hildebrand.internal.util)

(defn throw-empty [x]
  (assert (not (empty? x)))
  x)

(def type-aliases-out
  {:string     :S
   :number     :N
   :list       :L
   :binary     :B
   :number-set :NS
   :string-set :SS
   :binary-set :BS
   :map        :M
   :null       :NULL
   :boolean    :BOOL})

(def type-aliases-in
  (into {}
    (for [[k v] type-aliases-out]
      [v k])))

(defn defmulti-dispatch [method v->handler]
  (doseq [[v handler] v->handler]
    (defmethod method v [& args] (apply handler args))))

;; Port from plumbing.core
;; https://github.com/plumatic/plumbing/blob/master/src/plumbing/core.cljx
(defn map-vals
  "Build map k -> (f v) for [k v] in map, preserving the initial type"
  [f m]
  (cond
    (sorted? m)
    (reduce-kv (fn [out-m k v] (assoc out-m k (f v))) (sorted-map) m)
    (map? m)
    (persistent! (reduce-kv (fn [out-m k v] (assoc! out-m k (f v))) (transient {}) m))
    :else
    (reduce-kv (fn [acc k v] (merge {k (f v)})) {} m)))


(defn dissoc-in
  "Dissociate this keyseq from m, removing any empty maps created as a result
   (including at the top-level)."
  [m [k & ks]]
  (when m
    (if-let [res (and ks (dissoc-in (get m k) ks))]
      (assoc m k res)
      (let [res (dissoc m k)]
        (when-not (empty? res)
          res)))))

(defn error? [e]
  (instance? #? (:clj Exception :cljs js/Error) e))

(defn throw-err [e]
  (when (error? e)
    (throw e))
  e)

;; https://github.com/plumatic/plumbing/blob/master/src/plumbing/map.cljx
(defmacro keyword-map
  "Expands to a map whose keys are keywords with the same name as the given
  symbols, e.g.:
    (let [x 41, y (inc x)]
      (keyword-map x y))
    ;; => {:x 41, :y 42}"
  [& syms]
  (when-not (every? symbol? syms)
    (throw (ex-info "Arguments to keyword-map must be symbols!" {:args syms})))
  (zipmap (map #(keyword (name %)) syms) syms))

#?(:clj
   (defn <?! [ch]
     (throw-err (async/<!! ch))))

(defmulti translate-error-type
  (fn [service error-type]
    (keyword "eulalie.service" (name service))))
(defmethod translate-error-type :default [_ error-type] error-type)

(defn error->throwable [service {:keys [type message] :as error}]
  (let [type (translate-error-type service type)]
    (ex-info (str (name type) ": " message) (assoc error :type type))))
