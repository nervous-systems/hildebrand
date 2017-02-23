(ns hildebrand.internal.macros
  (:require [hildebrand.internal.response]
            [hildebrand.internal.request])
  (:require-macros [cljs.core.async.macros]))

(defmacro defissuer [service target-name issue-args req-fn resp-fn & [doc]]
  (let [fname!  (-> target-name name (str "!") symbol)
        fname!! (-> target-name name (str "!!") symbol)
        args'   (into '[creds] (conj issue-args '& 'args))
        body    `(let [[op-args# req-args#] ~'args]
                   (hildebrand.internal/issue-request!
                    (merge
                     {:service ~service
                      :target  ~(keyword target-name)
                      :creds   ~'creds
                      :body    (merge
                                (hildebrand.internal.util/keyword-map ~@issue-args)
                                op-args#)}
                     req-args#)
                    ~req-fn ~resp-fn))
        md      (cond-> (meta target-name)
                  doc (assoc :doc doc))]
    `(do
       (defn ~(with-meta fname! md) ~args' ~body)
       ~(when-not (:ns &env)
          `(defn ~(with-meta fname!! md) ~args' (g/<?! ~body))))))

(defmacro defissuer-dynamo [target-name args & [doc]]
  `(defissuer :dynamo
     ~target-name ~args
     hildebrand.internal.request/restructure-request
     hildebrand.internal.response/restructure-response*
     ~doc))

;; From glossop.core
;; https://github.com/nervous-systems/glossop/blob/master/src/glossop/core.cljc

(defmacro <? [ch]
  `(hildebrand.internal.util/throw-err (cljs.core.async/<! ~ch)))

(defmacro go-catching [& body]
  `(cljs.core.async.macros/go
     (try
       ~@body
       (catch js/Error e#
         e#))))

(defmacro memoized-fn
  "Like fn, but memoized (including recursive calls).
   The clojure.core memoize correctly caches recursive calls when you do a top-level def
   of your memoized function, but if you want an anonymous fibonacci function, you must use
   memoized-fn rather than memoize to cache the recursive calls."
  [name args & body]
  `(let [a# (atom {})]
     (fn ~name ~args
       (let [m# @a#
             args# ~args]
         (if-let [[_# v#] (find m# args#)]
           v#
           (let [v# (do ~@body)]
             (swap! a# assoc args# v#)
             v#))))))
