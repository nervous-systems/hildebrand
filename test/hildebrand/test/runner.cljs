(ns hildebrand.test.runner
  (:require [cljs.test]
            [hildebrand.test.core]
            [hildebrand.test.channeled]
            [hildebrand.test.streams]
            [hildebrand.test.streams.channeled]
            [hildebrand.test.internal.request]
            [hildebrand.test.internal.response]
            [hildebrand.test.internal.platform.number]))

(defn run []
  (cljs.test/run-tests
   'hildebrand.test.core
   'hildebrand.test.channeled
   'hildebrand.test.streams
   'hildebrand.test.streams.channeled
   'hildebrand.test.internal.request
   'hildebrand.test.internal.response
   'hildebrand.test.internal.platform.number))

(enable-console-print!)

(set! *main-cli-fn* run)
