(defproject io.nervous/hildebrand "0.2.2"
  :description "High-level, asynchronous AWS client library"
  :url "https://github.com/nervous-systems/hildebrand"
  :license {:name "Unlicense" :url "http://unlicense.org/UNLICENSE"}
  :scm {:name "git" :url "https://github.com/nervous-systems/hildebrand"}
  :deploy-repositories [["clojars" {:creds :gpg}]]
  :signing {:gpg-key "moe@nervous.io"}
  :global-vars {*warn-on-reflection* true}
  :source-paths ["src" "test"]
  :plugins [[codox "0.8.11"]]
  :codox {:include [hildebrand]
          :defaults {:doc/format :markdown}}
  :dependencies
  [[org.clojure/clojure    "1.6.0"]
   [org.clojure/core.async "0.1.346.0-17112a-alpha"]
   [io.nervous/eulalie     "0.1.1"]
   [io.nervous/glossop     "0.1.0"]
   [prismatic/plumbing     "0.4.1"]]
  :exclusions [[org.clojure/clojure]])
