(defproject io.nervous/hildebrand "0.1.0-SNAPSHOT"
  :description "High-level, asynchronous AWS client library"
  :url "https://github.com/nervous-systems/hildebrand"
  :license {:name "Unlicense" :url "http://unlicense.org/UNLICENSE"}
  :scm {:name "git" :url "https://github.com/nervous-systems/hildebrand"}
  :deploy-repositories [["clojars" {:creds :gpg}]]
  :signing {:gpg-key "moe@nervous.io"}
  :global-vars {*warn-on-reflection* true}
  :source-paths ["src" "test"]
  :dependencies
  [[org.clojure/clojure    "1.6.0"]
   [org.clojure/core.async "0.1.346.0-17112a-alpha"]
   [io.nervous/eulalie     "0.1.1-SNAPSHOT"]
   [io.nervous/glossop     "0.1.0-SNAPSHOT"]
   [prismatic/plumbing     "0.4.1"]
   [slingshot              "0.12.2"]
   [com.velisco/herbert    "0.6.7"]]
  :exclusions [[org.clojure/clojure]])
