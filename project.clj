(defproject io.nervous/hildebrand "1.0.0-SNAPSHOT"
  :description "High-level, asynchronous AWS client library"
  :url "https://github.com/nervous-systems/hildebrand"
  :license {:name "Unlicense" :url "http://unlicense.org/UNLICENSE"}
  :scm {:name "git" :url "https://github.com/nervous-systems/hildebrand"}
  :deploy-repositories [["clojars" {:creds :gpg}]]
  :signing {:gpg-key "moe@nervous.io"}
  :global-vars {*warn-on-reflection* true
                *print-meta* true}
  :source-paths ["src"]
  :plugins [[lein-cljsbuild "1.0.6"]
            [lein-npm "0.5.0"]]
  :dependencies
  [[org.clojure/clojure        "1.7.0"]
   [org.clojure/clojurescript  "0.0-3308"]
   [org.clojure/core.async     "0.1.346.0-17112a-alpha"]
   [io.nervous/eulalie         "1.0.0-SNAPSHOT"]
   [io.nervous/glossop         "1.0.0-SNAPSHOT"]
   [prismatic/plumbing         "0.4.1"]]
  :exclusions [[org.clojure/clojure]]
  :node-dependencies [[bignumber.js "2.0.7"]]
  :cljsbuild
  {:builds [{:id "main"
             :source-paths ["src"]
             :compiler {:output-to "hildebrand.js"
                        :target :nodejs
                        :hashbang false
                        :optimizations :none
                        :source-map true}}
            {:id "test-none"
             :source-paths ["src" "test"]
             :notify-command ["node" "target/test-none/hildebrand-test.js"]
             :compiler {:output-to "target/test-none/hildebrand-test.js"
                        :output-dir "target/test-none"
                        :target :nodejs
                        :optimizations :none
                        :main "hildebrand.test.runner"}}
            {:id "test-advanced"
             :source-paths ["src" "test"]
             :notify-command ["node" "target/test-advanced/hildebrand-test.js"]
             :compiler {:output-to "target/test-advanced/hildebrand-test.js"
                        :output-dir "target/test-advanced"
                        :target :nodejs
                        :optimizations :advanced}}]}
  :profiles {:dev
             {:repl-options
              {:nrepl-middleware
               [cemerick.piggieback/wrap-cljs-repl]}
              :dependencies
              [[com.cemerick/piggieback "0.2.1"]
               [org.clojure/tools.nrepl "0.2.10"]]
              :source-paths ["src" "test"]}})
