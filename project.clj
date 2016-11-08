(defproject io.nervous/hildebrand "0.4.5-SNAPSHOT"
  :description "High-level, asynchronous AWS client library"
  :url "https://github.com/nervous-systems/hildebrand"
  :license {:name "Unlicense" :url "http://unlicense.org/UNLICENSE"}
  :scm {:name "git" :url "https://github.com/nervous-systems/hildebrand"}
  :source-paths ["src"]
  :plugins [[lein-cljsbuild "1.1.4"]
            [lein-npm "0.6.2"]]
  :dependencies
  [[org.clojure/clojure        "1.8.0"]
   [org.clojure/clojurescript  "1.9.293"]
   [prismatic/schema           "1.1.3"]
   [prismatic/plumbing         "0.5.3" :exclusions [prismatic/schema]]
   [io.nervous/eulalie         "0.6.9" :exclusions [prismatic/plumbing
                                                    org.clojure/clojurescript]]]
  :npm {:dependencies [[bignumber.js "2.4.0"]]}
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
  :profiles {:dev {:source-paths ["src" "test"]}})
