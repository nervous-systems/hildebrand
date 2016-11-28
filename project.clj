(defproject io.nervous/hildebrand "0.4.5"
  :description "High-level, asynchronous AWS client library"
  :url "https://github.com/nervous-systems/hildebrand"
  :license {:name "Unlicense" :url "http://unlicense.org/UNLICENSE"}
  :scm {:name "git" :url "https://github.com/nervous-systems/hildebrand"}
  :source-paths ["src"]
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [io.nervous/eulalie  "0.6.10"]]
  :npm {:dependencies [[bignumber.js "2.4.0"]]}
  :plugins [[lein-cljsbuild "1.1.4"]
            [lein-npm       "0.6.2"]]
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
