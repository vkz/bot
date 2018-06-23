(defproject bot "0.0.1"
  :description ""
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [aleph "0.4.4"]
                 [byte-streams "0.2.4"]
                 [clj-http "3.9.0"]
                 [org.clojure/core.async "0.4.474"
                  :exclusions [[org.clojure/clojure]
                               [org.clojure/clojurescript]]]
                 [cheshire "5.8.0"]
                 [medley "1.0.0"
                  :exclusions [[org.clojure/clojure]]]
                 [com.taoensso/timbre "4.10.0"
                  :exclusions [[org.clojure/clojure]]]
                 [org.clojure/spec.alpha "0.1.143"
                  :exclusions [[org.clojure/clojure]]]
                 [org.clojure/test.check "0.9.0"
                  :exclusions [[org.clojure/clojure]
                               [org.clojure/clojurescript]]]]
  :paths ["src" "src/exch"]
  :jvm-opts ["--add-modules" "java.xml.bind"])
