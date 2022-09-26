(defproject tigerbeetle "0.0.1-SNAPSHOT"
  :description "A Jepsen Test for TigerBeetle."
  :url "https://github.com/jepsen-tigerbeetle"
  :license {:name "Licensed under the Apache License, Version 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0"}
  :dependencies [[com.tigerbeetle/tigerbeetle-java "0.0.1-SNAPSHOT"]
                 [org.clojure/clojure "1.11.1"]
                 [jepsen "0.2.8-SNAPSHOT"]]
  :main tigerbeetle.core
  :repl-options {:init-ns tigerbeetle.core}
  :plugins [[lein-codox "0.10.8"]
            [lein-localrepo "0.5.4"]]
  :codox {:output-path "target/doc/"
          :source-uri "https://github.com/nurturenature/tigerbeetle/tree/main/{filepath}#L{line}"
          :metadata {:doc/format :markdown}})
