(defproject magpie-tools "0.4.5-SNAPSHOT"
  :description "magpie tools"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories [["jd-libs-snapshots" "http://artifactory.360buy-develop.com/libs-snapshots"]
                 ["clojars" "http://clojars.org/repo"]]
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/tools.cli "0.3.3"]
                 [org.apache.thrift/libthrift "0.9.1"]
                 [clj-zookeeper "0.2.0-SNAPSHOT"]
                 [org.clojure/data.json "0.2.6"]
                 [com.jd.bdp.magpie/magpie-utils "0.1.0.2015092214-SNAPSHOT"]
                 [com.jd.magpie/magpie-client "1.1.2-SNAPSHOT"]
                 [org.slf4j/slf4j-log4j12 "1.7.13"]
                 [org.clojure/tools.logging "0.3.1"]]
  :main ^:skip-aot magpie-tools.core
  :source-paths ["src" "src/clj"]
  :java-source-paths ["src/jvm"]
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}}
  :plugins [[lein-kibit "0.1.2"]
            [cider/cider-nrepl "0.11.0"]])
