(defproject magpie-tasks "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [clj-zookeeper "0.2.0-SNAPSHOT"]
                 [org.clojure/data.json "0.2.6"]
                 [com.jd.bdp.magpie/magpie-utils "0.1.0.2015092214-SNAPSHOT"]]
  :main ^:skip-aot magpie-tasks.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
