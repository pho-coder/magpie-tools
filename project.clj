(defproject magpie-tools "0.2.0-SNAPSHOT"
  :description "magpie tools"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [clj-zookeeper "0.2.0-SNAPSHOT"]
                 [org.clojure/data.json "0.2.6"]
                 [com.jd.bdp.magpie/magpie-utils "0.1.0.2015092214-SNAPSHOT"]]
  :main ^:skip-aot magpie-tools.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}}
  :plugins [[lein-kibit "0.1.2"]
            [cider/cider-nrepl "0.11.0"]])
