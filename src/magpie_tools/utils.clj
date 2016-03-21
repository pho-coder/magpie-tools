(ns magpie-tools.utils
  (:require [clj-zookeeper.zookeeper :as zk]
            [clojure.tools.logging :as log]))

(def ^:dynamic *magpie-client* (atom nil))

(defn new-magpie-client
  [magpie-nimbus-path]
  (let [nimbuses (zk/get-children magpie-nimbus-path)]
    (log/info nimbuses)))
