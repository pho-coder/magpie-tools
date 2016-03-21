(ns magpie-tools.utils
  (:require [clj-zookeeper.zookeeper :as zk]))

(def ^:dynamic *magpie-client* (atom nil))

(defn new-magpie-client
  [magpie-nimbus-path]
  (let [nimbuses (zk/get-children magpie-nimbus-path)]
    (prn nimbuses)))
