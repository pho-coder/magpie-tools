(ns magpie-tools.utils
  (:require [clj-zookeeper.zookeeper :as zk]
            [clojure.tools.logging :as log]
            [clojure.data.json :as json])
  (:import [com.jd.magpie.client MagpieClient]))

(def ^:dynamic *magpie-client* (atom nil))

(defn new-magpie-client
  [magpie-nimbus-path]
  (let [nimbuses (zk/get-children magpie-nimbus-path)]
    (if (<= (.size nimbuses) 0)
      (do (log/error "there isn't any nimbus running now!")
          (System/exit 1))
      (let [nodes (to-array nimbuses)]
        (java.util.Arrays/sort nodes)
        (let [active-nimbus (first nodes)]
          (log/info "active nimbus:" active-nimbus)
          (let [active-nimbus-path (str magpie-nimbus-path "/" active-nimbus)
                nimbus-info (json/read-str (String. (zk/get-data active-nimbus-path))
                                           :key-fn keyword)
                nimbus-ip (:ip nimbus-info)
                nimbus-port (:port nimbus-info)
                magpie-client (MagpieClient. (hash-map) nimbus-ip nimbus-port)
                client (.getClient magpie-client)]
            (log/info client)))))))
