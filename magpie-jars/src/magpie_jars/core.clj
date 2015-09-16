(ns magpie-jars.core
  (:gen-class)
  (:require [clj-zookeeper.zookeeper :as zk]
            [clojure.data.json :as json]))

(defn get-assignments-info
  []
  (let [assignments-path "/magpie/assignments"
        children (zk/get-children assignments-path)]
    (doseq [child children]
      (prn (json (String. (zk/get-data (str assignments-path "/" child)))))))

(defn -main
  [& args]
  (println "Hi, magpie tools!")
  (let [zk-str "192.168.144.107:2181,192.168.144.108:2181,192.168.144.109:2181"]
    (zk/new-client zk-str)
    (get-assignments-info)
    (zk/close)))
