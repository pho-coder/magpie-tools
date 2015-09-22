(ns magpie-tasks.core
  (:gen-class)
  (:require [clj-zookeeper.zookeeper :as zk]
            [clojure.data.json :as json]
            [com.jd.bdp.magpie.utils :as utils]))

(defn get-all-tasks
  []
  (let [assignments-path "/magpie/assignments"
        children-names (zk/get-children assignments-path)]
    (map #(json/read-str (String. (zk/get-data (str assignments-path "/" %)))
                         :key-fn keyword)
         children-names)))

(defn get-all-supervisors
  []
  (let [supervisors-path "/magpie/supervisors"
        supervisors-names (zk/get-children supervisors-path)]
    (map #(json/read-str (String. (zk/get-data (str supervisors-path "/" %)))
                         :key-fn keyword)
         supervisors-names)))

(defn get-tasks-info
  []
  (let [supervisors (reduce (fn [m v]
                              (assoc m (v :id) v))
                            {}
                            (get-all-supervisors))]
    (map (fn [task]
           (assoc-in task [:start-time] (utils/timestamp2datetime (:start-time task))))
         (get-all-tasks))))

(defn get-assignments-info
  []
  (let [assignments-path "/magpie/assignments"
        children (vec (zk/get-children assignments-path))
        assignments-info (loop [assignments children result (sorted-map)]
                      (if (empty? assignments)
                        result
                        (let [info (json/read-str (String. (zk/get-data (str assignments-path "/" (last assignments))))
                                                  :key-fn keyword)]
                          (if (nil? (info :group))
                            (do (prn (str "NO GROUP: " info))
                                (recur (pop assignments) result))
                            (if (nil? (info :jar))
                              (do (prn (str "NO JAR: " info))
                                  (recur (pop assignments) result))
                              (if (nil? (info :class))
                                (do (prn (str "NO CLASS: " info))
                                    (recur (pop assignments) result))
                                (let [the-key (keyword (str (info :group) "_" (info :jar) "_" (info :class)))]
                                  (recur (pop assignments)
                                         (assoc result the-key (inc (result the-key 0)))))))))))]
    (doseq [ass assignments-info]
      (prn (str (key ass) "        " (val ass))))))

(defn -main
  [& args]
  (println "Hi, magpie tasks!")
  (let [zk-str (nth args 0)]
    (prn zk-str)
    (zk/new-client zk-str)
    (prn (get-all-tasks))
    (prn (get-tasks-info))
    (prn (get-all-supervisors))
    (get-tasks-info)
    (get-assignments-info)
    (zk/close)
    ))
