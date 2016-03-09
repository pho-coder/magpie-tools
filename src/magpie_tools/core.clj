(ns magpie-tools.core
  (:gen-class)
  (:require [clj-zookeeper.zookeeper :as zk]
            [clojure.data.json :as json]
            [com.jd.bdp.magpie.utils :as utils]))

(def SUPERVISORS-PATH "/magpie/supervisors")

(defn get-all-tasks
  []
  (let [assignments-path "/magpie/assignments"
        children-names (zk/get-children assignments-path)]
    (map #(json/read-str (String. (zk/get-data (str assignments-path "/" %)))
                         :key-fn keyword)
         children-names)))

(defn get-all-supervisors
  []
  (let [supervisors-path SUPERVISORS-PATH
        supervisors-names (zk/get-children supervisors-path)]
    (map #(json/read-str (String. (zk/get-data (str supervisors-path "/" %)))
                         :key-fn keyword)
         supervisors-names)))

(defn supervisors-health
  []
)

(defn get-tasks-info
  []
  (let [supervisors (reduce (fn [m v]
                              (assoc m (v :id) v))
                            {}
                            (get-all-supervisors))]
    (map (fn [task]
           (let [replace-start-time (if (nil? (task :start-time))
                                      task
                                      (assoc-in task [:start-time] (utils/timestamp2datetime (:start-time task))))
                 replace-supervisor (if (nil? (replace-start-time :supervisor))
                                      replace-start-time
                                      (assoc-in replace-start-time [:supervisor] ((supervisors (replace-start-time :supervisor)) :ip)))
                 replace-last-supervisor (if (nil? (replace-supervisor :last-supervisor))
                                           replace-supervisor
                                           (assoc-in replace-supervisor [:last-supervisor] ((supervisors (replace-supervisor :last-supervisor)) :ip)))]
             replace-last-supervisor))
         (get-all-tasks))))

(defn prn-tasks-info
  []
  (let [tasks (get-tasks-info)]
    (println)
    (println)
    (println "longest alive task:")
    (let [longest-alive-task (reduce (fn [u one]
                                       (if (< (compare (one :start-time) (u :start-time)) 0)
                                         one
                                         u))
                                     tasks)]
      (println longest-alive-task))
    (println)
    (println)
    (println "newest alive task:")
    (let [newest-alive-task (reduce (fn [u one]
                                      (if (> (compare (one :start-time) (u :start-time)) 0)
                                        one
                                        u))
                                    tasks)]
      (println newest-alive-task))
    (println)
    (println)
    (println "group_jar_class counts:")
    (println "tasks info:")
    (doseq [task tasks]
      (let [sorted-task (into (sorted-map) task)]
        (println sorted-task)))))

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
  (println "Hi, magpie tools!")
  (let [zk-str (nth args 0)]
    (prn zk-str)
    (zk/new-client zk-str)
    (prn-tasks-info)
    (get-assignments-info)
    (zk/close)))
