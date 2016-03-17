(ns magpie-tools.core
  (:gen-class)
  (:require [clj-zookeeper.zookeeper :as zk]
            [clojure.data.json :as json]
            [com.jd.bdp.magpie.utils :as utils]))

(def SUPERVISORS-PATH "/magpie/supervisors")
(def YOURTASKS-PATH "/magpie/yourtasks")
(def WARNNING-SCORE 50)

(defn get-all-tasks
  []
  (let [assignments-path "/magpie/assignments"
        children-names (zk/get-children assignments-path)]
    (map #(json/read-str (String. (zk/get-data (str assignments-path "/" %)))
                         :key-fn keyword)
         children-names)))

(defn get-tasks-in-supervisor
  [supervisor]
  (let [tasks-path (str YOURTASKS-PATH "/" supervisor)
        tasks (zk/get-children tasks-path)]
    (map #(assoc (json/read-str (String. (zk/get-data (str tasks-path "/" %)))
                         :key-fn keyword) :task-id %)
         tasks)))

(defn get-all-supervisors
  []
  (let [supervisors-path SUPERVISORS-PATH
        supervisors-names (zk/get-children supervisors-path)]
    (map #(json/read-str (String. (zk/get-data (str supervisors-path "/" %)))
                         :key-fn keyword)
         supervisors-names)))

(defn supervisors-health
  []
  (let [all-supervisors (get-all-supervisors)
        all-supervisors-groupped (reduce (fn [m one]
                                           (update-in m [(:group one)] conj one)) 
                                         {}
                                         all-supervisors)
        worst-supervisors (fn [one-group]
                            (reduce (fn [m one]
                                      (let [one-net-bandwidth-score (:net-bandwidth-score one)
                                            one-cpu-score (:cpu-score one)
                                            one-memory-score (:memory-score one)
                                            worst-net-bandwidth-score (:worst-net-bandwidth-score m)
                                            worst-cpu-score (:worst-cpu-score m)
                                            worst-memory-score (:worst-memory-score m)
                                            tmp (atom m)]
                                        (when (<= one-net-bandwidth-score worst-net-bandwidth-score)
                                          (reset! tmp (assoc-in @tmp [:worst-net-bandwidth-score] one-net-bandwidth-score))
                                          (reset! tmp (assoc-in @tmp [:worst-net-bandwidth-one] one)))
                                        (when (<= one-cpu-score worst-cpu-score)
                                          (reset! tmp (assoc-in @tmp [:worst-cpu-score] one-cpu-score))
                                          (reset! tmp (assoc-in @tmp [:worst-cpu-one] one)))
                                        (when (<= one-memory-score worst-memory-score)
                                          (reset! tmp (assoc-in @tmp [:worst-memory-score] one-memory-score))
                                          (reset! tmp (assoc-in @tmp [:worst-memory-one] one)))
                                        @tmp))
                                    {:worst-net-bandwidth-score 100
                                     :worst-net-bandwidth-one nil
                                     :worst-cpu-score 100
                                     :worst-cpu-one nil
                                     :worst-memory-score 100
                                     :worst-memory-one nil}
                                    one-group))]
    (map #(let [group (first %)
                one-group (second %)]
            {group (worst-supervisors one-group)}) all-supervisors-groupped)))

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

(defn prn-supervisors-health
  []
  (println "supervisors health:")
  (let [supervisors-health-info (supervisors-health)
        format-one (fn [one]
                     (let [group (first (keys one))
                           values (first (vals one))
                           worst-net-bandwidth-score (:worst-net-bandwidth-score values)
                           worst-cpu-score (:worst-cpu-score values)
                           worst-memory-score (:worst-memory-score values)
                           wnb-one (:worst-net-bandwidth-one values)
                           wc-one (:worst-cpu-one values)
                           wm-one (:worst-memory-one values)
                           worst-net-bandwidth-new {:id (:id wnb-one)
                                                    :ip (:ip wnb-one)
                                                    :hostname (:hostname wnb-one)
                                                    :max-net-bandwidth (:max-net-bandwidth wnb-one)
                                                    :tx-net-bandwidth (:tx-net-bandwidth wnb-one)
                                                    :rx-net-bandwidth (:rx-net-bandwidth wnb-one)
                                                    :net-bandwidth-score (:net-bandwidth-score wnb-one)}
                           worst-cpu-new {:id (:id wc-one)
                                          :ip (:ip wc-one)
                                          :hostname (:hostname wc-one)
                                          :cpu-core (:cpu-core wc-one)
                                          :load-avg (:load-avg wc-one)
                                          :cpu-score (:cpu-score wc-one)}
                           worst-memory-new {:id (:id wm-one)
                                             :ip (:ip wm-one)
                                             :hostname (:hostname wm-one)
                                             :total-memory (:total-memory wm-one)
                                             :memory-score (:memory-score wm-one)}]
                       (print "\n")
                       (println "group:" group)
                       (println "worst net-bandwidth score:" (if (>= worst-net-bandwidth-score WARNNING-SCORE)
                                                                worst-net-bandwidth-score
                                                                (str worst-net-bandwidth-score " WARNNING")))
                       (println worst-net-bandwidth-new)
                       (println "worst cpu score:" (if (>= worst-cpu-score WARNNING-SCORE)
                                                     worst-cpu-score
                                                     (str worst-cpu-score " WARNNING")))
                       (println worst-cpu-new)
                       (println "worst memory score:" (if (>= worst-memory-score WARNNING-SCORE)
                                                        worst-memory-score
                                                        (str worst-memory-score " WARNNING")))
                       (println worst-memory-new)))]
    (doseq [one-group supervisors-health-info]
      (format-one one-group))))

(defn -main
  [& args]
  (prn "Hi, magpie tools!")
  (let [zk-str "172.17.36.56:2181"]
    (prn zk-str)
    (zk/new-client zk-str)
;;    (prn (get-all-supervisors))
    ;;(prn (supervisors-health))
    (prn-supervisors-health)
    (prn (get-tasks-in-supervisor "BJYZ-magpie-Client-3658.hadoop.jd.local-8d3cac52-9ea6-4fb5-9b8c-ef660683ae5d"))
    (zk/close)))
