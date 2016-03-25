(ns magpie-tools.core
  (:gen-class)
  (:require [clj-zookeeper.zookeeper :as zk]
            [clojure.tools.logging :as log]
            [clojure.data.json :as json]
            [magpie-tools.utils :as utils]))

(def WARNNING-SCORE 40)

(defn prn-tasks-info
  []
  (let [tasks (utils/get-tasks-info)]
    (log/info "longest alive task:")
    (let [longest-alive-task (reduce (fn [u one]
                                       (if (< (compare (one :start-time) (u :start-time)) 0)
                                         one
                                         u))
                                     tasks)]
      (log/info longest-alive-task))
    (log/info "newest alive task:")
    (let [newest-alive-task (reduce (fn [u one]
                                      (if (> (compare (one :start-time) (u :start-time)) 0)
                                        one
                                        u))
                                    tasks)]
      (log/info newest-alive-task))
    (log/info "group_jar_class counts:")
    (log/info "tasks info:")
    (doseq [task tasks]
      (let [sorted-task (into (sorted-map) task)]
        (log/info sorted-task)))))

(defn prn-assignments-info
  []
  (let [
        children (vec (zk/get-children utils/MAGPIE-TASK-PATH))
        assignments-info (loop [assignments children result (sorted-map)]
                      (if (empty? assignments)
                        result
                        (let [info (json/read-str (String. (zk/get-data (str utils/MAGPIE-TASK-PATH "/" (last assignments))))
                                                  :key-fn keyword)]
                          (if (nil? (info :group))
                            (do (log/info (str "NO GROUP: " info))
                                (recur (pop assignments) result))
                            (if (nil? (info :jar))
                              (do (log/info (str "NO JAR: " info))
                                  (recur (pop assignments) result))
                              (if (nil? (info :class))
                                (do (log/info (str "NO CLASS: " info))
                                    (recur (pop assignments) result))
                                (let [the-key (keyword (str (info :group) "_" (info :jar) "_" (info :class)))]
                                  (recur (pop assignments)
                                         (assoc result the-key (inc (result the-key 0)))))))))))]
    (doseq [ass assignments-info]
      (log/info (str (key ass) "        " (val ass))))))

(defn prn-supervisors-health
  []
  (log/info "supervisors health:")
  (let [supervisors-health-info (utils/supervisors-health)
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
                       (log/info "group:" group)
                       (log/info "worst net-bandwidth score:" (if (>= worst-net-bandwidth-score WARNNING-SCORE)
                                                                worst-net-bandwidth-score
                                                                (str worst-net-bandwidth-score " WARNNING")))
                       (log/info worst-net-bandwidth-new)
                       (log/info "worst cpu score:" (if (>= worst-cpu-score WARNNING-SCORE)
                                                     worst-cpu-score
                                                     (str worst-cpu-score " WARNNING")))
                       (log/info worst-cpu-new)
                       (log/info "worst memory score:" (if (>= worst-memory-score WARNNING-SCORE)
                                                        worst-memory-score
                                                        (str worst-memory-score " WARNNING")))
                       (log/info worst-memory-new)))]
    (doseq [one-group supervisors-health-info]
      (format-one one-group))))

(defn balance-one-task
  [task-id]
  (log/info "start to balance" task-id)
  (let [task-info (utils/get-one-task task-id)
        id (:id task-info)
        jar (:jar task-info)
        klass (:class task-info)
        group (:group task-info)
        type (:type task-info)]
    (try
      (utils/kill-a-task id)
      (utils/submit-a-task id jar klass group type)
      (catch Throwable e
        (log/error e)
        (log/error "kill or submit error! please run kill and submit command again!")
        (log/info "kill command: magpie-client kill -id" id "-d")
        (log/info "submit command: magpie-client submit -class" klass "-id" id "-jar" jar "-group" group "-type" type "-d")
        (log/error "balance one task ERROR! please run commands above!")
        (System/exit 1))))
  (log/info "end balance" task-id))

(defn balance-one-supervisor
  [supervisor]
  (let [supervisor-id (:id supervisor)]
    (log/info "begin to balance" supervisor-id)
    (let [newest-tasks (reverse (sort-by :assign-time (utils/get-tasks-in-supervisor supervisor-id)))
          max-reschedule-size (int (/ (.size newest-tasks) 2))]
      (loop [tasks newest-tasks
             is-ok? (utils/the-supervisor-is-ok? supervisor-id)]
        (if is-ok?
          (log/info supervisor-id "is ok now!")
          (if (< (.size tasks) max-reschedule-size)
            (log/info "has rescheduled  max num tasks")
            (let [task (first tasks)]
              (balance-one-task (:task-id task))
              (recur (pop tasks)
                     (utils/the-supervisor-is-ok? supervisor-id))))))
      (log/info "end balance" supervisor-id))))

(defn balance-one-group
  [group]
  (let [supervisors (utils/get-all-supervisors group)
        bad-supervisors (apply list (filter #(or (< (:net-bandwidth-score %) WARNNING-SCORE)
                                                 (< (:cpu-score %) WARNNING-SCORE)
                                                 (< (:memory-score %) WARNNING-SCORE))
                                            supervisors))]
    (if (= (.size bad-supervisors) 0)
      (log/info "no bad supervisors in" group)
      (do (log/info "start to balance" group)
          (let [tasks-before (utils/get-all-tasks group)
                size-before (.size tasks-before)]
            (log/info tasks-before)
            (log/info "there are" size-before "tasks in group" group)
            (loop [supers bad-supervisors]
              (if (empty? supers)
                (log/info "finish balance" group)
                (do (balance-one-supervisor (first supers))
                    (recur (pop supers)))))
            (let [tasks-after (utils/get-all-tasks group)
                  size-after (.size tasks-after)]
              (if (= size-before size-after)
                (log/info "after balance, tasks num is the same as before")
                (do (log/error "after balance, tasks num is not the same as before, now only" size-after)
                    (log/info tasks-after)
                    (System/exit 1)))))))))

(defn -main
  [& args]
  (prn "Hi, magpie tools!")
  (let [zk-str (first args)                       ;"172.17.36.56:2181"
        group (second args)]
    (log/info "zk str:" zk-str)
    (log/info "group:" group)
    (zk/new-client zk-str)
    (utils/new-magpie-client)
    (balance-one-group group)
    (zk/close)))
