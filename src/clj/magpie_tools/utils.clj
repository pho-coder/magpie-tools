(ns magpie-tools.utils
  (:require [clj-zookeeper.zookeeper :as zk]
            [clojure.tools.logging :as log]
            [clojure.data.json :as json]
            [com.jd.bdp.magpie.utils :as mutils])
  (:import [com.jd.magpie.client MagpieClient]))

(def ^:dynamic *magpie-client* (atom nil))
(def MAGPIE-PATH "/magpie")
(def MAGPIE-TASK-PATH (str MAGPIE-PATH "/assignments"))
(def SUPERVISORS-PATH (str MAGPIE-PATH "/supervisors"))
(def YOURTASKS-PATH (str MAGPIE-PATH "/yourtasks"))

;; magpie client funs
(defn new-magpie-client
  []
  (let [magpie-nimbus-path (str MAGPIE-PATH "/nimbus")
        nimbuses (zk/get-children magpie-nimbus-path)]
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
            (reset! *magpie-client* client)))))))

(defn submit-a-task
  [id jar klass group type]
  (log/info "start to submit task:" id)
  (let [task-path (str MAGPIE-TASK-PATH "/" id)
        client @*magpie-client*]
    (when (zk/check-exists? task-path)
      (log/error id "exists! CAN'T submit!")
      (System/exit 1))
    (.submitTask client id jar klass group type)
    (Thread/sleep 300)
    (log/info "submit" id jar klass group type)
    (while (not (zk/check-exists? task-path))
      (log/warn "submitting" id)
      (Thread/sleep 1000))
    (log/info "end to submit task:" id)))

(defn kill-a-task
  [id]
  (log/info "start to kill task:" id)
  (let [task-path (str MAGPIE-TASK-PATH "/" id)
        client @*magpie-client*]
    (when (not (zk/check-exists? task-path))
      (log/error id "NOT exists! CAN'T kill!")
      (System/exit 1))
    (.operateTask client id "kill")
    (Thread/sleep 300)
    (log/info "kill" id)
    (while (zk/check-exists? task-path)
      (log/warn "killing" id)
      (Thread/sleep 1000))
    (log/info "end to kill task:" id)))

;; zk info funs
(defn get-all-tasks
  ([]
   (let [children-names (zk/get-children MAGPIE-TASK-PATH)]
     (map #(json/read-str (String. (zk/get-data (str MAGPIE-TASK-PATH "/" %)))
                          :key-fn keyword)
          children-names)))
  ([group]
   (filter #(= (:group %) group)
           (get-all-tasks))))

(defn get-tasks-in-supervisor
  [supervisor]
  (let [tasks-path (str YOURTASKS-PATH "/" supervisor)
        tasks (zk/get-children tasks-path)]
    (map #(assoc (json/read-str (String. (zk/get-data (str tasks-path "/" %)))
                                :key-fn keyword) :task-id %)
         tasks)))

(defn get-the-supervisor
  [supervisor-id]
  (json/read-str (String. (zk/get-data (str SUPERVISORS-PATH "/" supervisor-id)))
                 :key-fn keyword))

(defn the-supervisor-is-ok?
  [supervisor-id]
  (let [ok-score 30
        supervisor (get-the-supervisor supervisor-id)]
    (log/info supervisor)
    (and (>= (:net-bandwidth-score supervisor) ok-score)
         (>= (:cpu-score supervisor) ok-score)
         (>= (:memory-score supervisor) ok-score))))

(defn get-all-supervisors
  ([]
   (let [supervisors-names (zk/get-children SUPERVISORS-PATH)]
     (map #(json/read-str (String. (zk/get-data (str SUPERVISORS-PATH "/" %)))
                          :key-fn keyword)
          supervisors-names)))
  ([group]
   (filter #(= group (:group %))
           (get-all-supervisors))))

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
                                      (assoc-in task [:start-time] (mutils/timestamp2datetime (:start-time task))))
                 replace-supervisor (if (nil? (replace-start-time :supervisor))
                                      replace-start-time
                                      (assoc-in replace-start-time [:supervisor] ((supervisors (replace-start-time :supervisor)) :ip)))
                 replace-last-supervisor (if (nil? (replace-supervisor :last-supervisor))
                                           replace-supervisor
                                           (assoc-in replace-supervisor [:last-supervisor] ((supervisors (replace-supervisor :last-supervisor)) :ip)))]
             replace-last-supervisor))
         (get-all-tasks))))

(defn get-one-task
  [task-id]
  (let [task-path (str MAGPIE-TASK-PATH "/" task-id)]
    (json/read-str (String. (zk/get-data task-path))
                   :key-fn keyword)))
