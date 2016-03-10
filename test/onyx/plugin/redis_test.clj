(ns onyx.plugin.redis-test
  (:require [aero.core :refer [read-config]]
            [taoensso.carmine :as car :refer [wcar]]
            [clojure.test :refer [deftest is]]
            [onyx api
             [job :refer [add-task]]
             [test-helper :refer [with-test-env]]]
            [onyx.redis.tasks :as redis]
            [onyx.plugin
             [core-async :refer [take-segments!]]
             [core-async-tasks :as core-async]
             [redis]]))

(defn build-job [redis-uri batch-size batch-timeout]
  (let [batch-settings {:onyx/batch-size batch-size :onyx/batch-timeout batch-timeout}
        base-job (merge {:workflow [[:in :inc]
                                    [:inc :out]]
                         :catalog [{:onyx/name :inc
                                    :onyx/fn ::my-inc
                                    :onyx/type :function
                                    :onyx/batch-size batch-size}]
                         :lifecycles []
                         :windows []
                         :triggers []
                         :flow-conditions []
                         :task-scheduler :onyx.task-scheduler/balanced})]
    (-> base-job
        (add-task (redis/reader :in redis-uri ::store :lpop batch-settings))
        (add-task (redis/writer :out redis-uri (merge {:onyx/fn ::create-writes}
                                                      batch-settings))))))

(defn my-inc [segment]
  (update (:value segment) :n inc))

(defn create-writes [segment]
  {:args [::store-out segment]
   :op :lpush})

(defn ensure-redis! [redis-conn]
  (wcar redis-conn
        (car/flushall)
        (car/flushdb)
        (mapv (fn [n]
                (car/lpush ::store {:n n}))
              (range 100))
        (car/lpush ::store :done)))

(deftest redis-plugin-general-test
  (let [{:keys [env-config
                peer-config
                redis-config]} (read-config (clojure.java.io/resource "config.edn") {:profile :test})
        redis-uri (get redis-config :redis/uri)
        job (build-job redis-uri 10 1000)
        redis-conn {:spec {:uri redis-uri}}]
    (try
      (with-test-env [test-env [3 env-config peer-config]]
        (ensure-redis! redis-conn)
        (onyx.test-helper/validate-enough-peers! test-env job)
        (->> (:job-id (onyx.api/submit-job peer-config job))
             (onyx.api/await-job-completion peer-config))
        (let [vs (sort-by :n
                          (wcar redis-conn
                                (car/lrange ::store-out 0 100000)))]
          (is (= vs
                 (map (fn [v]
                        {:n (inc v)})
                      (range 100))))))
      (finally (wcar redis-conn
                     (car/flushall)
                     (car/flushdb))))))
