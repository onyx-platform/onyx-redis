(ns onyx.plugin.redis-writer-test
  (:require [aero.core :refer [read-config]]
            [clojure.core.async :refer [pipe]]
            [clojure.core.async.lab :refer [spool]]
            [clojure.test :refer [deftest is]]
            [onyx api
             [job :refer [add-task]]
             [test-helper :refer [with-test-env]]]
            [onyx.plugin
             [redis]
             [core-async :refer [get-core-async-channels]]]
            [onyx.tasks
             [core-async :as core-async]
             [redis :as redis]]
            [taoensso.carmine :as car :refer [wcar]]))

(defn sample-data [hll-counter]
  [{:op :sadd :args ["cyclists" {:name "John" :age 20}]}
   {:op :sadd :args ["runners" {:name "Mike" :age 24}]}
   {:op :sadd :args ["cyclists" {:name "Jane" :age 25}]}
   {:op :sadd :args ["runners" {:name "Mike" :age 24}]}
   {:op :pfadd :args [hll-counter 1]}
   {:op :pfadd :args [hll-counter 2]}
   :done])

(defn build-job [redis-uri batch-size batch-timeout]
  (let [batch-settings {:onyx/batch-size batch-size :onyx/batch-timeout batch-timeout}
        base-job (merge {:workflow [[:in :out]
                                    ]
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
        (add-task (core-async/input :in batch-settings))
        (add-task (redis/writer :out redis-uri batch-settings)))))

(deftest redis-writer-test
  (let [{:keys [env-config
                peer-config
                redis-config]} (read-config (clojure.java.io/resource "config.edn") {:profile :test})
        redis-uri (get redis-config :redis/uri)
        job (build-job redis-uri 10 1000)
        {:keys [in]}(get-core-async-channels job)
        redis-conn {:spec {:uri redis-uri}}
        hll-counter "hll_counter"
        messages (sample-data hll-counter)]
    (try
      (with-test-env [test-env [2 env-config peer-config]]
        (pipe (spool messages) in false)
        (onyx.test-helper/validate-enough-peers! test-env job)
        (->> (:job-id (onyx.api/submit-job peer-config job))
             (onyx.api/await-job-completion peer-config))
        (is (= (sort-by :name
                        (car/wcar redis-conn
                                  (set (car/smembers "cyclists"))))
               [{:name "Jane" :age 25}
                {:name "John" :age 20}]))
        (is (= (sort-by :name
                        (car/wcar redis-conn
                                  (set (car/smembers "runners"))))
               [{:name "Mike" :age 24}]))
        (is (= (car/wcar redis-conn (car/pfcount hll-counter))
               2)))
      (finally (wcar redis-conn
                     (car/flushall)
                     (car/flushdb))))))
