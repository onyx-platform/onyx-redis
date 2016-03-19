(ns onyx.plugin.redis-writer-test
  (:require [aero.core :refer [read-config]]
            [clojure.core.async :refer [pipe]]
            [clojure.core.async.lab :refer [spool]]
            [clojure.test :refer [deftest is use-fixtures testing]]
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

(def config (atom {}))

(defn redis-conn []
  {:spec {:uri (get-in @config [:redis-config :redis/uri])}})

(defn load-config [test-fn]
  (reset! config (read-config (clojure.java.io/resource "config.edn") {:profile :test}))
  (test-fn))

(defn flush-redis [test-fn]
  (wcar (redis-conn)
        (car/flushall)
        (car/flushdb))
  (test-fn))

(use-fixtures :once load-config)
(use-fixtures :each flush-redis)


(def sample-data
  [{:op :sadd :args ["cyclists" {:name "John" :age 20}]}
   {:op :sadd :args ["runners" {:name "Mike" :age 24}]}
   {:op :sadd :args ["cyclists" {:name "Jane" :age 25}]}
   {:op :sadd :args ["runners" {:name "Mike" :age 24}]}
   {:op :publish :args ["race-stats" {:leader "Mike"}]}
   {:op :publish :args ["race-stats" {:leader "Jane"}]}
   {:op :publish :args ["race-stats" {:leader "John"}]}
   {:op :pfadd :args ["hll_counter" 1]}
   {:op :pfadd :args ["hll_counter" 2]}
   :done])


(def race-stats (atom []))

(defn subscribe-race-stats []
  (car/with-new-pubsub-listener (redis-conn)
    {"race-stats" #(swap! race-stats conj %)}
    (car/subscribe "race-stats")))

(defn count-hll-counter [redis-spec]
  (car/wcar redis-spec (car/pfcount "hll_counter")))

(defn build-job [redis-spec batch-size batch-timeout]
  (let [redis-uri (get-in redis-spec [:spec :uri])
        batch-settings {:onyx/batch-size batch-size :onyx/batch-timeout batch-timeout}
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
                peer-config]} @config
        redis-spec (redis-conn)
        job (build-job redis-spec 10 1000)
        {:keys [in]} (get-core-async-channels job)
        _ (subscribe-race-stats)]
    (with-test-env [test-env [2 env-config peer-config]]
      (pipe (spool sample-data) in false)
      (onyx.test-helper/validate-enough-peers! test-env job)
      (->> (:job-id (onyx.api/submit-job peer-config job))
           (onyx.api/await-job-completion peer-config))
      (testing "redis :sadd and :smembers are correctly distributed"
        (let [members (fn [key] (sort-by :name
                                         (car/wcar redis-spec
                                                   (set (car/smembers key)))) )]
          (is (= (members "cyclists")
                 [{:name "Jane" :age 25}
                  {:name "John" :age 20}]))
          (is (= (members "runners")
                 [{:name "Mike" :age 24}]))))
      (testing "redis :pfcount is the last value given"
        (is (= (count-hll-counter redis-spec)
               2)))
      (testing "redis :publish can be consumed by subscriber"
        (let [stats (take 4 @race-stats)]
          (is (= stats
                 [["subscribe" "race-stats" 1]
                  ["message" "race-stats" {:leader "Mike"}]
                  ["message" "race-stats" {:leader "Jane"}]
                  ["message" "race-stats" {:leader "John"}]])))))))
