(ns onyx.plugin.redis-test
  (:require [onyx.peer.pipeline-extensions :as p-ext]
            [onyx.plugin.redis :refer :all]
            [clojure.core.async :refer [chan go-loop >!! <!! <!]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [midje.sweet :refer :all]
            [taoensso.timbre :refer [info]]
            [taoensso.carmine :as car :refer [wcar]]
            [clojure.core.async :as async :refer [chan <!! >!!]]
            [onyx.api]))

(def id (java.util.UUID/randomUUID))

(def env-config
  {:zookeeper/address "127.0.0.1:2188"
   :zookeeper/server? true
   :zookeeper.server/port 2188
   :onyx/id id})

(def peer-config
  {:zookeeper/address "127.0.0.1:2188"
   :onyx.peer/job-scheduler :onyx.job-scheduler/greedy
   :onyx.messaging/impl :aeron
   :onyx.messaging/peer-port-range [40200 40260]
   :onyx.messaging/bind-addr "localhost"
   :onyx/id id})

(def env (onyx.api/start-env env-config))

(def peer-group (onyx.api/start-peer-group peer-config))

(def n-messages (rand-int 1000))

(def batch-size 8)

(def redis-conn {:spec {:host "127.0.0.1"}})

(fact (wcar redis-conn
            (car/flushall)
            (car/flushdb)) => ["OK" "OK"])

(wcar redis-conn
      (mapv (fn [n]
             (car/lpush ::store {:n n}))
           (range n-messages))
      (car/lpush ::store :done))

;;;;;
;;;;;
;;;;;
(defn my-inc [segment]
  (update (:value segment) :n inc))

(defn create-writes [segment]
  {:key ::store-out
   :value segment
   :op :lpush})

(def catalog
  [{:onyx/name :in
    :onyx/plugin :onyx.plugin.redis/consumer
    :onyx/type :input
    :onyx/medium :redis
    :redis/host "127.0.0.1"
    :redis/port 6379
    :redis/key ::store
    :redis/op :lpop
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Reads segments via redis"}

   {:onyx/name :inc
    :onyx/fn ::my-inc
    :onyx/type :function
    :onyx/batch-size batch-size}

   {:onyx/name :out-redis
    :onyx/plugin :onyx.plugin.redis/writer
    :onyx/ident :redis/write
    :onyx/type :output
    :onyx/medium :redis
    :onyx/fn ::create-writes
    :redis/host "127.0.0.1"
    :redis/port 6379
    :onyx/batch-size batch-size}])

(def workflow
  [[:in :inc]
   [:inc :out-redis]])

(def out-chan (async/chan 1000))

(defn inject-writer-ch [event lifecycle]
  {:core.async/chan out-chan})

(def out-lifecycle
  {:lifecycle/before-task-start inject-writer-ch})

(def lifecycles
  [{:lifecycle/task :in
    :lifecycle/calls :onyx.plugin.redis/reader-state-calls}])

(def v-peers (onyx.api/start-peers 3 peer-group))

(def job-id
  (:job-id
   (onyx.api/submit-job
    peer-config
    {:catalog catalog
     :workflow workflow
     :lifecycles lifecycles
     :task-scheduler :onyx.task-scheduler/balanced})))

(onyx.api/await-job-completion peer-config job-id)

(Thread/sleep 10000)

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-peer-group peer-group)

(onyx.api/shutdown-env env)

(let [vs (sort-by :n 
                  (wcar redis-conn
                        (car/lrange ::store-out 0 100000)))]
  (fact vs => 
        (map (fn [v]
               {:n (inc v)})
             (range n-messages))))

(fact (wcar redis-conn
            (car/flushall)
            (car/flushdb)) => ["OK" "OK"])
