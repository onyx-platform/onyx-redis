(ns onyx.plugin.redis-writer-test
  (:require [clojure.string :as s]
            [onyx.peer.pipeline-extensions :as p-ext]
            [onyx.plugin.redis :refer :all]
            [clojure.core.async :refer [chan go-loop >!! <!! <! close!]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [midje.sweet :refer :all]
            [taoensso.carmine :as car :refer [wcar]]
            [clojure.core.async :as async :refer [chan <!! >!!]]
            [onyx.api]))

(def id (java.util.UUID/randomUUID))
(def zkAddress ["127.0.0.1" 2188])

(def env-config
  {:zookeeper/address (s/join ":" zkAddress)
   :zookeeper/server? true
   :zookeeper.server/port (second zkAddress)
   :onyx/id id})

(def peer-config
  {:zookeeper/address (s/join ":" zkAddress)
   :onyx.peer/job-scheduler :onyx.job-scheduler/greedy
   :onyx.messaging/impl :aeron
   :onyx.messaging/peer-port 40200
   :onyx.messaging/bind-addr "localhost"
   :onyx/id id})

(def env (onyx.api/start-env env-config))

(def peer-group (onyx.api/start-peer-group peer-config))

(def n-messages (rand-int 1000))

(def batch-size 8)

(def redis-uri "redis://127.0.0.1:6379")
(def redis-conn {:spec {:uri redis-uri}})

(def hll-counter "hll_counter")

(def messages
  [{:op :sadd :args ["cyclists" {:name "John" :age 20}]}
   {:op :sadd :args ["runners" {:name "Mike" :age 24}]}
   {:op :sadd :args ["cyclists" {:name "Jane" :age 25}]}
   {:op :sadd :args ["runners" {:name "Mike" :age 24}]}
   {:op :publish :args ["race-stats" {:leader "Mike"}]}
   {:op :publish :args ["race-stats" {:leader "Jane"}]}
   {:op :publish :args ["race-stats" {:leader "John"}]}
   {:op :pfadd :args [hll-counter 1]}
   {:op :pfadd :args [hll-counter 2]}
   :done])

(def in-chan (chan (count messages)))

(doseq [m messages]
  (>!! in-chan m))

(close! in-chan)

(def redis-subscription (atom []))

(car/with-new-pubsub-listener redis-conn
  {"race-stats" (fn f1 [msg] (swap! redis-subscription conj msg))}
  (car/subscribe "race-stats"))

(defn inject-in-ch [event lifecycle]
  {:core.async/chan in-chan})

(def in-calls
  {:lifecycle/before-task-start inject-in-ch})

(def lifecycles
  [{:lifecycle/task :in
    :lifecycle/calls ::in-calls}
   {:lifecycle/task :in
    :lifecycle/calls :onyx.plugin.core-async/reader-calls}])

(def catalog
  [{:onyx/name :in
    :onyx/plugin :onyx.plugin.core-async/input
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Reads segments via redis"}

   {:onyx/name :out-redis
    :onyx/plugin :onyx.plugin.redis/writer
    :onyx/type :output
    :onyx/medium :redis
    :redis/uri redis-uri
    :onyx/batch-size batch-size}])

(def workflow
  [[:in :out-redis]])

(def v-peers (onyx.api/start-peers 2 peer-group))

(def job-id
  (:job-id
   (onyx.api/submit-job
    peer-config
    {:catalog catalog
     :workflow workflow
     :lifecycles lifecycles
     :task-scheduler :onyx.task-scheduler/balanced})))

(onyx.api/await-job-completion peer-config job-id)

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-peer-group peer-group)

(onyx.api/shutdown-env env)

(fact (sort-by :name
               (car/wcar redis-conn
                         (set (car/smembers "cyclists"))))
      =>
      [{:name "Jane" :age 25}
       {:name "John" :age 20}])

(fact (sort-by :name
               (car/wcar redis-conn
                         (set (car/smembers "runners"))))
      =>
      [{:name "Mike" :age 24}])

(fact (car/wcar redis-conn (car/pfcount hll-counter))
      =>
      2
      )

(fact (take 4 @redis-subscription)
      =>
      [["subscribe" "race-stats" 1]
       ["message" "race-stats" {:leader "Mike"}]
       ["message" "race-stats" {:leader "Jane"}]
       ["message" "race-stats" {:leader "John"}]]
      )
