(ns onyx.plugin.redis-lifecycle-test
  (:require [clojure.string :as s]
            [onyx.peer.pipeline-extensions :as p-ext]
            [onyx.plugin.redis :refer :all]
            [onyx.plugin.core-async :refer [take-segments!]]
            [midje.sweet :refer :all]
            [taoensso.timbre :refer [info]]
            [taoensso.carmine :as car :refer [wcar]]
            [clojure.core.async :as async :refer [chan <!! >!!]]
            [onyx.api]))

(def id (java.util.UUID/randomUUID))
(def zkAddress ["127.0.0.1" 2188])

(def env-config
  {:zookeeper/address (s/join ":" zkAddress)
   :zookeeper/server? true
   :zookeeper.server/port (second zkAddress)
   :onyx/tenancy-id id})

(def peer-config
  {:zookeeper/address (s/join ":" zkAddress)
   :onyx.peer/job-scheduler :onyx.job-scheduler/greedy
   :onyx.messaging/impl :aeron
   :onyx.messaging/peer-port 40200
   :onyx.messaging/bind-addr "localhost"
   :onyx/tenancy-id id})

(def env (onyx.api/start-env env-config))

(def peer-group (onyx.api/start-peer-group peer-config))

(def n-messages (rand-int 1000))

(def batch-size 8)

  (def redis-uri "redis://192.168.99.100:6379")
(def redis-conn {:spec {:uri redis-uri}})

;;;;; Load up the redis with test data
;;;;;
;;;;;
(doseq [n (range n-messages)]
  (wcar redis-conn
        (car/lpush ::some-key n)
        (car/pfadd ::hll_some-key n)
        ))

(defn my-lookup [conn segment]
  {:results (wcar conn
                  (car/lrange (:key segment) 0 1000))})

(def catalog
  [{:onyx/name :in
    :onyx/plugin :onyx.plugin.core-async/input
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Reads segments via core async"}

   {:onyx/name :lookup
    :onyx/fn ::my-lookup
    :redis/uri redis-uri
    :redis/param? true
    :onyx/type :function
    :onyx/batch-size batch-size}

   {:onyx/name :out
    :onyx/plugin :onyx.plugin.core-async/output
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/doc ""
    :onyx/max-peers 1}])

(def workflow
  [[:in :lookup]
   [:lookup :out]])

(def in-chan (chan 1000))

(>!! in-chan {:key ::some-key})
(>!! in-chan :done)

(def out-chan (async/chan 1000))

(defn inject-reader-ch [event lifecycle]
  {:core.async/chan in-chan})

(def in-lifecycle
  {:lifecycle/before-task-start inject-reader-ch})

(defn inject-writer-ch [event lifecycle]
  {:core.async/chan out-chan})

(def out-lifecycle
  {:lifecycle/before-task-start inject-writer-ch})

(def lifecycles
  [{:lifecycle/task :in
    :lifecycle/calls ::in-lifecycle}
   {:lifecycle/task :in
    :lifecycle/calls :onyx.plugin.core-async/reader-calls}

   {:lifecycle/task :lookup
    :lifecycle/calls :onyx.plugin.redis/reader-conn-spec

    :lifecycle/doc "Initialises redis conn spec into event map, or as a :onyx.core/param"}

   {:lifecycle/task :out
    :lifecycle/calls ::out-lifecycle}
   {:lifecycle/task :out
    :lifecycle/calls :onyx.plugin.core-async/writer-calls}])

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

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-peer-group peer-group)

(onyx.api/shutdown-env env)

(def results (take-segments! out-chan))

(fact results => [{:results (map str (reverse (range n-messages)))}
                  :done])

(fact (wcar redis-conn
            (car/flushall)
            (car/flushdb)) => ["OK" "OK"])
