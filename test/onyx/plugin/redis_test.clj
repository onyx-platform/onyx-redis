(ns onyx.plugin.redis-test
  (:require [onyx.peer.pipeline-extensions :as p-ext]
            [onyx.plugin.redis :refer :all]
            [clojure.core.async :refer [chan go-loop >!! <!! <!]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [midje.sweet :refer :all]
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
   :onyx.messaging/impl :core.async
   :onyx.messaging/bind-addr "localhost"
   :onyx/id id})

(def env (onyx.api/start-env env-config))

(def peer-group (onyx.api/start-peer-group peer-config))

(def n-messages (rand-int 1000))

(def batch-size 10)

(def redis-conn {:spec {:host "192.168.99.100"}})


;;;;; Load up the redis with test data
;;;;;
;;;;;
(doseq [n (range n-messages)]
  (let [message {::key (str (Math/abs (hash n)))
                 :hello :world}]
    (wcar redis-conn
          (car/sadd (::key message) message)
          (car/lpush ::keystore (::key message)))))

;;;;;
;;;;;
;;;;;
(defn my-inc [{:keys [n] :as segment}]
  segment)

(def catalog
  [{:onyx/name :in
    :onyx/plugin :onyx.plugin.redis/read-sets-from-redis
    :onyx/ident :redis/read-from-set
    :onyx/type :input
    :onyx/medium :redis
    :redis/host "192.168.99.100"
    :redis/port 6379
    :redis/keystore ::keystore
    :redis/step-size 1
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Reads segments via redis"}

   {:onyx/name :inc
    :onyx/fn ::my-inc
    :onyx/type :function
    :onyx/batch-size batch-size}

   {:onyx/name :out
    :onyx/plugin :onyx.plugin.core-async/output
    :onyx/ident :core.async/write-to-chan
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/doc ""
    :onyx/max-peers 1}])

(def workflow
  [[:in :inc]
   [:inc :out]])

(def out-chan (async/chan 1000))

(defn inject-writer-ch [event lifecycle]
  {:core.async/chan out-chan})

(def out-lifecycle
  {:lifecycle/before-task-start inject-writer-ch})

(def lifecycles
  [{:lifecycle/task :in
    :lifecycle/calls :onyx.plugin.redis/reader-state-calls}

   {:lifecycle/task :out
    :lifecycle/calls ::out-lifecycle}

   {:lifecycle/task :out
    :lifecycle/calls :onyx.plugin.core-async/writer-calls}])

(def retry? (atom true))

(defn retry-once [event _ segment all-new]
  (let [match (str (Math/abs (hash 2)))
        key (:key segment)]
    (if (and (= key match) @retry?)
      (do (reset! retry? false)
          true)
      false)))

(def constantly-true (constantly true))

(def flow
  [{:flow/from :inc
    :flow/to :none
    :flow/short-circuit? true
    :flow/predicate ::retry-once
    :flow/action :retry}

   {:flow/from :inc
    :flow/to [:out]
    :flow/predicate ::constantly-true}])

(def v-peers (onyx.api/start-peers 3 peer-group))

(def job-id
  (:job-id
   (onyx.api/submit-job
    peer-config
    {:catalog catalog
     :workflow workflow
     :lifecycles lifecycles
     :flow-conditions flow
     :task-scheduler :onyx.task-scheduler/balanced})))

(def r (take-segments! out-chan))

(onyx.api/await-job-completion peer-config job-id)

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-peer-group peer-group)

(onyx.api/shutdown-env env)

(fact (count r) => (inc n-messages))
(fact (last r) => :done)
(fact @retry? => false)

(let [ochan (chan)
      _ (wcar redis-conn
              (mapv (partial car/lpush :testtest)
                    (reverse (clojure.string/split
                              "hello from earth!" #" "))))
      res (batch-load-keys redis-conn :testtest 3)]
  (let [[h f e] res]
    (fact h => "hello")
    (fact f => "from")
    (fact e => "earth!")))

(fact (wcar redis-conn
            (car/flushall)
            (car/flushdb)) => ["OK" "OK"])
