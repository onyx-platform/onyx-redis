(ns onyx.plugin.redis-test
  (:require [onyx.peer.pipeline-extensions :as p-ext]
            [onyx.plugin.redis]
            [midje.sweet :refer :all]
            [taoensso.carmine :as car :refer [wcar]]
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

(def n-messages 1000)

(def batch-size 20)

(def input-queue-name (str "input_queue_" (Math/abs (hash (java.util.UUID/randomUUID)))))

(def output-queue-name (str "output_queue_" (Math/abs (hash (java.util.UUID/randomUUID)))))

(def dir "/tmp")

(def conn (d/queues dir {}))

(doseq [n (range n-messages)]
  (d/put! conn input-queue-name {:n n}))

(d/put! conn input-queue-name :done)

(defn my-inc [{:keys [n] :as segment}]
  (assoc segment :n (inc n)))

(def catalog
  [{:onyx/name :in
    :onyx/ident :durable-queue/read-from-queue
    :onyx/type :input
    :onyx/medium :durable-queue
    :durable-queue/queue-name input-queue-name
    :durable-queue/directory dir
    :durable-queue/fsync-take? true
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Reads segments via durable-queue"}

   {:onyx/name :inc
    :onyx/fn :onyx.plugin.durable-queue-test/my-inc
    :onyx/type :function
    :onyx/batch-size batch-size}

   {:onyx/name :out
    :onyx/ident :durable-queue/write-to-queue
    :onyx/type :output
    :onyx/medium :durable-queue
    :durable-queue/queue-name output-queue-name
    :durable-queue/directory dir
    :durable-queue/fsync-put? true
    :onyx/batch-size batch-size
    :onyx/doc "Writes segments via durable-queue"}])

(def workflow
  [[:in :inc]
   [:inc :out]])

(def lifecycles
  [{:lifecycle/task :in
    :lifecycle/calls :onyx.plugin.durable-queue/reader-state-calls}
   {:lifecycle/task :in
    :lifecycle/calls :onyx.plugin.durable-queue/reader-connection-calls}
   {:lifecycle/task :out
    :lifecycle/calls :onyx.plugin.durable-queue/writer-calls}])

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

(def results
  (loop [rets []]
    (let [x (d/take! (d/queues dir {}) output-queue-name 1000 nil)]
      (when x
        (d/complete! x))
      (cond (nil? x)
            rets
            (= @x :done)
            (conj rets @x)
            :else
            (recur (conj rets @x))))))

(let [expected (set (map (fn [x] {:n (inc x)}) (range n-messages)))]
  (fact (set (butlast results)) => expected)
  (fact (last results) => :done))

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-peer-group peer-group)

(onyx.api/shutdown-env env)
