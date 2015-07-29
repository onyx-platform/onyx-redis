(ns onyx.plugin.redis
  (:require [onyx.peer.pipeline-extensions :as p-ext]
            [taoensso.carmine :as car :refer (wcar)]
            [onyx.static.default-vals :refer [arg-or-default]]
            [clojure.core.async :refer [<!! timeout close! go <! take!]]))

(def server1-conn {:pool {} :spec {:host "192.168.99.100"}})
(defmacro wcar* [& body]
  `(car/wcar server1-conn ~@body))

(defn get-next-item [conn key-set]
  (when-let [key (wcar conn (car/lrange key-set 0 1))]
    {key (wcar conn (car/smembers key))}))

(defn build-key-queue! [conn key-queue keystore]
  "Takes a conn and keystore key, returns a queue built up from it's constituents"
  (let [keys (wcar conn (car/smembers keystore))
        key-queue (str key-queue)]
    (doseq [k keys]
      (wcar conn (car/lpush key-queue k)))
    key-queue))

(defn inject-pending-state [event lifecycle]
  (let [task (:onyx.core/task-map event)]
    {:redis/conn (:redis/connection task)
     :redis/keystore (:redis/keystore task)
     :redis/pending-messages (atom {})}))

(defmethod p-ext/read-batch :redis/read-from-set
  [{:keys [onyx.core/task-map redis/conn redis/keystore redis/pending-messages]}]
  (let [pending (count @pending-messages)
        max-pending (arg-or-default :onyx/max-pending task-map)
        batch-size (:onyx/batch-size task-map)
        max-segments (min (- max-pending pending) batch-size)
        ms (arg-or-default :onyx/batch-timeout task-map)
        batch-end-time (+ ms (System/currentTimeMillis))
        batch (if (pos? max-segments)
                (loop [segments [] cnt 0]

                  (if (or (= cnt max-segments)
                          (neg? (- batch-end-time (System/currentTimeMillis))))
                    segments
                    (let [key (wcar conn (car/rpoplpush keystore keystore))
                          message (wcar conn (car/smembers key))]
                      (if message
                        (recur (conj segments
                                     {:id key
                                      :input :redis
                                      :message message})
                               (inc cnt))
                        segments)))))]
    (doseq [m batch]
      (swap! pending-messages assoc (:id m) (:message m)))
    {:onyx.core/batch batch}))

(defmethod p-ext/ack-message :redis/read-from-set
  [{:keys [redis/conn redis/keystore redis/pending-messages]} message-id]
  (wcar conn (car/lrem keystore 0 message-id))
  (swap! pending-messages dissoc message-id))

(defmethod p-ext/retry-message :redis/read-from-set
  [{:keys [redis/conn]} message-id]
  (comment "Dont need this"))

(defmethod p-ext/pending? :redis/read-from-set
  [{:keys []} message-id]
  (get @pending-messages message-id))

(defmethod p-ext/drained? :redis/read-from-set
  [{:keys [redis/conn redis/keystore redis/pending-messages]}]
  (and (= (count @pending-messages) 1)
       (zero? (wcar conn (car/llen keystore)))))

(def reader-state-calls
  {:lifecycle/before-task-start inject-pending-state})
