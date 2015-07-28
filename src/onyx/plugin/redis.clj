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

(defn create-pending-state [event lifecycle]
  (let [task (:onyx.core/task-map event)
        keystore (:redis/keystore task)
        conn (:redis/connection task)
        keystore-queue (build-key-queue! conn (str keystore "-queue") keystore)]
    {:redis/conn conn
     :redis/keystore keystore
     :redis/keystore-queue keystore-queue}))

(defmethod p-ext/read-batch :redis/read-from-set
  [{:keys [onyx.core/task-map redis/conn redis/keystore redis/keystore-queue]}]
  (let [task-map {:onyx/batch-size 5 :onyx/max-pending 10000 :onyx/batch-timeout 1000}
        pending (wcar conn (car/llen keystore-queue))
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
                    (let [next-key (wcar conn (car/rpoplpush keystore-queue keystore-queue))
                          message (wcar conn (car/smembers next-key))]
                      (if message
                        (recur (conj segments
                                     {:id next-key
                                      :input :redis
                                      :message {next-key message}})
                               (inc cnt))
                        segments)))))]
    {:onyx.core/batch batch}))

(defmethod p-ext/ack-message :redis/read-from-set
  [{:keys [redis/conn redis/keystore redis/keystore-queue]} message-id]
  (wcar conn (car/lrem keystore-queue 0 message-id)))

(defmethod p-ext/retry-message :redis/read-from-set ;; fix these to be actually stateless...
  [{:keys [redis/conn redis/keystore-queue]} message-id]
  (wcar conn (car/lpush keystore-queue message-id)))

(defmethod p-ext/pending? :redis/read-from-set
  [{:keys []} message-id]
  (get @pending-messages message-id))

(defmethod p-ext/drained? :redis/read-from-set
  [])

;;;; Steps for using a multi-tenant hashmap in redis.
;;;;;; In order to achieve multi-tenant hashmaps with a low probability of key collision,
;;;;;; namespace your key's, and store all the namespaced keys in a single set.
;;;;;; The onyx-redis plugin will take the the name of a key, that links to the set of keys.
;;;;;; Each key read will become a segment tagged as {:key contents}.
;;;;;;
;;;;;; To achieve this, we place all the keys from the key set into a linked list. This
;;;;;; gives us explicit ordering.
;;;;;;
;;;;;;


                                        ;(get-next-item :df 1)

                                        ;(keys @pending-messages)
                                        ;(deref (future (get-next-item "clarity.functions.denver-health/keystore")) 10000 nil)


(let
    {:onyx.core/batch batch})
