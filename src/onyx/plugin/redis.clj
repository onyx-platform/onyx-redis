(ns onyx.plugin.redis
  (:require [onyx.peer.pipeline-extensions :as p-ext]
            [taoensso.carmine :as car :refer (wcar)]
            [onyx.static.default-vals :refer [arg-or-default]]
            [clojure.core.async :refer [<!! timeout close! go <! take! go-loop
                                        >!! >! <!! <! chan] :as async]))

(defn batch-load-keys
  ([conn keystore]
   (batch-load-keys conn keystore 100))
  ([conn keystore step]
   (when-let [[keys _] (wcar conn
                             (car/lrange keystore 0 (dec step))
                             (car/ltrim keystore step -1))]
     keys)))

(batch-load-keys aaa "clarity.functions.denver-health/keystore")

(defn batch-load-records [conn keys]
  "Starts loading records"
  (wcar conn (mapv (fn [key]
                     (car/parse (partial assoc {} key) (car/smembers key))) keys)))


(defn take-from-redis [conn keystore batch-size steps timeout]
  "      conn: Carmine map for connecting to redis
     keystore: The name of a redis list for looking up the relevant sets/vals
   batch-size: The maximum size of a returned batch
        steps: granularity of each step
      timeout: stop processing new collections after `timeout`/ms''

  In order to stay consistent in Redis list consumption, this function will
  finish processing the batch it's currently on before returning. This means that
  if your batch sizes are large and steps are small, it is possible to block for
  and extended ammount of time. Returns nil if the list is exausted."
  (let [end (+ timeout (System/currentTimeMillis))
        step-size (int (Math/floor (/ batch-size steps)))
        return (atom [])]
    (loop [step steps
           end? (- end (System/currentTimeMillis))]
      (if (and (pos? end?)
               (pos? step))
        (do (let [keys (batch-load-keys conn keystore step-size)
                  records (batch-load-records conn keys)]
              (swap! return into records))
            (recur (dec step)
                   (- end (System/currentTimeMillis))))
        @return))))

;(def res (take-from-redis aaa "clarity.functions.denver-health/keystore" 10000 10 1000))

(defn get-key-list [conn keystore]
  "Returns the entire list of keystore elements"
  (wcar conn
        (car/lrange keystore 0 -1)))

(defn build-key-queue! [conn key-queue keystore]
  "Takes a conn and keystore key, returns a queue built up from it's constituents"
  (let [keys (wcar conn (car/smembers keystore))
        key-queue (str key-queue)]
    (doseq [k keys]
      (wcar conn (car/lpush key-queue k)))
    key-queue))

(defn inject-pending-state [event lifecycle]
  (let [task     (:onyx.core/task-map event)
        conn     (:redis/connection task)
        keystore (:redis/keystore task)]
    {:redis/conn             conn
     :redis/keystore         keystore
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
                    (let [key (wcar conn (car/lpop keystore))
                          message (wcar conn (car/smembers key))]
                      (if (and (not (empty? message))
                               (not (nil? key)))
                        (recur (conj segments
                                     {:id key
                                      :input :redis
                                      :message {key message}})
                               (inc cnt))
                        (conj segments
                              {:id :done
                               :input :redis
                               :message :done}))))))]
    (doseq [m batch]
      (swap! pending-messages assoc (:id m) (:message m)))
    {:onyx.core/batch batch}))

(defmethod p-ext/ack-message :redis/read-from-set
  [{:keys [redis/conn redis/keystore redis/pending-messages]} message-id]
  (swap! pending-messages dissoc message-id))

(defmethod p-ext/retry-message :redis/read-from-set
  [{:keys [redis/conn redis/keystore redis/pending-messages]} message-id]
  (let [msg (get @pending-messages message-id)]
    (when (not= msg :done)
      (wcar conn (car/lpush keystore message-id)))))

(defmethod p-ext/pending? :redis/read-from-set
  [{:keys [redis/pending-messages]} message-id]
  (get @pending-messages message-id))

(defmethod p-ext/drained? :redis/read-from-set
  [{:keys [redis/conn redis/keystore redis/pending-messages]}]
  (and (= (count @pending-messages) 1)
       (zero? (wcar conn (car/llen keystore)))))

(def reader-state-calls
  {:lifecycle/before-task-start inject-pending-state})

(def aaa {:pool {} :spec {:host "192.168.99.100"}})
