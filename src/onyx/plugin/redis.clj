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
              (swap! return conj records))
            (recur (dec step)
                   (- end (System/currentTimeMillis))))
        (do (swap! return flatten)
            ()@return)))))

(defn inject-pending-state [event lifecycle]
  (let [task     (:onyx.core/task-map event)
        conn     (:redis/connection task)
        keystore (:redis/keystore task)
        drained? (atom false)]
    {:redis/conn             conn
     :redis/keystore         keystore
     :redis/drained?         drained?
     :redis/pending-messages (atom {})}))

(defmethod p-ext/read-batch :redis/read-from-set
  [{:keys [onyx.core/task-map redis/conn redis/keystore
           redis/pending-messages redis/drained? redis/step-size]}]
  (let [pending (count @pending-messages)
        max-pending (arg-or-default :onyx/max-pending task-map)
        batch-size (:onyx/batch-size task-map)
        max-segments (min (- max-pending pending) batch-size)
        ms (arg-or-default :onyx/batch-timeout task-map)
        batch (if (pos? max-segments)
                (when-let [records (take-from-redis conn keystore batch-size (or step-size 1) ms)]
                  (mapv (fn [record]
                          (if record
                            {:id (java.util.UUID/randomUUID)
                             :input :redis
                             :message record}
                            {:id (java.util.UUID/randomUUID)
                             :input :redis
                             :message :done}))
                        records)))]
    (doseq [m batch]
      (when (and (= (:message m) :done)
                 (= 0 (wcar conn (car/llen keystore))))

        (reset! drained? true))
      (swap! pending-messages assoc (:id m) (:message m)))
    {:onyx.core/batch batch}))

(defmethod p-ext/ack-message :redis/read-from-set
  [{:keys [redis/conn redis/keystore redis/pending-messages]} message-id]
  (when-let [message (get @pending-messages message-id)]
    (swap! pending-messages dissoc message-id)))

(defmethod p-ext/retry-message :redis/read-from-set
  [{:keys [redis/conn redis/keystore redis/pending-messages]} message-id]
  (when-let [message (get @pending-messages message-id)]
    (wcar conn
          (car/rpush keystore (first (keys message))))))

(defmethod p-ext/pending? :redis/read-from-set
  [{:keys [redis/pending-messages]} message-id]
  (get @pending-messages message-id))

(defmethod p-ext/drained? :redis/read-from-set
  [{:keys [redis/conn redis/keystore redis/pending-messages redis/drained?]}]
  @drained?)

(def reader-state-calls
  {:lifecycle/before-task-start inject-pending-state})
