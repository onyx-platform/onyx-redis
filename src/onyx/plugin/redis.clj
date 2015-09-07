(ns onyx.plugin.redis
  (:require [onyx.peer.pipeline-extensions :as p-ext]
            [taoensso.carmine :as car :refer (wcar)]
            [onyx.static.default-vals :refer [arg-or-default]]
            [clojure.core.async :refer [<!! timeout close! go <! take! go-loop
                                        >!! >! <!! <! chan] :as async]
            [taoensso.timbre :refer [info]]
            [onyx.peer.function]
            [taoensso.carmine.connections]))

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
  (when-let [record-batch (wcar conn (mapv
                                      (fn [key]
                                        (car/parse (fn [set]
                                                     {:key key
                                                      :records set})
                                                   (car/smembers key))) keys))]
    (if (map? record-batch)
      [record-batch]
      record-batch)))

(defn take-from-redis
  "      conn: Carmine map for connecting to redis
     keystore: The name of a redis list for looking up the relevant sets/vals
   batch-size: The maximum size of a returned batch
        steps: granularity of each step
      timeout: stop processing new collections after `timeout`/ms''

  In order to stay consistent in Redis list consumption, this function will
  finish processing the batch it's currently on before returning. This means that
  if your batch sizes are large and steps are small, it is possible to block for
  and extended ammount of time. Returns nil if the list is exausted."
  [conn keystore batch-size steps timeout]

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
        (do (swap! return (partial reduce into []))
            @return)))))

(defn inject-pending-state [event lifecycle]
  (let [task     (:onyx.core/task-map event)
        host     (:redis/host task)
        port     (:redis/port task)
        keystore (:redis/keystore task)]
    (when (> (:onyx/max-peers task) 1)
      (throw (Exception. "Onyx-Redis can not run with :onyx/max-peers greater than 1")))
    {:redis/conn             {:spec {:host host :port port} :pool nil}
     :redis/keystore         keystore
     :redis/drained?         (atom false)
     :redis/pending-messages (atom {})}))

(defrecord RedisSetReader [max-pending batch-size batch-timeout
                           conn keystore pending-messages
                           drained? step-size]
  p-ext/Pipeline
  p-ext/PipelineInput
  (write-batch [this event]
    (onyx.peer.function/write-batch event))

  (read-batch [_ event]
    (let [pending (count @pending-messages)
          max-segments (min (- max-pending pending) batch-size)
          batch (if (pos? max-segments)
                  (when-let [records (take-from-redis conn
                                                      keystore max-segments
                                                      (or step-size 1)
                                                      batch-timeout)]
                    (if (seq records)
                      (mapv (fn [record]
                              {:id (java.util.UUID/randomUUID)
                               :input :redis
                               :message record})
                            records)
                      [{:id :done
                        :input :redis
                        :message :done}])))]
      (doseq [m batch]
        (swap! pending-messages assoc (:id m) (:message m)))
      {:onyx.core/batch batch}))

  (ack-segment
    [_ _ segment-id]
    (let [msg (get @pending-messages segment-id)]
      (swap! pending-messages dissoc segment-id)))

  (retry-segment
    [_ _ segment-id]
    (if (not (= :done segment-id))
      (let [msg (get @pending-messages segment-id)
            key (:key msg)]
        (wcar conn (car/rpush keystore key))
        (swap! pending-messages dissoc segment-id))))

  (pending?
    [_ _ segment-id]
    (get @pending-messages segment-id))

  (drained?
    [_ event]
    (and (contains? @pending-messages :done)
         (= 1 (count @pending-messages))
         (= 0 (wcar conn (car/llen keystore))))))

(defn read-sets-from-redis [pipeline-data]
  (let [catalog-entry    (:onyx.core/task-map pipeline-data)
        max-pending      (arg-or-default :onyx/max-pending catalog-entry)
        batch-size       (:onyx/batch-size catalog-entry)
        batch-timeout    (arg-or-default :onyx/batch-timeout catalog-entry)
        pending-messages (atom {})
        step-size        (:redis/step-size catalog-entry)
        drained?         (atom false)
        conn             {:pool nil
                          :spec {:host (:redis/host catalog-entry)
                                 :port (:redis/port catalog-entry)
                                 :read-timeout-ms (or (:redis/read-timeout-ms catalog-entry)
                                                      4000)}}
        keystore         (:redis/keystore catalog-entry)]
    (->RedisSetReader max-pending batch-size batch-timeout
                      conn keystore pending-messages
                      drained? step-size)))

(defrecord RedisSetWriter [conn keystore prefix]
  p-ext/Pipeline
  (read-batch [_ event]
    (onyx.peer.function/read-batch event))

  (write-batch [_ {:keys [onyx.core/results]}]
    (doseq [msg (mapcat :leaves (:tree results))]
      (let [segment (:message msg)
            key     (str prefix (:key segment))
            records (:records segment)]
        (wcar conn
              (mapv #(car/sadd key %) records)
              (car/lpush keystore key))
        {:onyx.core/written? true})))
  (seal-resource [_ _]
    {}))

(defn write-to-set [pipeline-data]
  (let [catalog-entry (:onyx.core/task-map pipeline-data)
        keystore      (:redis/keystore catalog-entry)
        conn          {:spec {:host (:redis/host catalog-entry)
                              :port (:redis/port catalog-entry)
                              :read-timeout-ms (or (:redis/read-timeout-ms catalog-entry)
                                                   4000)}}
        prefix        (or (:redis/key-prefix catalog-entry) nil)]
    (->RedisSetWriter conn keystore prefix)))

(defrecord Ops [sadd lpush])

(def operations 
  (->Ops car/sadd car/lpush))

(defrecord RedisWriter [conn keystore prefix]
  p-ext/Pipeline
  (read-batch [_ event]
    (onyx.peer.function/read-batch event))

  (write-batch [_ {:keys [onyx.core/results]}]
    (wcar conn 
          (map (fn [{:keys [message] :as leaf}] 
                  (let [op ((:op message) operations)] 
                    (op (car/key prefix (:key message)) (:value message)))) 
                (mapcat :leaves (:tree results))))
    {})
  (seal-resource [_ _]
    {}))

(defn writer [pipeline-data]
  (let [catalog-entry (:onyx.core/task-map pipeline-data)
        keystore      (:redis/keystore catalog-entry)
        conn          {:spec {:host (:redis/host catalog-entry)
                              :port (:redis/port catalog-entry)
                              :read-timeout-ms (or (:redis/read-timeout-ms catalog-entry)
                                                   4000)}}
        prefix        (or (:redis/key-prefix catalog-entry) nil)]
    (->RedisWriter conn keystore prefix)))

(def reader-state-calls
  {:lifecycle/before-task-start inject-pending-state})
