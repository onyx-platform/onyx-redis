(ns onyx.plugin.redis
  (:require [onyx.peer.pipeline-extensions :as p-ext]
            [taoensso.carmine :as car :refer (wcar)]
            [onyx.static.default-vals :refer [arg-or-default]]
            [clojure.core.async :refer [<!! timeout close! go <! take! go-loop
                                        >!! >! <!! <! chan] :as async]
            [onyx.static.uuid :refer [random-uuid]]
            [taoensso.timbre :refer [info]]
            [onyx.types :as t]
            [onyx.peer.function]
            [taoensso.carmine.connections]))


(defrecord Ops [sadd lpush zadd set lpop spop rpop pfcount pfadd])

(def operations
  (->Ops car/sadd car/lpush car/zadd car/set car/lpop car/spop car/rpop car/pfcount car/pfadd))

;;;;;;;;;;;;;;;;;;;;
;; Connection lifecycle code

(defn inject-conn-spec [{:keys [onyx.core/params onyx.core/task-map] :as event}
                        {:keys [redis/param? redis/uri redis/read-timeout-ms] :as lifecycle}]
  (when-not (or uri (:redis/uri task-map))
      (throw (ex-info ":redis/uri must be supplied to output task." lifecycle)))
  (let [conn {:spec {:uri (or (:redis/uri task-map) uri)
                     :read-timeout-ms (or read-timeout-ms 4000)}}]
    {:onyx.core/params (if (or (:redis/param? task-map) param?)
                         (conj params conn)
                         params)
     :redis/conn conn}))

(def reader-conn-spec
  {:lifecycle/before-task-start inject-conn-spec})

;;;;;;;;;;;;;;;;;;;;;
;; Output plugin code


(defrecord RedisWriter [conn]
  p-ext/Pipeline
  (read-batch [_ event]
    (onyx.peer.function/read-batch event))

  (write-batch [_ {:keys [onyx.core/results]}]
    (wcar conn
          (doall
            (map (fn [{:keys [message]}]
                   (let [op ((:op message) operations)]
                     (assert (:args message) "Redis expected format was changed to expect: {:op :operation :args [arg1, arg2, arg3]}")
                     (apply op (:args message))))
                 (mapcat :leaves (:tree results)))))
    {})
  (seal-resource [_ _]
    {}))

(defn writer [pipeline-data]
  (let [catalog-entry (:onyx.core/task-map pipeline-data)
        uri (:redis/uri catalog-entry)
        _ (when-not uri
            (throw (ex-info ":redis/uri must be supplied to output task." catalog-entry)))
        conn          {:spec {:uri uri
                              :read-timeout-ms (or (:redis/read-timeout-ms catalog-entry)
                                                   4000)}}]
    (->RedisWriter conn)))

;;;;;;;;;;;;;;;;;;;;;
;; Input plugin code

(defn inject-pending-state [event lifecycle]
  (let [pipeline (:onyx.core/pipeline event)
        task     (:onyx.core/task-map event)]
    (when (> (:onyx/max-peers task) 1)
      (throw (Exception. "Onyx-Redis can not run with :onyx/max-peers greater than 1")))
    {:redis/conn             (:conn pipeline)
     :redis/drained?         (:drained? pipeline)
     :redis/pending-messages (:pending-messages pipeline)}))

(defn all-done? [messages]
  (empty? (remove #(= :done (:message %))
                  messages)))

(defn take-from-redis
  "conn: Carmine map for connecting to redis
  key: The name of a redis key for looking up the relevant values
  batch-size: The maximum size of a returned batchs
  timeout: stop processing new collections after `timeout`/ms''

  In order to stay consistent in Redis list consumption, this function will
  finish processing the batch it's currently on before returning. This means that
  if your batch sizes are large and steps are small, it is possible to block for
  and extended ammount of time. Returns nil if the list is exausted."
  [conn k batch-size timeout]
  (let [end (+ timeout (System/currentTimeMillis))]
    (loop [return []]
      (if (and (< (System/currentTimeMillis) end)
               (< (count return) batch-size))
        (let [vs (wcar conn
                       (doall (map (fn [_]
                                     (car/lpop k))
                                   (range (- batch-size (count return))))))]
              (recur (into return vs)))
        return))))

(defrecord RedisConsumer [max-pending batch-size batch-timeout conn k pending-messages drained?]
  p-ext/Pipeline
  p-ext/PipelineInput
  (write-batch [this event]
    (onyx.peer.function/write-batch event))

  (read-batch [_ event]
    (let [pending (count @pending-messages)
          max-segments (min (- max-pending pending) batch-size)
          batch (keep (fn [v]
                       (cond (= v "done")
                             (t/input (random-uuid)
                                      :done)
                             v
                             (t/input (random-uuid)
                                      {:key k
                                       :value v})))
                     (take-from-redis conn k batch-size batch-timeout))]
      (if (and (all-done? (vals @pending-messages))
               (all-done? batch)
               (or (not (empty? @pending-messages))
                   (not (empty? batch))))
        (reset! drained? true)
        ;; Allow unset to reduce the chance of draining race conditions
        ;; I believe these are still possible in this plugin thanks to the
        ;; sentinel handling
        (reset! drained? false))
      (doseq [m batch]
        (swap! pending-messages assoc (:id m) m))
      {:onyx.core/batch batch}))

  (ack-segment
    [_ _ segment-id]
    (let [msg (get @pending-messages segment-id)]
      (swap! pending-messages dissoc segment-id)))

  (retry-segment
    [_ _ segment-id]
    (when-let [msg (get @pending-messages segment-id)]
      (wcar conn
            (car/lpush k (:message msg)))
      (swap! pending-messages dissoc segment-id)))

  (pending?
    [_ _ segment-id]
    (get @pending-messages segment-id))

  (drained?
    [_ event]
    @drained?))

(defn consumer [pipeline-data]
  (let [catalog-entry    (:onyx.core/task-map pipeline-data)
        max-pending      (arg-or-default :onyx/max-pending catalog-entry)
        batch-size       (:onyx/batch-size catalog-entry)
        batch-timeout    (arg-or-default :onyx/batch-timeout catalog-entry)
        pending-messages (atom {})
        drained?         (atom false)
        read-timeout     (or (:redis/read-timeout-ms catalog-entry) 4000)
        k (:redis/key catalog-entry)
        uri (:redis/uri catalog-entry)
        _ (when-not uri
            (throw (ex-info ":redis/uri must be supplied to output task." catalog-entry)))
        op (or ((:redis/op catalog-entry) operations)
               (throw (Exception. (str "redis/op not found."))))
        conn             {:pool nil
                          :spec {:uri uri
                                 :read-timeout-ms read-timeout}}]
    (->RedisConsumer max-pending batch-size batch-timeout
                     conn k pending-messages
                     drained?)))

(def reader-state-calls
  {:lifecycle/before-task-start inject-pending-state})
