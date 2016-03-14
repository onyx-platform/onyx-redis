(ns onyx.plugin.redis
  (:require [clojure.core.async :as async :refer [timeout]]
            [onyx.peer function
             [pipeline-extensions :as p-ext]]
            [onyx.static
             [default-vals :refer [arg-or-default]]
             [uuid :refer [random-uuid]]]
            [onyx.types :as t]
            [taoensso.carmine :as car :refer [wcar]]))

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
