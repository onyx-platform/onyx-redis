(ns onyx.plugin.redis
  (:require [clojure.core.async :as async :refer [timeout]]
            [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.string :as string]
            [onyx.plugin.protocols :as p]
            [onyx.static
             [default-vals :refer [arg-or-default]]
             [uuid :refer [random-uuid]]]
            [onyx.types :as t]
            [taoensso.carmine :as car :refer [wcar]]))

;;;;;;;;;;;;;;;;;;;;
;; Redis operations

(defn redis-commands
  "Loads the official Redis command reference. The original
   JSON file can be found on
   https://github.com/antirez/redis-doc/blob/master/commands.json."
  []
  (-> (io/resource "commands.json")
      (slurp)
      (json/read-str :key-fn keyword)))

(defonce operations
  (letfn [(operation-name [key]
            (-> (name key)
                (string/replace #" " "-")
                (string/lower-case)))
          (resolve-operation [op-name]
            [(keyword op-name)
             (resolve (symbol "taoensso.carmine" op-name))])]
    (let [names (map operation-name (keys (redis-commands)))]
      (into {} (map resolve-operation) names))))

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

(defn perform-operation
  "Validates a Redis operation submitted to RedisWriter and executes it."
  [config message]
  (let [{:keys [allowed-commands ops]} config
        op ((:op message) (:ops config))]
    (assert op (if-not (some #{(:op message)} allowed-commands)
                 (str "The Redis operation " (:op message) " is not in "
                      "the list of allowed commands set via "
                      ":redis/allowed-commands: " allowed-commands)
                 (str "The Redis operation " (:op message) " is not "
                      "supported by onyx-redis / carmine. Supported "
                      "operations are: " (keys operations))))
    (assert (:args message)
            (str "Redis expected format was changed to expect: "
                 "{:op :operation :args [arg1, arg2, arg3]}"))
    (apply op (:args message))))


(defrecord RedisWriter [conn config]
  p/Plugin
  (start [this event] 
    this)

  (stop [this event] 
    this)

  p/BarrierSynchronization
  (synced? [this epoch]
    true)

  (completed? [this]
    true)

  p/Checkpointed
  (recover! [this _ _] 
    this)

  (checkpoint [this])

  (checkpointed! [this epoch])

  p/Output
  (prepare-batch [this event replica _]
    true)

  (write-batch [this {:keys [onyx.core/results]} replica _]
    (wcar conn (doall
                (map (partial perform-operation config)
                     (mapcat :leaves (:tree results)))))
    true))

(defn writer [event]
  (let [catalog-entry (:onyx.core/task-map event)
        uri (:redis/uri catalog-entry)
        allowed-commands (:redis/allowed-commands catalog-entry)
        _ (when-not uri
            (throw (ex-info ":redis/uri must be supplied to output task." catalog-entry)))
        conn          {:spec {:uri uri
                              :read-timeout-ms (or (:redis/read-timeout-ms catalog-entry)
                                                   4000)}}]
    (->RedisWriter conn {:allowed-commands allowed-commands
                         :ops (cond-> operations
                                allowed-commands
                                (select-keys allowed-commands))})))

(defn take-from-redis
  "conn: Carmine map for connecting to redis
  key: The name of a redis key for looking up the relevant values
  batch-size: The maximum size of a returned batch
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
