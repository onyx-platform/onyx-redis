(ns onyx.tasks.redis
  (:require [onyx.schema :as os]
            [schema.core :as s]))

(s/defn ^:always-validate connected-task
  "Creates a redis connected task, where the first argument
   to the function located at kw-fn is a redis(carmine) connection"
  ([task-name :- s/Keyword opts]
   {:task {:task-map (merge {:onyx/name task-name
                             :onyx/type :function}
                            opts)
           :lifecycles [{:lifecycle/task task-name
                         :lifecycle/calls :onyx.plugin.redis/reader-conn-spec}]}})
  ([task-name :- s/Keyword
    kw-fn :- s/Keyword
    uri :- s/Str
    task-opts :- {s/Any s/Any}]
   (connected-task task-name (merge {:onyx/fn kw-fn
                                     :redis/param? true
                                     :redis/uri uri}
                                    task-opts))))

(def RedisReaderTaskMap
  {:redis/uri s/Str
   :redis/key (s/either s/Str s/Keyword)
   :redis/op (s/enum :lpop :rpop :spop)
   (s/optional-key :redis/read-timeout-ms) s/Num
   (os/restricted-ns :redis) s/Any})

(s/defn ^:always-validate reader
  ([task-name :- s/Keyword opts]
   {:task {:task-map (merge {:onyx/name task-name
                             :onyx/plugin :onyx.plugin.redis/reader
                             :onyx/type :input
                             :onyx/medium :redis
                             :onyx/max-peers 1}
                            opts)
           :lifecycles [{:lifecycle/task task-name
                         :lifecycle/calls :onyx.plugin.redis/reader-state-calls}]}
    :schema {:task-map RedisReaderTaskMap}})
  ([task-name :- s/Keyword
    uri :- s/Str
    k :- (s/either s/Str s/Keyword)
    op :- (s/enum :lpop :rpop :spop)
    task-opts :- {s/Any s/Any}]
   (reader task-name (merge {:redis/uri uri
                             :redis/key k
                             :redis/op op} task-opts))))

(def RedisWriterTaskMap
  {:redis/uri s/Str
   (s/optional-key :redis/read-timeout-ms) s/Num
   (os/restricted-ns :redis) s/Any})

(s/defn ^:always-validate writer
  ([task-name :- s/Keyword opts]
   {:task {:task-map (merge {:onyx/name task-name
                             :onyx/plugin :onyx.plugin.redis/writer
                             :onyx/type :output
                             :onyx/medium :redis}
                            opts)}
    :schema {:task-map RedisWriterTaskMap}})
  ([task-name :- s/Keyword
    uri :- s/Str
    task-opts :- {s/Any s/Any}]
   (writer task-name (merge {:redis/uri uri}
                            task-opts))))
