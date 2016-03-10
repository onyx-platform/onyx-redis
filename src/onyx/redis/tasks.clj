(ns onyx.redis.tasks
  (:require [schema.core :as s]
            [onyx.schema :as os]))

                                        ;   (s/defn ^:always-validate <BLA>
;;     ([task-name :- s/Keyword opts]
;;      {:task {:task-map (merge {:onyx/name task-name
;;                                :onyx/plugin
;;                                :onyx/type :input
;;                                :onyx/medium :datomic
;;                                :onyx/doc "Reads messages from a Kafka topic"}
;;                               opts)
;;              :lifecycles [{:lifecycle/task task-name
;;                            :lifecycle/calls }]}
;;       :schema {:task-map
;;                :lifecycles [os/Lifecycle]}})
;;     ([task-name :- s/Keyword
;;       task-opts :- {s/Any s/Any}]
;;      (<BLA> task-name (merge {}
;;                              task-opts))))

(def UserTaskMapKey
  (os/build-allowed-key-ns :redis))

(def DatomicReadLogTaskMap
  (s/->Both [os/TaskMap
             {:datomic/uri
              (s/optional-key :onyx/n-peers)
              UserTaskMapKey s/Any}]))

(s/defn ^:always-validate connected-task
  "Creates a redis connected task, where the first argument
   to the function located at kw-fn is a redis(carmine) connection"
  ([task-name :- s/Keyword opts]
   {:task {:task-map (merge {:onyx/name task-name
                             :onyx/type :function}
                            opts)
           :lifecycles [{:lifecycle/task task-name
                         :lifecycle/calls :onyx.plugin.redis/reader-conn-spec}]}
    :schema {:task-map os/TaskMap
             :lifecycles [os/Lifecycle]}})
  ([task-name :- s/Keyword
    kw-fn :- s/Keyword
    uri :- s/Str
    task-opts :- {s/Any s/Any}]
   (connected-task task-name (merge {:onyx/fn kw-fn
                                           :redis/param? true
                                           :redis/uri uri}
                                          task-opts))))

(def RedisConsumerTaskMap
  (s/->Both [os/TaskMap
             {:redis/uri s/Str
              :redis/key (s/either s/Str s/Keyword)
              :redis/op (s/enum :lpop :rpop :spop)
              (s/optional-key :redis/read-timeout-ms) s/Num
              UserTaskMapKey s/Any}]))

(s/defn ^:always-validate reader
  ([task-name :- s/Keyword opts]
   {:task {:task-map (merge {:onyx/name task-name
                             :onyx/plugin :onyx.plugin.redis/consumer
                             :onyx/type :input
                             :onyx/medium :redis
                             :onyx/max-peers 1}
                            opts)
           :lifecycles [{:lifecycle/task task-name
                         :lifecycle/calls :onyx.plugin.redis/reader-state-calls}]}
    :schema {:task-map RedisConsumerTaskMap
             :lifecycles [os/Lifecycle]}})
  ([task-name :- s/Keyword
    uri :- s/Str
    k :- (s/either s/Str s/Keyword)
    op :- (s/enum :lpop :rpop :spop)
    task-opts :- {s/Any s/Any}]
   (reader task-name (merge {:redis/uri uri
                                     :redis/key k
                                     :redis/op op}
                                    task-opts))))

(def RedisWriterTaskMap
  (s/->Both [os/TaskMap
             {:redis/uri s/Str
              (s/optional-key :redis/read-timeout-ms) s/Num
              UserTaskMapKey s/Any}]))

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
