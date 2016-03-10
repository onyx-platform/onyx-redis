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

(s/defn ^:always-validate redis-connected-task
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
   (redis-connection-arg task-name (merge {:onyx/fn kw-fn
                                           :onyx/param? true
                                           :redis/uri uri}
                                          task-opts))))
