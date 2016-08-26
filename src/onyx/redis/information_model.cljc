(ns onyx.redis.information-model)

(def model
  {:catalog-entry
   {:onyx.plugin.redis/reader
    {:model {:redis/uri
             {:type :string
              :doc "Redis uri to connect to."}

             :redis/key
             {:type :keyword
              :doc "Redis key to read against."}

             :redis/op
             {:type :keyword
              :doc "Redis operation to perform."}

             :redis/read-timeout-ms
             {:type :long
              :doc "Time to wait (in ms) before giving up on trying to write to Redis."}}}

    :onyx.plugin.redis/writer
    {:model {:redis/uri
             {:type :string
              :doc "Redis uri to connect to."}

             :redis/read-timeout-ms
             {:type :long
              :doc "Time to wait (in ms) before giving up on trying to write to Redis."}}}}

   :lifecycle-entry
   {:onyx.plugin.redis/reader
    {:model
     [{:task.lifecycle/name :reader-conn-spec
       :lifecycle/calls :onyx.plugin.redis/reader-conn-spec}]}

    :onyx.plugin.redis/writer
    {:model
     []}}

   :display-order
   {:onyx.plugin.redis/reader
    [:redis/uri
     :redis/op
     :redis/key
     :redis/read-timeout-ms]

    :onyx.plugin.redis/writer
    [:redis/uri
     :redis/read-timeout-ms]}})
