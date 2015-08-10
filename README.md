## onyx-redis

Onyx plugin for redis.

#### Installation

In your project file:

```clojure
[onyx-redis "0.7.0.2"]
```

In your peer boot-up namespace:

```clojure
(:require [onyx.plugin.redis])
```

#### Functions

##### sample-entry

Catalog entry:

```clojure

{:onyx/name :in-from-redis
:onyx/plugin :onyx.plugin.redis/read-sets-from-redis
:onyx/ident :redis/read-from-set
:onyx/type :input
:onyx/medium :redis
:redis/host "127.0.0.1"
:redis/port 6379
:redis/keystore ::keystore
:redis/step-size 5
:onyx/batch-size batch-size
:onyx/max-peers 1
:onyx/doc "Reads segments via redis"}

{:onyx/name :out-to-redis
:onyx/plugin :onyx.plugin.redis/write-to-set
:onyx/ident :redis/write-to-set
:onyx/type :output
:onyx/medium :redis
:redis/key-prefix "new"
:redis/host "127.0.0.1"
:redis/port 6379
:redis/keystore ::keystore-out
:onyx/batch-size batch-size}
```

Lifecycle entry:

```clojure
[{:lifecycle/task :input-task
  :lifecycle/calls :onyx.plugin.redis/reader-state-calls}]
```

#### Attributes
##### Read from Redis

|key                           | type                 | description
|------------------------------|----------------------|------------
|`:redis/host`                 | `string`             | Redis hostname
|`:redis/port`                 | `int`                | Redis port
|`:redis/keystore`             |`keyword` or `string` | A Redis [list](http://redis.io/topics/data-types) containing each the keys of each set the plugin is concerned with
|`:redis/step-size`            |`int`                 | The step granularity to batch requests to Redis. defaults to 10
|`:redis/read-timeout-ms`      |`int`                 | Time to wait (in ms) before giving up on trying to read from Redis.

##### Write to Redis

|key                           | type                 | description
|------------------------------|----------------------|------------
|`:redis/host`                 | `string`             | Redis hostname
|`:redis/port`                 | `int`                | Redis port
|`:redis/keystore`             |`keyword` or `string` | A Redis key to write a reference key for each output.
|`:redis/key-prefix`           |`str`                 | A prefix for each set key. Leave blank for update-in-place.
|`:redis/read-timeout-ms`      |`int`                 | Time to wait (in ms) before giving up on trying to write to Redis.

#### Contributing

Pull requests into the master branch are welcomed.

#### License

Copyright © 2015 Gardner Vickers

Distributed under the Eclipse Public License, the same as Clojure.
