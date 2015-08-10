## onyx-redis

Onyx plugin for redis.

#### Installation

In your project file:

```clojure
[onyx-redis "0.6.0"]
```

In your peer boot-up namespace:

```clojure
(:require [onyx.plugin.redis])
```

#### Functions

##### sample-entry

Catalog entry:

```clojure
{:onyx/name :entry-name
:onyx/ident :redis/task
:onyx/type :input
:onyx/medium :redis
:onyx/batch-size batch-size
:redis/host "192.168.99.100"
:redis/port 6379
:redis/keystore :my-key-store
:onyx/doc "Reads segments from redis"}
```

Lifecycle entry:

```clojure
[{:lifecycle/task :input-task
:lifecycle/calls :onyx.plugin.redis/lifecycle-calls}]
```

#### Attributes

|key                           | type                 | description
|------------------------------|----------------------|------------
|`:redis/host`                 | `string`             | Redis hostname
|`:redis/port`                 | `int`                | Redis port
|`:redis/keystore`             |`keyword` or `string` | A Redis [list](http://redis.io/topics/data-types) containing each the keys of each set the plugin is concerned with
|`:redis/step-size`            |`int`                 | The step granularity to batch requests to Redis. defaults to 10
|`:redis/read-timeout-ms`      |`int`                 | Time to wait (in ms) before giving up on trying to read from Redis.

#### Contributing

Pull requests into the master branch are welcomed.

#### License

Copyright © 2015 Gardner Vickers

Distributed under the Eclipse Public License, the same as Clojure.
