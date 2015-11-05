## onyx-redis

Onyx plugin for redis.

#### Installation

In your project file:

```clojure
[org.onyxplatform/onyx-redis "0.8.0-SNAPSHOT"]
```

In your peer boot-up namespace:

```clojure
(:require [onyx.plugin.redis])
```

#### Functions

##### Write to Redis

Example:

```clojure
{:onyx/name :out-to-redis
 :onyx/plugin :onyx.plugin.redis/writer
 :onyx/type :output
 :onyx/medium :redis
 :redis/host "127.0.0.1"
 :redis/port 6379
 :onyx/batch-size batch-size}
```

###### Attributes

|key                           | type                 | description
|------------------------------|----------------------|------------
|`:redis/host`                 | `string`             | Redis hostname
|`:redis/port`                 | `int`                | Redis port
|`:redis/read-timeout-ms`      | `int`                | Time to wait (in ms) before giving up on trying to write to Redis.

Segments should be supplied to the plugin in the form:
```clojure
{:op :sadd :args [redis-key redis-value]}
```

Where op is one of:
```
:sadd, :zadd, :lpush, :set
```
These correspond to their equivalent calls in carmine, see [documentation] (http://ptaoussanis.github.io/carmine/).

##### Inject Connection Spec lifecycle

Injects an carmine connection spec into the event map. Will also inject as an :onyx/fn param if :onyx/param? is true.

```clojure
{:lifecycle/task :use-redis-task
 :lifecycle/calls :onyx.plugin.redis/reader-conn-spec
 :redis/host redis-hostname
 :redis/port redis-port
 :redis/read-timeout-ms <<optional-timeout>>
 :onyx/param? true
 :lifecycle/doc "Initialises redis conn spec into event map, or as a :onyx.core/param"}
```

When used with task :use-redis-task and with :onyx/param? true, you can use it from your function e.g.

```clojure
(ns your.ns
  (:require [taoensso.carmine :as car :refer (wcar)]))

(defn use-redis-task-fn [conn segment]
  (wcar conn
        [(car/smembers (:key segment)])))
```

Alternatively, the conn that is created is placed under `:redis/conn` in the
event map, for use within lifecycles e.g. :before-batch. Use in this way allows
requests to be batched.

Please see the Carmine documentation for examples for how to use Carmine.

##### Consume from Key Input Plugin

Consumes from a redis set or list. 

**NOTE** this plugin does not currently support any state recovery / check
pointing. If a peer crashes any pending segments will be lost i.e. those that have been read
from the set or list and are not fully acked.

Catalog entry:

```clojure

{:onyx/name :in-from-redis
 :onyx/plugin :onyx.plugin.redis/consumer
 :onyx/type :input
 :onyx/medium :redis
 :redis/host "127.0.0.1"
 :redis/port 6379
 :redis/key ::your-key
 :redis/read-timeout-ms <<optional-timeout>>
 :redis/op :lpop
 :onyx/batch-size batch-size
 :onyx/max-peers 1
 :onyx/doc "Reads segments via redis"}

```

Currently supported `:redis/op` consume operations are `:lpop`, `:rpop`, and `:spop`.

Lifecycle entry:

```clojure
[{:lifecycle/task :input-task
  :lifecycle/calls :onyx.plugin.redis/reader-state-calls}]
```

###### Attributes

|key                           | type                 | description
|------------------------------|----------------------|------------
|`:redis/host`                 | `string`             | Redis hostname
|`:redis/port`                 | `int`                | Redis port
|`:redis/key`                  |`keyword` or `string` | A Redis [list](http://redis.io/topics/data-types) or set containing segments
|`:redis/op`                   |`keyword`             | A Redis datastructure operation (:lpop / :rpop/ :spop)
|`:redis/read-timeout-ms`      | `int`                | Time to wait (in ms) before giving up on trying to read from Redis.

#### Contributing

Pull requests into the master branch are welcomed.

#### License

Copyright © 2015 Gardner Vickers

Distributed under the Eclipse Public License, the same as Clojure.
