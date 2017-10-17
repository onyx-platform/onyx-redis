## onyx-redis

Onyx plugin for redis.

#### Installation

In your project file:

```clojure
[org.onyxplatform/onyx-redis "0.11.1.1-SNAPSHOT"]
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
 :redis/uri "redis://127.0.0.1:6379"
 :redis/allowed-commands [:set :get]
 :onyx/batch-size batch-size}
```

###### Attributes

|key                           | type                  | description
|------------------------------|-----------------------|------------
|`:redis/uri`                  | `string`              | Redis uri
|`:redis/read-timeout-ms`      | `int`                 | Time to wait (in ms) before giving up on trying to write to Redis.
|`:redis/allowed-commands`     | `vector` of `keyword` | Optional list of Redis commands that are allowed (default: all).

Segments should be supplied to the plugin in the form:
```clojure
{:op :sadd :args [redis-key redis-value]}
```

Where op is a keyword that corresponds 1:1 to a Redis command function
in `taoensso.carmine`, e.g. `:sadd -> taoensso.carmine/sadd`. Other
examples are `:set`, `:del`, `:pfadd`, `:zadd`, `:lpush`. For more
information on the commands supported by carmine, see
[its documentation](http://ptaoussanis.github.io/carmine/).

##### Inject Connection Spec lifecycle

Injects an carmine connection spec into the event map. Will also inject as an :onyx/fn param if :onyx/param? is true.

```clojure
{:lifecycle/task :use-redis-task
 :lifecycle/calls :onyx.plugin.redis/reader-conn-spec
 :redis/uri "redis://127.0.0.1:6379"
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

#### Contributing

Pull requests into the master branch are welcomed.

#### License

Copyright © 2016 Distributed Masonry

Distributed under the Eclipse Public License, the same as Clojure.
