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
:redis/conn {:pool {} :spec {:host "localhost"}}
:redis/keystore :my-key-store
:onyx/doc "Reads segments from redis"}
```

Lifecycle entry:

```clojure
[{:lifecycle/task :input-task
:lifecycle/calls :onyx.plugin.redis/lifecycle-calls}]
```

#### Attributes

|key                           | type      | description
|------------------------------|-----------|------------
|`:redis/conn`                 | `map`     | This is a [Carmine](https://github.com/ptaoussanis/carmine) connection map of the form `{:pool {<opts>} :spec {<opts>}}`
|`:redis/keystore`             |`keyword` or `string` | A Redis [list](http://redis.io/topics/data-types) containing each the keys of each set the plugin is concerned with

#### Contributing

Pull requests into the master branch are welcomed.

#### License

Copyright © 2015 FIX ME

Distributed under the Eclipse Public License, the same as Clojure.
