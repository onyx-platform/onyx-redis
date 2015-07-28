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
 :onyx/doc "Reads segments from redis"}
```

Lifecycle entry:

```clojure
[{:lifecycle/task :your-task-name
  :lifecycle/calls :onyx.plugin.redis/lifecycle-calls}]
```

#### Attributes

|key                           | type      | description
|------------------------------|-----------|------------
|`:redis/attr`            | `string`  | Description here.

#### Contributing

Pull requests into the master branch are welcomed.

#### License

Copyright © 2015 FIX ME

Distributed under the Eclipse Public License, the same as Clojure.
