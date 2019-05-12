(defproject org.onyxplatform/onyx-redis "0.14.5.1-SNAPSHOT"
  :description "Onyx plugin for redis"
  :url "https://github.com/onyx-platform/onyx-redis"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories {"snapshots" {:url "https://clojars.org/repo"
                              :username :env
                              :password :env
                              :sign-releases false}
                 "releases" {:url "https://clojars.org/repo"
                             :username :env
                             :password :env
                             :sign-releases false}}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/data.json "0.2.6"]
                 ^{:voom {:repo "git@github.com:onyx-platform/onyx.git" :branch "master"}}
                 [org.onyxplatform/onyx "0.14.5"]
                 [com.taoensso/carmine "2.15.1" :exclusions [com.taoensso/timbre com.taoensso/encore]]]
  :profiles {:dev {:dependencies [[aero "0.2.0"]]
                   :plugins [[lein-set-version "0.4.1"]
                             [lein-update-dependency "0.1.2"]
                             [lein-pprint "1.1.1"]]
                   :resource-paths ["test-resources/"]}
             :circle-ci {:jvm-opts ["-Xmx4g"]}}
  :jvm-opts ["-XX:-OmitStackTraceInFastThrow"])
