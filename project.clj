(defproject org.onyxplatform/onyx-redis "0.9.7.0-SNAPSHOT"
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
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/data.json "0.2.6"]
                 ^{:voom {:repo "git@github.com:onyx-platform/onyx.git" :branch "master"}}
                 [org.onyxplatform/onyx "0.9.7-beta2"]
                 [com.taoensso/carmine "2.12.2" :exclusions [com.taoensso/timbre com.taoensso/encore]]
                 [aero "0.2.0"]]
  :profiles {:dev {:dependencies []
                   :plugins [[lein-set-version "0.4.1"]
                             [lein-update-dependency "0.1.2"]
                             [lein-pprint "1.1.1"]]}
             :circle-ci {:jvm-opts ["-Xmx4g"]}}
  :jvm-opts ["-XX:-OmitStackTraceInFastThrow"])
