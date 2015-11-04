(defproject org.onyxplatform/onyx-redis "0.8.0-alpha1"
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
                 ^{:voom {:repo "git@github.com:onyx-platform/onyx.git" :branch "master"}}
                 [org.onyxplatform/onyx "0.8.0-alpha1"]
                 [com.taoensso/carmine "2.11.1" :exclusions [com.taoensso/timbre com.taoensso/encore]]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]]
  :profiles {:dev {:dependencies [[midje "1.7.0"]]
                   :plugins [[lein-midje "3.1.3"]
                             [lein-set-version "0.4.1"]
                             [lein-update-dependency "0.1.2"]
                             [lein-pprint "1.1.1"]]}
             :circle-ci {:jvm-opts ["-Xmx4g"]}}
  :jvm-opts ["-XX:-OmitStackTraceInFastThrow"])
