(ns tigerbeetle.db
  (:require [clojure.string :refer [split-lines]]
            [clojure.tools.logging :refer [info warn]]
            [jepsen
             [db :as db]
             [control :as c]
             [util :as u]]
            [jepsen.control
             [util :as cu]]
            [jepsen.os.debian :as deb]
            [slingshot.slingshot :refer [try+]]
            [tigerbeetle.tigerbeetle :as tb]))

(def root     "/root")
(def tb-dir   (str root   "/tigerbeetle"))
(def data-dir (str tb-dir "/data"))
(def tb-bin   (str tb-dir "/tigerbeetle"))
(def pid-path (str tb-dir "/tigerbeetle.pid"))
(def log-file "tigerbeetle.log")
(def log-path (str tb-dir "/" log-file))
(def tb-git   "https://github.com/tigerbeetledb/tigerbeetle.git")
(def tb-zig-conf (str tb-dir "/src/config.zig"))

(defn db
  "TigerBeetle."
  [_version]
  (reify db/DB
    (setup! [this {:keys [tigerbeetle-debug? tigerbeetle-log-level tigerbeetle-git] :as test} node]
      (let [actions (cond-> {}
                      (not (cu/exists? tb-dir)) (assoc :git      "head"
                                                       :install   true)
                      (not (cu/file? tb-bin))   (assoc :install   true)
                      tigerbeetle-debug?        (assoc :debug     true
                                                       :install   true)
                      tigerbeetle-log-level     (assoc :log-level tigerbeetle-log-level
                                                       :install   true)
                      tigerbeetle-git           (assoc :git       tigerbeetle-git
                                                       :install   true))]
        (when (seq actions)
          (info "Installing TigerBeetle: " actions)
          (deb/install [:git] [:--assume-yes])
          (c/su
           (c/exec :mkdir :-p tb-dir)
           (c/cd tb-dir
                 (when (not (cu/exists? :.git))
                   (c/exec :git :clone tb-git))
                 (when (:git actions)
                   (c/exec :git :reset :--hard :main)
                   (c/exec :git :switch :--detach (:git actions)))
                 (when (:log-level actions)
                   (c/exec :sed :-i
                           (str "s/^pub const log_level = .*/pub const log_level = " (:log-level actions) ";/")
                           tb-zig-conf))
                 (when (:install actions) (c/exec "scripts/install.sh" (if tigerbeetle-debug? :--debug "")))
                 (info "log-level: " (c/exec :grep "pub const log_level" tb-zig-conf))
                 (info "tigerBeetle debug / bin: " (boolean tigerbeetle-debug?) " / " (c/exec :ls :-l tb-bin))))))
      (c/cd tb-dir
            (info "git commit: " (c/exec :git :show :--oneline :-s)))

      ; create the TigerBeetle data file
      (c/su
       (c/cd tb-dir
             (c/exec :rm :-rf data-dir)
             (c/exec :mkdir :-p data-dir)
             (c/exec tb-bin
                     :format
                     (str "--cluster=" tb/tb-cluster)
                     (str "--replica=" (tb/tb-replica node))
                     (str data-dir "/" (tb/tb-data node)))))

      ; start TigerBeetle
      (db/start! this test node)

      ; TODO: sleep needed?
      (Thread/sleep 5000))

    (teardown! [this test node]
      ; TODO: no good place to do this?
      ; OK it's being done on each node
      (tb/drain-client-pool)

      (db/kill! this test node)

      ; rm TigerBeetle data, log files
      (c/su
       (c/cd tb-dir
             (c/exec :rm :-rf data-dir)
             (c/exec :rm :-rf log-path)))

      (warn "Leaving TigerBeetle source, build, at: " tb-dir))

    ; TigerBeetle doesn't have "primaries".
    ; We'll use them to mean "leader".
    ; Look in log file on each node for leader view changes.
    ; Select highest.
    db/Primary
    (primaries [_db test]
      (->> (c/on-nodes test (fn [_test node]
                              (let [[_match view] (try+
                                                   (->> (c/exec :grep :-E " view=([[:digit:]]+) leader" log-path)
                                                        (split-lines)
                                                        (last)
                                                        (re-find #" view=(\d+) leader$"))
                                                   (catch [:exit 1] {}
                                                     ; not found
                                                     [nil nil]))
                                    view          (when view (parse-long view))]
                                (info "Leader view on " node " : " view)
                                view)))
           (sort-by val)
           (last)
           (key)
           (u/coll)))

    ; TigerBeetle doesn't have "primaries".
    ; Used to initialize database by setting up accounts,
    ; creating a pool of clients
    (setup-primary! [_db {:keys [accounts] :as test} _node]
      (let [num-clients      (tb/fill-client-pool test)
            [client-num
             concurrency-num
             client]         (tb/get-tb-client)
            created-accounts (tb/create-accounts client (->> accounts
                                                             (map (fn [id] [:a id {:ledger tb/tb-ledger}]))))
            _ (tb/put-tb-client [client-num concurrency-num client])]
        (info "Created client pool of " num-clients " connections")
        (info "Created accounts " created-accounts)))

    db/LogFiles
    (log-files [_db _test _node]
      {log-path log-file})

    db/Kill
    (start! [_this {:keys [nodes] :as _test} node]
      (if (cu/daemon-running? pid-path)
        :already-running
        (do
          (c/su
           (cu/start-daemon!
            {:chdir tb-dir
             :logfile log-path
             :pidfile pid-path}
            tb-bin
            :start
            (str "--addresses=" (tb/tb-addresses nodes))
            (str data-dir "/" (tb/tb-data node))))
          :started)))

    (kill! [_this _test _node]
      (c/su
       (cu/stop-daemon! pid-path)
       (cu/grepkill! :tigerbeetle))
      :killed)

    db/Pause
    (pause! [_this _test _node]
      (c/su
       (cu/grepkill! :stop :tigerbeetle))
      :paused)

    (resume! [_this _test _node]
      (c/su
       (cu/grepkill! :cont :tigerbeetle))
      :resumed)))
