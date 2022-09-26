(ns tigerbeetle.db
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen
             [db :as db]
             [control :as c]]
            [jepsen.control
             [util :as cu]]
            [jepsen.os.debian :as deb]
            [tigerbeetle
             [bank :as bank]
             [tigerbeetle :as tb]]))

(def root     "/root")
(def tb-dir   (str root "/tigerbeetle"))
(def tb-bin   (str tb-dir "/tigerbeetle"))
(def pid-path (str tb-dir "/tigerbeetle.pid"))
(def log-file "tigerbeetle.log")
(def log-path (str tb-dir "/" log-file))

(defn db
  "TigerBeetle."
  [_version]
  (reify db/DB
    (setup! [this {:keys [update-tigerbeetle?] :as test} node]
      (when (or (not (cu/file? tb-bin))
                update-tigerbeetle?)
       ; pull or clone from github, run build script
        (deb/install [:git] [:--assume-yes])
        (c/su
         (if (cu/exists? (str tb-dir "/.git"))
           (c/cd tb-dir
                 (c/exec :git :pull :--no-rebase))
           (c/cd root
                 (c/exec :rm :-f "tigerbeetle")
                 (c/exec :git :clone "https://github.com/tigerbeetledb/tigerbeetle.git")))
         (c/cd tb-dir
               ; all changes are isolated to tb-dir
               (c/exec "scripts/install.sh"))))

      ; create the TigerBeetle data file
      (c/su
       (c/cd tb-dir
             (c/exec :rm :-rf (tb/tb-data node))
             (c/exec tb-bin
                     :format
                     (str "--cluster=" tb/tb-cluster)
                     (str "--replica=" (tb/tb-replica node))
                     (tb/tb-data node))))

      ; start TigerBeetle
      (db/start! this test node)

      ; TODO: sleep needed?
      (Thread/sleep 1000))

    (teardown! [this test node]
      (db/kill! this test node)

      ; rm TigerBeetle data, log files
      (c/su
       (c/cd tb-dir
             (c/exec :rm :-rf (tb/tb-data node))
             (c/exec :rm :-rf log-path)))

      (warn "Leaving TigerBeetle source, build, at: " tb-dir))

    db/Primary
    ; TODO
    ; TigerBeetle doesn't have "primaries".
    ; We'll use them to mean "leader'."
    (primaries [_db test]
      (:nodes test))

    ; TigerBeetle doesn't have "primaries".
    ; Used to initialize database by setting up accounts.
    (setup-primary! [_db test _node]
      (info "Creating accounts: " (:accounts test))
      (bank/create-accounts test))

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
            (tb/tb-data node)))
          :started)))

    (kill! [_this _test _node]
      (c/su
       (cu/stop-daemon! pid-path)
       (cu/grepkill! :tigerbeetle)))

    db/Pause
    (pause! [_this _test _node]
      (c/su
       (cu/grepkill! :stop :tigerbeetle)))

    (resume! [_this _test _node]
      (c/su
       (cu/grepkill! :cont :tigerbeetle)))))
