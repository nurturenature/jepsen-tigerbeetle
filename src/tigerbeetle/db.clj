(ns tigerbeetle.db
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen
             [db :as db]
             [control :as c]]
            [jepsen.control
             [util :as cu]]
            [jepsen.os.debian :as deb]
            [tigerbeetle
             [tigerbeetle :as tb]]))

(def root     "/root")
(def tb-dir   (str root   "/tigerbeetle"))
(def data-dir (str tb-dir "/data"))
(def tb-bin   (str tb-dir "/tigerbeetle"))
(def pid-path (str tb-dir "/tigerbeetle.pid"))
(def log-file "tigerbeetle.log")
(def log-path (str tb-dir "/" log-file))
(def tb-git   "https://github.com/tigerbeetledb/tigerbeetle.git")

(defn db
  "TigerBeetle."
  [_version]
  (reify db/DB
    (setup! [this {:keys [update-tigerbeetle? tigerbeetle-debug?] :as test} node]
      (when (or (not (cu/file? tb-bin))
                update-tigerbeetle?)
       ; pull or clone from github, run build script
        (deb/install [:git] [:--assume-yes])
        (c/su
         (if (cu/exists? (str tb-dir "/.git"))
           (do (info "Updating existing " tb-dir " with git pull")
               (c/cd tb-dir
                     (c/exec :git :pull :--no-rebase)))
           (do (info "Cloning " tb-git " with git clone")
               (c/cd root
                     (c/exec :rm :-f "tigerbeetle")
                     (c/exec :git :clone tb-git))))
         (info "Building TigerBeetle from source at " tb-dir " " (if tigerbeetle-debug? :--debug ""))
         (c/cd tb-dir
               ; all changes are isolated to tb-dir
               (c/exec "scripts/install.sh" (if tigerbeetle-debug? :--debug "")))))

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
      (db/kill! this test node)

      ; rm TigerBeetle data, log files
      (c/su
       (c/cd tb-dir
             (c/exec :rm :-rf data-dir)
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
    (setup-primary! [_db {:keys [nodes accounts] :as _test} _node]
      (info "Creating accounts: " accounts)
      (tb/with-tb-client nodes tb/create-accounts accounts))

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
       (cu/grepkill! :tigerbeetle)))

    db/Pause
    (pause! [_this _test _node]
      (c/su
       (cu/grepkill! :stop :tigerbeetle)))

    (resume! [_this _test _node]
      (c/su
       (cu/grepkill! :cont :tigerbeetle)))))
