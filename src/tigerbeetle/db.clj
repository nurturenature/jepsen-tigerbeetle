(ns tigerbeetle.db
  (:require [clojure.string :refer [join]]
            [clojure.tools.logging :refer [warn]]
            [jepsen
             [db :as db]
             [control :as c]]
            [jepsen.control
             [net :as cn]
             [util :as cu]]
            [jepsen.os.debian :as deb]))

(def root     "/root")
(def tb-dir   (str root "/tigerbeetle"))
(def tb-bin   (str tb-dir "/tigerbeetle"))
(def pid-path (str tb-dir "/tigerbeetle.pid"))
(def log-file "tigerbeetle.log")
(def log-path (str tb-dir "/" log-file))

(def tb-cluster
  "TigerBeetle Cluster number."
  0)

(def tb-port
  "TigerBeetle port."
  3000)

(defn tb-replica
  "Return the TigerBeetle replica number for the node."
  [node]
  (- (->> node
          (re-find #"\d")
          read-string)
     1))

(defn tb-data
  "Return name of the TigerBeetle data file for the node."
  [node]
  (str tb-cluster "_" (tb-replica node) ".tigerbeetle"))

(defn tb-addresses
  "Return a comma separated string of all TigerBeetle nodes, ip:port, in the cluster."
  [nodes]
  (->> nodes
       sort
       (map (fn [node]
              (str (cn/ip node) ":" tb-port)))
       (join ",")))

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
             (c/exec tb-bin
                     :format
                     (str "--cluster=" tb-cluster)
                     (str "--replica=" (tb-replica node))
                     (tb-data node))))

      ; start TigerBeetle
      (db/start! this test node)

      ; TODO: sleep needed?
      (Thread/sleep 1000))

    (teardown! [this test node]
      (db/kill! this test node)

      ; rm TigerBeetle data, log files
      (c/su
       (c/exec :rm :-rf (str tb-dir "/" (tb-data node)))
       (c/exec :rm :-rf log-path))

      (warn "Leaving TigerBeetle source, build, at: " tb-dir))

    ; TigerBeetle doesn't have "primaries".
    ; We'll use them to mean "leader'."
    ;; db/Primary
    ;; ; TODO
    ;; (primaries [_db test]
    ;;   (:nodes test))

    ;; (setup-primary! [_db _test _node]
    ;;   ; TODO: add accounts
    ;;   )

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
            (str "--addresses=" (tb-addresses nodes))
            (tb-data node)))
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
