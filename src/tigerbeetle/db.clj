(ns tigerbeetle.db
  (:require [clojure.tools.logging :refer [info]]
            [jepsen
             [db :as db]
             [control :as c]]
            [jepsen.control
             [scp :as scp]
             [util :as cu]]
            [slingshot.slingshot :refer [throw+]]))

(def tb-dir   :TODO)
(def pid-file :TODO)
(def log-file :TODO)

(defn db
  "TigerBeetle."
  [_version]
  (reify db/DB
    (setup! [this {:keys [:TODO] :as test} node]
      ; :TODO install from github

      (info "db/setup, node: " node)
      (comment
        (db/start! this test node)

        (Thread/sleep 10000)))

    (teardown! [this test node]
      (info "db/teardown, node: " node)
      (comment
        (db/kill! this test node)

        (c/su
         (c/exec :rm :-rf :TODO))))

    db/Primary
    ; TODO
    (primaries [_db test]
      (:nodes test))

    (setup-primary! [_db _test _node]
      ; TODO: add accounts
      )

    ;; db/LogFiles
    ;; (log-files [_db _test _node]
    ;;   {:TODO :TODO})

    ;; db/Kill
    ;; ; TODO
    ;; (start! [_this _test _node]
    ;;   (if (cu/daemon-running? pid-file)
    ;;     :already-running
    ;;     (do
    ;;       (c/su
    ;;        (cu/start-daemon!
    ;;         {:chdir tb-dir
    ;;          :env {:TODO :TODO}
    ;;          :logfile log-file
    ;;          :pidfile pid-file}
    ;;         :TODO
    ;;         :foreground))
    ;;       :restarted)))

    ;; (kill! [_this _test _node]
    ;;   (c/su
    ;;    (cu/stop-daemon! pid-file)
    ;;    (cu/grepkill! :TODO)))

    ;; db/Pause
    ;; (pause! [_this _test _node]
    ;;   (c/su
    ;;    (cu/grepkill! :stop :TODO)))

    ;; (resume! [_this _test _node]
    ;;   (c/su
    ;;    (cu/grepkill! :cont :TODO))))
    ))
