(ns tigerbeetle.workloads.bank
  (:require [clojure.tools.logging :refer [info]]
            [jepsen
             [client :as client]
             [generator :as gen]
             [util :as u]]
            [jepsen.tests.bank :as jbank]
            [slingshot.slingshot :refer [throw+]]
            [tigerbeetle.tigerbeetle :as tb]))

; TODO: use flags.linked for linked transactions
; TODO: use flags.pending for pending transactions

(defrecord BankClient [conn]
  client/Client
  (open! [this {:keys [nodes] :as _test} node]
    (info "BankClient/open (" node "): " (tb/tb-replica-addresses nodes))
    (let [conn  (u/timeout tb/tb-timeout :timeout
                           (tb/new-tb-client nodes))]
      (if (= :timeout conn)
        (throw+ [:client-open node :error :timeout])
        (assoc this
               :conn conn
               :node node))))

  (setup! [_this _test]
    ; no-op
    )

  (invoke! [{:keys [conn] :as _this} {:keys [accounts] :as _test} {:keys [f value] :as op}]
    (let [op (assoc op :node (:node conn))]
      (case f
        :transfer (let [errors (u/timeout tb/tb-timeout :timeout
                                          (tb/create-transfers conn [value]))]
                    (cond
                      (= errors :timeout)
                      ; TODO
                      (throw+ [:transfer value :error :timeout])
                      ;; (assoc op
                      ;;        :type  :info
                      ;;        :error :timeout)

                      (seq errors)
                      ; TODO: info or errors?
                      (assoc op
                             :type  :info
                             :error errors)

                      :else
                      (assoc op :type :ok)))

        :read (let [results (u/timeout tb/tb-timeout :timeout
                                       (tb/lookup-accounts conn accounts))]
                (cond
                  (= :timeout results)
                  ; TODO
                  (throw+ [:read accounts :error :timeout])
                  ;; (assoc op
                  ;;        :type :info
                  ;;        :value :timeout)

                  :else
                  (let [results (->> results
                                     (reduce (fn [acc {:keys [id credits-posted debits-posted]}]
                                               (assoc acc id (- credits-posted debits-posted)))
                                             {}))]
                    (assoc op
                           :type :ok
                           :value results)))))))

  (teardown! [_this _test]
    ; no-op
    )

  (close! [{:keys [conn] :as _this} _test]
    (tb/close-tb-client conn)))

(defn workload
  "Constructs a workload:
   ```clj
   {:client, :generator, :final-generator, :checker}
   ```
   for a bank test, given options from the CLI test constructor."
  [{:keys [accounts] :as opts}]
  (let [; TigerBeetle accounts cannot start at 0
        accounts     (or accounts (vec (range 1 9)))
        total-amount 0
        bank-test    (jbank/test opts)]
    (merge
     bank-test
     ; checker      from test
     ; generator    from test
     ; max-transfer from test
     {:accounts     accounts
      :total-amount total-amount
      :client       (BankClient. nil)
      :final-generator (gen/phases
                        (gen/log "No quiesce...")
                        (gen/log "Final reads...")
                        (->> jbank/read
                             (gen/once)
                             (gen/each-thread)
                             (gen/clients)))})))
