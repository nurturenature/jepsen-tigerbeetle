(ns tigerbeetle.bank
  (:require [clojure.tools.logging :refer [info]]
            [jepsen
             [client :as client]
             [generator :as gen]
             [util :as u]]
            [jepsen.tests.bank :as jbank]
            [slingshot.slingshot :refer [throw+]]
            [tigerbeetle.tigerbeetle :as tb])
  (:import (com.tigerbeetle Account AccountsBatch Client CreateTransferResult Transfer)
           (java.util UUID)))

(defn new-tb-client
  "Create a new TigerBeetle client for the cluster of nodes.
  It is a new java Object."
  [nodes]
  (Client. tb/tb-cluster (into-array String (tb/tb-replica-addresses nodes))))

(defn create-accounts
  "Create accounts in TigerBeetle cluster."
  [{:keys [nodes accounts] :as _test}]
  (let [client (new-tb-client nodes)
        batch  (AccountsBatch. (count accounts))
        _      (doseq [account accounts]
                 (let [account (doto (Account.)
                                 (.setId     (UUID. 0 account))
                                 (.setCode   100)
                                 (.setLedger tb/tb-ledger))]
                   (.add batch account)))
        errors (.createAccounts client batch)
        errors (areduce errors idx ret {}
                        (let [error   (aget errors idx)
                              account (get accounts (.index error))
                              msg     (.result error)]
                          (assoc ret account msg)))]
    (.close client)
    (when (seq errors)
      (throw+ [:errors errors :accounts accounts]))))

(defrecord BankClient [conn]
  client/Client
  (open! [this {:keys [nodes] :as _test} node]
    (info "BankClient/open (" node "): " (tb/tb-replica-addresses nodes))
    (let [conn  (u/timeout 100 :timeout
                           (new-tb-client nodes))]
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
        :transfer (let [{:keys [from to amount]} value
                        transfer (doto (Transfer.)
                                   (.setId              (random-uuid))
                                   (.setCreditAccountId (UUID. 0 to))
                                   (.setDebitAccountId  (UUID. 0 from))
                                   (.setCode            1)
                                   (.setAmount          amount)
                                   (.setLedger          tb/tb-ledger))
                        result (u/timeout 100 :timeout
                                          (.createTransfer conn transfer))]
                    (cond
                      (= result (CreateTransferResult/Ok))
                      (assoc op :type :ok)

                      (= result :timeout)
                      ; TODO
                      (throw+ [:transfer value :error :timeout])
                      ;; (assoc op
                      ;;        :type  :info
                      ;;        :error :timeout)

                      :else
                      (assoc op
                             :type  :info
                             :error (.toString result))))

        :read (let [batch (into-array UUID (map (fn [account]
                                                  (UUID. 0 account))
                                                accounts))
                    accounts (u/timeout 100 :timeout
                                        (.lookupAccounts conn batch))]
                (cond
                  (nil? accounts)
                  ; TODO
                  (throw+ [:read accounts :error :timeout])
                  ;; (assoc op
                  ;;        :type :info
                  ;;        :value :timeout)

                  :else
                  (let [accounts (areduce accounts idx ret {}
                                          (let [account (get accounts idx)
                                                id      (->> account
                                                             .getId
                                                             .getLeastSignificantBits)
                                                amt     (- (.getCreditsPosted account)
                                                           (.getDebitsPosted  account))]
                                            (assoc ret id amt)))]
                    (assoc op
                           :type :ok
                           :value accounts)))))))

  (teardown! [_this _test]
    ; no-op
    )

  (close! [{:keys [conn] :as _this} _test]
    (.close conn)))

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
