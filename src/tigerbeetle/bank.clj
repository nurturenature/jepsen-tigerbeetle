(ns tigerbeetle.bank
  (:require [clojure.tools.logging :refer [info]]
            [jepsen
             [client :as client]
             [generator :as gen]
             [util :as u]]
            [jepsen.tests.bank :as jbank]
            [slingshot.slingshot :refer [throw+]]
            [tigerbeetle.tigerbeetle :as tb])
  (:import (com.tigerbeetle AccountBatch Client IdBatch TransferBatch UInt128)))

; TODO: use flags.linked for linked transactions
; TODO: use flags.pending for pending transactions

(defn new-tb-client
  "Create a new TigerBeetle client for the cluster of nodes.
  It is a new java Object."
  [nodes]
  (Client. tb/tb-cluster (into-array String (tb/tb-replica-addresses nodes))))

(defn close-tb-client
  "Closes a TigerBeetle client."
  [client]
  (.close client))

(defn with-tb-client
  "Creates a TigerBeetle client for the given nodes,
   passes it to `(f client & args), closes client, and returns the results."
  [nodes f & args]
  (let [client  (new-tb-client nodes)
        results (apply f client args)]
    (close-tb-client client)
    results))

(defn create-accounts
  "Takes a seq of accounts to create in TigerBeetle.
   Returns a lazy sequence of any errors `{account error}`."
  [client accounts]
  (let [batch (AccountBatch. (count accounts))
        _     (doseq [account accounts]
                (.add batch)
                (.setId     batch account 0)
                (.setCode   batch tb/tb-code)
                (.setLedger batch tb/tb-ledger))
        errors (.createAccounts client batch)]
    (repeatedly (.getLength errors)
                (fn []
                  (.next errors)
                  (let [i (.getIndex errors)
                        r (.getResult errors)
                        a (do (.setPosition batch i)
                              (.getId batch UInt128/LeastSignificant))]
                    {a r})))))

(defn lookup-accounts
  "Takes a sequence of accounts and looks them up in TigerBeetle.
   Returns a map of `{account total ...}`."
  [client accounts]
  (let [batch (IdBatch. (count accounts))
        _     (doseq [account accounts]
                (.add batch)
                (.setId batch account 0))
        results (.lookupAccounts client batch)
        results (repeatedly (.getLength results)
                            (fn []
                              (.next results)
                              (let [id  (.getId results UInt128/LeastSignificant)
                                    amt (- (.getCreditsPosted results)
                                           (.getDebitsPosted  results))]
                                [id amt])))]
    (->> results
         (reduce (fn [acc [id amt]]
                   (assoc acc id amt))
                 {}))))

(defn create-transfers
  "Takes a sequence of transfers and creates them in TigerBeetle.
   Returns a lazy sequence of any errors `{:id :error :from :to :amount}`."
  [client transfers]
  (let [batch (TransferBatch. (count transfers))
        _     (doseq [{:keys [from to amount] :as _transfer} transfers]
                (.add batch)
                ; TODO: create linked transfers
                (.setId              batch (u/rand-distribution {:min 1, :max 1e+8}) 0)
                (.setCreditAccountId batch to 0)
                (.setDebitAccountId  batch from 0)
                (.setCode            batch tb/tb-code)
                (.setAmount          batch amount)
                (.setLedger          batch tb/tb-ledger))
        errors (.createTransfers client batch)]
    (repeatedly (.getLength errors)
                (fn []
                  (.next errors)
                  (let [i (.getIndex  errors)
                        r (.getResult errors)]
                    (.setPosition batch i)
                    {:id     (.getId              batch UInt128/LeastSignificant)
                     :error  r
                     :from   (.getDebitAccountId  batch UInt128/LeastSignificant)
                     :to     (.getCreditAccountId batch UInt128/LeastSignificant)
                     :amount (.getAmount          batch)})))))


(defrecord BankClient [conn]
  client/Client
  (open! [this {:keys [nodes] :as _test} node]
    (info "BankClient/open (" node "): " (tb/tb-replica-addresses nodes))
    (let [conn  (u/timeout tb/tb-timeout :timeout
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
        :transfer (let [errors (u/timeout tb/tb-timeout :timeout
                                          (create-transfers conn [value]))]
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
                                       (lookup-accounts conn accounts))]
                (cond
                  (= :timeout results)
                  ; TODO
                  (throw+ [:read accounts :error :timeout])
                  ;; (assoc op
                  ;;        :type :info
                  ;;        :value :timeout)

                  :else
                  (assoc op
                         :type :ok
                         :value results))))))

  (teardown! [_this _test]
    ; no-op
    )

  (close! [{:keys [conn] :as _this} _test]
    (close-tb-client conn)))

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
