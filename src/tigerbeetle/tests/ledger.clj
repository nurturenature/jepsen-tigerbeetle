(ns tigerbeetle.tests.ledger
  "A strict serializable ledger."
  (:require [clojure.pprint :refer [pprint]]
            [clojure.tools.logging :refer [info warn]]
            [jepsen
             [generator :as gen]
             [util :as u]]
            [jepsen.tests.cycle :as cycle]))

(defn transfers
  "A lazy sequence of transfer operations."
  [{:keys [accounts max-transfer] :as _opts}]
  (->> (repeatedly (fn []
                     {:type  :invoke,
                      :f     :transfer,
                      :value {:debit-acct  (rand-nth accounts)
                              :credit-acct (rand-nth accounts)
                              :amount      (u/rand-distribution {:min 1 :max max-transfer})}}))
       (remove #(= (:debit-acct %) (:credit-acct %)))))

(defn reads
  "A lazy sequence of account read operations.
   If `:final?`, then operations include `:final? true`."
  [{:keys [accounts final?] :as _opts}]
  (let [read (merge
              {:type  :invoke,
               :f     :read,
               :value accounts}
              (when final? {:final? true}))]
    (repeat read)))


(defn generator
  "Generates operations for a ledger.
  Designed to be checked wth [[checker]]."
  [{:keys [_accounts _max-transfer] :as opts}]
  (gen/mix [(reads opts)
            (transfers opts)]))

(defn final-generator
  "Final generator operations for a ledger.
  Designed to be checked wth [[checker]]."
  [{:keys [_accounts _max-transfer] :as opts}]
  (gen/phases
   (gen/log "No quiesce...")
   (gen/log "Final reads...")
   (->>
    (gen/once (reads (merge {:final? true} opts)))
    (gen/each-thread)
    (gen/clients))))

(defn  checker
  ":TODO"
  [_opts]
  (cycle/checker :TODO))

(defn workload
  "Workload for an assumed strict serializable ledger."
  [opts]
  {:generator (generator opts)
   :final-generator (final-generator opts)
   :checker (checker opts)})
