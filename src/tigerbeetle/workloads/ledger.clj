(ns tigerbeetle.workloads.ledger
  (:require [clojure.tools.logging :refer [info]]
            [jepsen
             [client :as client]
             [generator :as gen]
             [util :as u]]
            [slingshot.slingshot :refer [throw+]]
            [tigerbeetle.tigerbeetle :as tb]
            [tigerbeetle.tests.bank :as bank]))

; TODO: use flags.linked for linked transactions
; TODO: use flags.pending for pending transactions

(defrecord BankClient [conn]
  client/Client
  (open! [this {:keys [nodes] :as _test} node]
    (info "BankClient/open (" node "): " (tb/tb-replica-addresses nodes))
    (let [conn  ; (u/timeout tb/tb-timeout :timeout
                ;            (tb/new-tb-client nodes))
          :pool-placeholder]
      (if (= :timeout conn)
        (assoc this
               :conn  :no-client
               :error :timeout
               :node  node)
        (assoc this
               :conn conn
               :node node))))

  (setup! [_this _test]
    ; no-op
    )

  (invoke! [{:keys [conn node] :as _this} {:keys [accounts] :as _test} {:keys [f value] :as op}]
    (assert (= :pool-placeholder conn))
    (let [[idx conn] (tb/rand-tb-client)
          op (assoc op
                    :node node
                    :client idx)]
      (case f
        :transfer (let [errors (u/timeout tb/tb-timeout :timeout
                                          (tb/create-transfers conn [value]))]
                    (cond
                      (= errors :timeout)
                      (assoc op
                             :type  :info
                             :error :timeout)

                      ; transfer failed
                      (seq errors)
                      (assoc op
                             :type  :fail
                             :error errors)

                      :else
                      (assoc op :type :ok)))

        :read (let [results (u/timeout tb/tb-timeout :timeout
                                       (tb/lookup-accounts conn accounts))]
                (cond
                  (= :timeout results)
                  (assoc op
                         :type  :info
                         :error :timeout)

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
    (assert (= :pool-placeholder conn))
    ; no-op
    ))

(defn workload
  "Constructs a workload:
   ```clj
   {:client, :generator, :final-generator, :checker}
   ```
   for a bank test, given options from the CLI test constructor."
  [{:keys [rate-cycle?] :as opts}]
  (let [{:keys [generator final-generator rate nemesis-interval] :as bank-test} (bank/test opts)
        [generator
         final-generator] (if rate-cycle?
                            [(gen/cycle-times
                              nemesis-interval (->> generator (gen/stagger (/ (/ rate 4))))
                              nemesis-interval (->> generator (gen/stagger (/ (/ rate 2))))
                              nemesis-interval (->> generator (gen/stagger (/ (/ rate 4))))
                              nemesis-interval (->> generator (gen/stagger (/ rate))))
                             (->> final-generator (gen/stagger (/ rate)))]
                            [(->> generator       (gen/stagger (/ rate)))
                             (->> final-generator (gen/stagger (/ rate)))])]
    (merge
     bank-test
     {:client          (BankClient. nil)
      :generator       generator
      :final-generator final-generator})))
