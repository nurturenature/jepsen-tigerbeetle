(ns tigerbeetle.workloads.ledger
  (:require [clojure.tools.logging :refer [info]]
            [jepsen
             [client :as client]
             [generator :as gen]
             [util :as u]]
            [slingshot.slingshot :refer [throw+]]
            [tigerbeetle.tigerbeetle :as tb]
            [tigerbeetle.tests.ledger :as ledger]))

; TODO: use flags.linked for linked transactions
; TODO: use flags.pending for pending transactions

(def attempted-transfers
  "All attempted transfer txn ids."
  (atom #{}))

(defrecord LedgerClient [conn]
  client/Client
  (open! [this _test node]
    (let [[client-num concurrency-num client] (tb/get-tb-client)]
      (info "LedgerClient/open: " [client-num concurrency-num] " (" node ")")
      (assoc this
             :node node
             :conn client
             :client-num client-num
             :concurrency-num concurrency-num)))

  (setup! [_this _test]
    ; no-op
    )

  (invoke! [{:keys [conn client-num concurrency-num node] :as _this} _test {:keys [f value] :as op}]
    (assert (= :txn f))
    (let [op (assoc op
                    :node   node
                    :client [client-num concurrency-num])
          [f _k _v] (first value)]
      (case f
        :t (let [_       (doseq [[_:t id _values] value]
                           (swap! attempted-transfers conj id))
                 results (u/timeout tb/tb-timeout :timeout
                                    (tb/create-transfers conn value))]
             (cond
               (= results :timeout)
               (assoc op
                      :type  :info
                      :error :timeout)

               :else
               (assoc op :type :ok :value value)))

        :r (let [results (u/timeout tb/tb-timeout :timeout
                                    (tb/lookup-accounts conn value))]
             (cond
               (= :timeout results)
               (assoc op
                      :type  :info
                      :error :timeout)

               :else
               (assoc op
                      :type  :ok
                      :value results)))

        :l-t (let [lookups (->> @attempted-transfers (map (fn [id] [:l-t id nil])))
                   results (u/timeout tb/tb-timeout :timeout
                                      (tb/lookup-transfers conn lookups))]
               (cond
                 (= :timeout results)
                 (assoc op
                        :type  :info
                        :error :timeout)

                 :else
                 (assoc op
                        :type  :ok
                        :value results))))))

  (teardown! [_this _test]
    ; no-op
    )

  (close! [{:keys [conn client-num concurrency-num] :as this} _test]
    (tb/put-tb-client [client-num concurrency-num conn])
    (dissoc this :node :conn :client-num :concurrency-num)))

(defn workload
  "Constructs a workload:
   ```clj
   {:client, :generator, :final-generator, :checker}
   ```
   for a ledger test, given options from the CLI test constructor."
  [{:keys [rate-cycle?] :as opts}]
  (let [{:keys [generator final-generator rate nemesis-interval] :as ledger-test} (ledger/test opts)
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
     ledger-test
     {:client          (LedgerClient. nil)
      :generator       generator
      :final-generator final-generator})))
