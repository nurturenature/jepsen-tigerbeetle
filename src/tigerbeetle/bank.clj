(ns tigerbeetle.bank
  (:require [clojure.tools.logging :refer [info]]
            [jepsen
             [client :as client]
             [checker :as checker]
             [generator :as gen]]
            [jepsen.tests.bank :as jbank]))

(defrecord BankClient [conn]
  client/Client
  (open! [this _test node]
    (info "BankClient/open")
    (assoc this
           :conn :TODO
           :node node))

  (setup! [_this _test])

  (invoke! [_this _test {:keys [f value] :as op}]
    (let [op (assoc op :node (:node conn))]
      (case f
        :transfer (do (info :transfer " : " value)
                      (assoc op :type :ok))

        :read (do (info :read " : " value)
                  (assoc op :type :ok)))))

  (teardown! [_this _test])

  (close! [_this _test]
    :TODO))

(defn workload
  "Constructs a workload:
   ```clj
   {:client, :generator, :final-generator, :checker}
   ```
   for a bank test, given options from the CLI test constructor."
  [opts]
  (merge (jbank/test opts)
         {;generator    from test
          ;checker      from test
          ;max-transfer from test
          ;total-amount from test
          ;accounts     from test  
          ; TODO: use test checker
          :checker (checker/unbridled-optimism)
          :client    (BankClient. nil)
          :final-generator (gen/phases
                            (gen/log "No quiesce...")
                            (gen/log "Final reads...")
                            (->> jbank/read
                                 (gen/once)
                                 (gen/each-thread)
                                 (gen/clients)))}))
