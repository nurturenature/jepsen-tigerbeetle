(ns tigerbeetle.tests.cycle.g-counter
  "A strict serializable ledger."
  (:require [clojure.pprint :refer [pprint]]
            [clojure.tools.logging :refer [info warn]]
            [jepsen
             [checker :as checker]
             [generator :as gen]]
            [tigerbeetle.workloads.bank :as tbank]
            [jellepsen.tests.cycle.og-counter :as jellepsen]))

(defn g-counter-generator
  "Wraps a bank generator to shape :transfer ops so history :read values are monotonic."
  [bank-generator donor]
  (->> bank-generator
       (gen/map (fn [{:keys [f] :as op}]
                  (if (= :transfer f)
                    (assoc-in op [:value :from] donor)
                    op)))
       (gen/filter (fn [{:keys [f value] :as _op}]
                     (if (and (= :transfer f)
                              (= (:from value) (:to value)))
                       false
                       true)))))
(defn abs-read-values
  "Update an account's read history so value is absolute."
  [account history]
  (->> history
       (map (fn [{:keys [f value] :as op}]
              (if (and (= :read f)
                       (get value account))
                (update-in op [:value account] abs)
                op)))))

(defn read-values->mops
  "Converts a history of bank read values into mops for Elle."
  [history]
  (->> history
       (map (fn [{:keys [f value] :as op}]
              (assert (= :read f) (str "Assumed only reads! But: " f))
              (assoc op
                     :f :txn
                     :value (->> value
                                 (reduce (fn [acc [k v]]
                                           (conj acc [:r k v]))
                                         [])))))))

(defn g-counter-rr-cycle-checker
  "Takes an existing bank test history and checks it with Elle's cycle detection.
   
   History must have been generated using the [g-counter-generator]."
  [donor]
  (reify checker/Checker
    (check [this test history opts]
      (let [history (->> history
                         (filter (comp #{:read} :f))
                         (abs-read-values donor)
                         (read-values->mops))]
        (jellepsen/check this test history opts)))))

(defn workload
  "Returns a 
   ```clj
   {:generator, :final-generator, :checker}
   ```
   wrapping the original [[jepsen.tests.bank]] test to behave as a monotonic counter 
   and using Elle's cycle detection as a checker.
   
   To get monotonic values:
     - map the existing test generators to only transfer from a donor account
     
   To be analyzable by Elle, map history:
     - donor account read values to (abs value)
   "
  [{:keys [accounts] :as opts}]
  (let [donor (->> accounts sort first)
        {:keys [generator checker]
         :as bank-test} (tbank/workload opts)]
    (merge bank-test
           {:generator (g-counter-generator generator donor)
            :checker   (checker/compose {:g-counter (g-counter-rr-cycle-checker donor)
                                         :bank checker})})))

(def sample-history
  [{:type :invoke, :f :transfer, :value {:from 1, :to 8, :amount 4}, :time 2985659978, :process 0, :index 0}
   {:type :ok, :f :transfer, :value {:from 1, :to 8, :amount 4}, :time 3141238643, :process 0, :node "n1", :index 1}
   {:type :invoke, :f :read, :time 14546616606, :process 0, :index 2}
   {:type :ok, :f :read, :time 14562225443, :process 0, :node "n1", :value {1 -4, 2 0, 3 0, 4 0, 5 0, 6 0, 7 0, 8 4}, :index 3}
   {:type :invoke, :f :read, :time 40935169862, :process 0, :index 4}
   {:type :ok, :f :read, :time 40947595324, :process 0, :node "n1", :value {1 -4, 2 0, 3 0, 4 0, 5 0, 6 0, 7 0, 8 4}, :index 5}
   {:type :invoke, :f :transfer, :value {:from 1, :to 3, :amount 4}, :time 54161909529, :process 0, :index 6}
   {:type :ok, :f :transfer, :value {:from 1, :to 3, :amount 4}, :time 54211566767, :process 0, :node "n1", :index 7}
   {:type :invoke, :f :read, :time 54215825679, :process 0, :index 8}
   {:type :ok, :f :read, :time 54273457715, :process 0, :node "n1", :value {1 -8, 2 0, 3 4, 4 0, 5 0, 6 0, 7 0, 8 4}, :index 9}])
