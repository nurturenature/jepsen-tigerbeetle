(ns tigerbeetle.tests.ledger
  "An assumed strict serializable ledger test."
  (:refer-clojure :exclude [read test])
  (:require [clojure.core.reducers :as r]
            [clojure.tools.logging :refer [info]]
            [clojure.set :as set]
            [jepsen
             [checker :as checker]
             [generator :as gen] [store :as store]
             [util :as util]]
            [jepsen.checker.perf :as perf]
            [knossos
             [history :as history]
             [op :as op]]
            [gnuplot.core :as g]))

(defn op->txn-f
  "Takes an op and returns the first :f in the :txn :value."
  [{:keys [value] :as _op}]
  (let [[f _id _values] (->> value util/coll first)]
    f))

(def transfer-id
  "Most recent transfer id used."
  (atom 0))

(def reads
  "A generator of read operations.
   Reads all accounts in a single txn."
  (fn [{:keys [accounts] :as _test} _ctx]
    (let [rs (->> accounts
                  (map (fn [acct]
                         [:r acct nil])))]
      {:type  :invoke
       :f     :txn
       :value rs})))

(def lookup-transfers
  "A generator of lookup transfers operations.
   Reads all transfers in a single txn."
  (fn [_test _ctx]
    {:type  :invoke
     :f     :txn
     :value [[:l-t nil nil]]}))

(def transfers
  "Generator of transfer operations:
   a random amount between two different randomly selected accounts."
  (fn [{:keys [accounts max-transfer] :as _test} _ctx]
    (let [debit-acct  (rand-nth accounts)
          credit-acct ((fn diff-acct []
                         (let [credit-acct (rand-nth accounts)]
                           (if (not= credit-acct debit-acct)
                             credit-acct
                             (diff-acct)))))
          amount (util/rand-distribution {:min 1 :max (+ 1 max-transfer)})
          id     (swap! transfer-id + 1)]
      {:type  :invoke
       :f     :txn
       :value [[:t id {:debit-acct  debit-acct
                       :credit-acct credit-acct
                       :amount      amount}]]})))

(defn generator
  "A mixture of reads and transfers for clients."
  []
  (gen/mix [reads transfers]))

(defn final-generator
  "A generator that does a `:final? true` read,
   and a `:l-t`, lookup all attempted transfers, on each worker."
  []
  (gen/phases
   (gen/log "Quiesce...")
   (gen/sleep 5)
   (gen/log "Final reads...")
   (->> reads
        (gen/map (fn [op] (assoc op :final? true)))
        (gen/once)
        (gen/each-thread)
        (gen/clients))
   (gen/log "Final lookup all transfers...")
   (->> lookup-transfers
        (gen/map (fn [op] (assoc op :final? true)))
        (gen/once)
        (gen/each-thread)
        (gen/clients))))

(defn ledger->bank
  "Takes a history from a ledger test and maps it to bank test semantics."
  [history]
  (->> history
       (map (fn [{:keys [type value process] :as op}]
              (if (int? process)
                (let [[f _ledger _value] (->> value first)]
                  (case [type f]
                    ([:invoke :r] [:info :r] [:fail :r])
                    (assoc op :f :read)

                    [:ok :r]
                    (let [value  (->> value
                                      (reduce (fn [acc [_:r id {:keys [debits-posted credits-posted]}]]
                                                (assoc acc id (- credits-posted debits-posted)))
                                              {}))]
                      (assoc op :f :read :value value))

                    ([:invoke :t] [:ok :t] [:info :t] [:fail :t])
                    (assoc op :f :transfer)

                    ([:invoke :l-t] [:ok :l-t] [:info :l-t] [:fail :l-t])
                    nil))
                ; :process :nemesis, etc
                op)))
       (remove nil?)))

(defn err-badness
  "Takes a bank error and returns a number, depending on its type. Bigger
  numbers mean more egregious errors."
  [test err]
  (case (:type err)
    :unexpected-key (count (:unexpected err))
    :nil-balance    (count (:nils err))
    :wrong-total    (Math/abs (float (/ (- (:total err) (:total-amount test))
                                        (:total-amount test))))
    :negative-value (- (reduce + (:negative err)))))

(defn check-op
  "Takes a single op and returns errors in its balance"
  [accts total negative-balances? op]
  (let [ks       (keys (:value op))
        balances (vals (:value op))]
    (cond (not-every? accts ks)
          {:type        :unexpected-key
           :unexpected  (remove accts ks)
           :op          op}

          (some nil? balances)
          {:type    :nil-balance
           :nils    (->> (:value op)
                         (remove val)
                         (into {}))
           :op      op}

          (not= total (reduce + balances))
          {:type     :wrong-total
           :total    (reduce + balances)
           :op       op}

          (and (not negative-balances?) (some neg? balances))
          {:type     :negative-value
           :negative (filter neg? balances)
           :op       op})))

(defn checker
  "Verifies that all reads must sum to (:total test), and, unless
  :negative-balances? is true, checks that all balances are
  non-negative."
  [checker-opts]
  (reify checker/Checker
    (check [this test history opts]
      (let [history (->> history (ledger->bank))
            accts (set (:accounts test))
            total (:total-amount test)
            reads (->> history
                       (r/filter op/ok?)
                       (r/filter #(= :read (:f %))))
            errors (->> reads
                        (r/map (partial check-op
                                        accts
                                        total
                                        (:negative-balances? checker-opts)))
                        (r/filter identity)
                        (group-by :type))]
        {:valid?      (every? empty? (vals errors))
         :read-count  (count (into [] reads))
         :error-count (reduce + (map count (vals errors)))
         :first-error (util/min-by (comp :index :op) (map first (vals errors)))
         :errors      (->> errors
                           (map
                            (fn [[type errs]]
                              [type
                               (merge {:count (count errs)
                                       :first (first errs)
                                       :worst (util/max-by
                                               (partial err-badness test)
                                               errs)
                                       :last  (peek errs)}
                                      (if (= type :wrong-total)
                                        {:lowest  (util/min-by :total errs)
                                         :highest (util/max-by :total errs)}
                                        {}))]))
                           (into {}))}))))

(defn unexpected-ops
  "A checker for unexpected ops:
   - invokes that were never resolved
   - infos
   - fails

   If present, will mark the test results as unknown."
  []
  (reify checker/Checker
    (check [_this _test history _opts]
      (let [history     (->> history (remove #(not (int? (:process %)))))
            end-time    (->> history last :time)
            open-ops (history/unmatched-invokes history)
            opens    (->> open-ops
                          (map (fn [{:keys [time] :as op}]
                                 [(util/nanos->ms (- end-time time)) op]))
                          (into [])
                          rseq)
            fails (->> history (filter op/fail?))]
        (merge
         {:valid? true}
         (when (seq opens)
           {:valid? :unknown
            :open-ops opens})
         (when (seq fails)
           {:valid? :unknown
            :fail-ops fails}))))))

(defn lookup-all-invoked-transfers
  "Did final lookup transfers read all invoked transfers?"
  []
  (reify checker/Checker
    (check [_this _test history _opts]
      (let [history (->> history
                         (filter (comp int? :process)))
            all-invoked-transfers (->> history
                                       (filter (comp #{:t} op->txn-f))
                                       (filter op/invoke?)
                                       (reduce (fn [acc {:keys [value] :as _op}]
                                                 (->> value (reduce (fn [acc [_:t id _values]]
                                                                      (conj acc id))
                                                                    acc)))
                                               #{}))
            final-lookups (->> history
                               (filter (comp #{:l-t} op->txn-f))
                               (filter op/ok?)
                               (filter :final?))
            suspect-final-lookups (->> final-lookups
                                       (filter (fn [{:keys [value] :as _op}]
                                                 (let [ids (->> value
                                                                (reduce (fn [acc [_:l-t id _values]]
                                                                          (conj acc id))
                                                                        #{}))]
                                                   (seq (set/difference all-invoked-transfers ids))))))]
        (merge
         {:valid? true}
         (when (seq suspect-final-lookups)
           {:valid? false
            :suspect-final-lookups suspect-final-lookups}))))))

(defn final-reads
  "Insures that final reads
   - exist
   - are equal"
  []
  (reify checker/Checker
    (check [_this _test history _opts]
      (let [history (->> history
                         (filter (comp int? :process)))
            unique-final-reads (->> history
                                    (filter (comp #{:r} op->txn-f))
                                    (filter op/ok?)
                                    (filter :final?)
                                    (map :value)
                                    set)
            unique-final-lookups (->> history
                                      (filter (comp #{:l-t} op->txn-f))
                                      (filter op/ok?)
                                      (filter :final?)
                                      (map :value)
                                      set)]
        (merge
         {:valid? true}
         (when (not= 1 (count unique-final-reads))
           {:valid? false
            :unequal-final-reads unique-final-reads})
         (when (not= 1 (count unique-final-lookups))
           {:valid? false
            :unequal-final-lookups unique-final-lookups}))))))

(defn ok-reads
  "Filters a history to just OK reads. Returns nil if there are none."
  [history]
  (let [h (filter #(and (op/ok? %)
                        (= :read (:f %)))
                  history)]
    (when (seq h)
      (vec h))))

(defn by-node
  "Groups operations by node."
  [test history]
  (let [nodes (:nodes test)
        n     (count nodes)]
    (->> history
         (r/filter (comp number? :process))
         (group-by (fn [op]
                     (let [p (:process op)]
                       (nth nodes (mod p n))))))))

(defn points
  "Turns a history into a sequence of [time total-of-accounts] points."
  [history]
  (mapv (fn [op]
          [(util/nanos->secs (:time op))
           (reduce + (remove nil? (vals (:value op))))])
        history))

(defn plotter
  "Renders a graph of balances over time"
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [history (->> history (ledger->bank))]
        (when-let [reads (ok-reads history)]
          (let [totals (->> reads
                            (by-node test)
                            (util/map-vals points))
                colors (perf/qs->colors (keys totals))
                path (.getCanonicalPath
                      (store/path! test (:subdirectory opts) "ledger.png"))
                preamble (concat (perf/preamble path)
                                 [['set 'title (str (:name test) " ledger")]
                                  '[set ylabel "Total of all accounts"]])
                series (for [[node data] totals]
                         {:title      node
                          :with       :points
                          :pointtype  2
                          :linetype   (colors node)
                          :data       data})]
            (-> {:preamble  preamble
                 :series    series}
                (perf/with-range)
                (perf/with-nemeses history (:nemeses (:plot test)))
                perf/plot!)
            {:valid? true}))))))

(defn test
  "Assumed strict serializable ledger test:
     - can make default choices for accounts and amounts
     - generator of reads and transfers
     - final-generator for final? true reads on all workers
     - checker: bank, unexpected ops, final reads
  
   Options:
   ```
   {:negative-balances? boolean} ; if true, doesn't verify that balances remain positive
   ```"
  ([]
   (test {:negative-balances? false}))
  ([{:keys [accounts max-transfer total-amount] :as opts}]
   (let [max-transfer (or max-transfer 5)
         total-amount (or total-amount 0)
         accounts     (or accounts (vec (range 1 9)))
         opts         (assoc opts
                             :accounts     accounts
                             :max-transfer max-transfer
                             :total-amount total-amount)]
     (merge opts
            {:checker (checker/compose {:SI   (checker opts)
                                        :plot (plotter)
                                        :lookup-transfers (lookup-all-invoked-transfers)
                                        :final-reads (final-reads)
                                        :unexpected-ops (unexpected-ops)})
             :generator       (generator)
             :final-generator (final-generator)}))))
