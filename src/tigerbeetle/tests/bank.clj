(ns tigerbeetle.tests.bank
  "Helper functions for doing bank tests, where you simulate transfers between
  accounts, and verify that reads always show the same balance. The test map
  should have these additional options:

  :accounts     A collection of account identifiers.
  :total-amount Total amount to allocate.
  :max-transfer The largest transfer we'll try to execute."
  (:refer-clojure :exclude [read test])
  (:require [knossos.op :as op]
            [clojure.core.reducers :as r]
            [jepsen
             [checker :as checker]
             [generator :as gen] [store :as store]
             [util :as util]]
            [jepsen.checker.perf :as perf]
            [knossos.history :as history]
            [gnuplot.core :as g]))

(def reads
  "A generator of read operations."
  (repeat {:type :invoke
           :f    :read}))

(def transfers
  "Generator of transfer operations:
   a random amount between two different randomly selected accounts."
  (fn [{:keys [accounts max-transfer] :as _test} _ctx]
    (let [from   (rand-nth accounts)
          to     ((fn find-to []
                    (let [to (rand-nth accounts)]
                      (if (not= to from)
                        to
                        (find-to)))))
          amount (util/rand-distribution {:min 1 :max (+ 1 max-transfer)})
          id     (util/rand-distribution {:min 1})]
      {:type  :invoke
       :f     :transfer
       :value {:id     id
               :from   from
               :to     to
               :amount amount}})))

(defn generator
  "A mixture of reads and transfers for clients."
  []
  (gen/mix [reads transfers]))

(defn final-generator
  "A generator that does a `:final? true` read on each worker."
  []
  (gen/phases
   (gen/log "No quiesce...")
   (gen/log "Final reads...")
   (->> reads
        (gen/map (fn [op] (assoc op :final? true)))
        (gen/once)
        (gen/each-thread)
        (gen/clients))))

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
      (let [accts (set (:accounts test))
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
      (let [pair-index+ (->> history history/pair-index+)
            end-time    (->> history last :time)
            open-ops (history/unmatched-invokes history)
            opens    (->> open-ops
                          (map (fn [{:keys [time] :as op}]
                                 [(util/nanos->ms (- end-time time)) op]))
                          (into [])
                          rseq)
            info-ops (->> history (filter op/info?))
            infos    (->> info-ops
                          (map (fn [op]
                                 (let [start (:time (history/invocation pair-index+ op))
                                       end   (:time op)]
                                   [(util/nanos->ms (- end start)) op])))
                          (into [])
                          rseq)
            fails (->> history (filter op/fail?))]
        (merge
         {:valid? true}
         (when (seq opens)
           {:valid? :unknown
            :open-ops opens})
         (when (seq infos)
           {:valid? :unknown
            :info-ops infos})
         (when (seq fails)
           {:valid? :unknown
            :fail-ops fails}))))))

(defn final-reads
  "Insures all final reads are equal."
  []
  (reify checker/Checker
    (check [_this _test history _opts]
      (let [unique-finals (->> history
                               (filter (comp #{:read} :f))
                               (filter op/ok?)
                               (filter :final?)
                               (group-by :value)
                               (map (fn [[v ops]]
                                      {:value v :reads (count ops)})))]
        (merge
         {:valid? true}
         (when (< 1 (count unique-finals))
           {:valid? false
            :open-ops unique-finals}))))))

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
      (when-let [reads (ok-reads history)]
        (let [totals (->> reads
                          (by-node test)
                          (util/map-vals points))
              colors (perf/qs->colors (keys totals))
              path (.getCanonicalPath
                    (store/path! test (:subdirectory opts) "bank.png"))
              preamble (concat (perf/preamble path)
                               [['set 'title (str (:name test) " bank")]
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
          {:valid? true})))))

(defn test
  "A bank test:
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
                                        :unexpected-ops (unexpected-ops)
                                        :final-reads (final-reads)})
             :generator       (generator)
             :final-generator (final-generator)}))))
