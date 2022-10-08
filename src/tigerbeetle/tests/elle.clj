(ns tigerbeetle.tests.elle
  "A workspace to develop a ledger test using Elle's cycle detection."
  (:require [clojure
             [pprint :refer [pprint]]
             [set :as set]]
            [clojure.tools.logging :refer [info warn]]))

(def r0
  {:f :read
   :id              :r0
   :account-id      1
   :debits-posted   #{}
   :credits-posted  #{}})

(def r1
  {:f :read
   :id              :r1
   :account-id      1
   :debits-posted   #{0}
   :credits-posted  #{}})

(def r2
  {:f               :read
   :id              :r2
   :account-id      1
   :debits-posted   #{1 0}
   :credits-posted  #{}})

(def r3
  {:f               :read
   :id              :r3
   :account-id      1
   :debits-posted   #{2 1 0}
   :credits-posted  #{}})

(def w1
  {:f           :transfer
   :id          :w1
   :amount      #{0}
   :debit-acct  1
   :credit-acct :bank})

(def w2
  {:f           :transfer
   :id          :w2
   :amount      #{1}
   :debit-acct  1
   :credit-acct :bank})

(def w3
  {:f           :transfer
   :id          :w3
   :amount      #{2}
   :debit-acct  1
   :credit-acct :bank})

(def sample-history
  [r0 w1 r1 w2 r2 w3 r3])

(def num-bits-amt
  "The number of bits in an amount value.
   Default u64."
  ; 64
  4)

(def num-accts
  "The number of accounts.
   Default 64."
  ; 64
  4)

(def all-bits
  "A set of all of the bits in an amount value."
  (into (sorted-set) (range num-bits-amt)))

(def all-accounts
  "1..num-accts."
  (range 1 (+ 1 num-accts)))

(defn wr-compare
  "Compare a write and a read and determine:
      -1 w < r  write happened before read
       0 w = r  don't know
      +1 w > r  read happened before write"
  [w r]
  (let [{:keys [amount
                debit-acct
                credit-acct]} w
        {:keys [account-id
                debits-posted
                credits-posted]} r]
    (cond
      (not (#{debit-acct credit-acct} account-id))
      [0 :different-accounts]

      (= account-id debit-acct)
      (if (set/subset? amount debits-posted)
        [-1 :write-in-read]
        [+1 :write-not-in-read])

      (= account-id credit-acct)
      (if (set/subset? amount credits-posted)
        [-1 :write-in-read]
        [+1 :write-not-in-read]))))

(defn rr-only-in
  "Compares `r` and `r'` returning the values that were only in `r`."
  [r r']
  (->> [:debits-posted :credits-posted]
       (reduce (fn [acc k]
                 (let [diff (set/difference (get r k) (get r' k))]
                   (if (seq diff)
                     (assoc acc k diff)
                     acc)))
               {})))

(defn rr-compare
  "Compare a read and a read' and determine:
      -1 r < r'  read happened before read'
       0 r = r'  don't know, which is Ok
      +1 r > r'  read happened after read'"
  [r r']
  (let [{id  :account-id} r
        {id' :account-id} r']
    (if (not= id id')
      [0 :different-accounts]
      (let [only-r  (rr-only-in r r')
            only-r' (rr-only-in r' r)]
        (assert (not (and (seq only-r)
                          (seq only-r')))
                (str "Both reads happened before each other: only " only-r " in " r ", only " only-r' " in " r'))
        (cond
          (and (not (seq only-r))
               (not (seq only-r')))
          [0 :read-values-equal]

          (seq only-r)
          [+1 :read-value-gt-read'-value]

          (seq only-r')
          [-1 :read-value-lt-read'-value])))))

(defn ww-compare
  "Cannot say anything about order just from seeing the individual writes."
  [_w _w']
  [0 :not-enough-information])

(defn ops-map
  "Given a history, builds a map that indexes:
   ```clj
   {:read     {acct {:debits-posted  {#{amounts} #{op-ids}}
                     :credits-posted {#{amounts} #{op-ids}}}}  
    :transfer {acct {:debits-posted  {#{amounts} #{op-ids}}
                     :credits-posted {#{amounts} #{op-ids}}}}}```"
  [history]
  (->> history
       (reduce (fn [acc {:keys [f id
                                account-id debits-posted credits-posted
                                debit-acct credit-acct amount] :as _op}]
                 (cond
                   (= :transfer f)
                   (-> acc
                       (update-in [:transfer debit-acct  :debits-posted  amount]
                                  (fn [x]
                                    (if (nil? x)
                                      (sorted-set id)
                                      (conj x id))))
                       (update-in [:transfer credit-acct :credits-posted amount]
                                  (fn [x]
                                    (if (nil? x)
                                      (sorted-set id)
                                      (conj x id)))))

                   (= :read f)
                   (-> acc
                       (update-in [:read account-id :debits-posted debits-posted]
                                  (fn [x]
                                    (if (nil? x)
                                      (sorted-set id)
                                      (conj x id))))
                       (update-in [:read account-id :credits-posted credits-posted]
                                  (fn [x]
                                    (if (nil? x)
                                      (sorted-set id)
                                      (conj x id)))))

                   :else
                   (update-in acc [:error f] set/union #{id})))
               (sorted-map))))

(defn read-relations
  "Given a read operation and a history,
  will return [before-ops after-ops]."
  [{:keys [id account-id debits-posted credits-posted] :as _r} history] ; TODO: credits-posted
  (let [ops-map       (ops-map history)
        before-writes (->> debits-posted ; writes for all of my values must have happened before
                           (reduce (fn [acc amt]
                                     (let [writes (get-in ops-map [:transfer account-id :debits-posted #{amt}])]
                                       (if (seq writes)
                                         (set/union acc writes)
                                         acc)))
                                   (sorted-set)))
        before-reads (let [reads-vals (get-in ops-map [:read account-id :debits-posted])]
                       (->> reads-vals
                            (reduce (fn [acc [amts ids]]
                                      (if (seq (set/difference debits-posted amts))
                                        ; reads missing my values must have happened before
                                        (set/union acc ids)
                                        acc))
                                    (sorted-set))))
        after-writes (->> (set/difference all-bits debits-posted)
                          ; values that I am missing were written after me
                          (reduce (fn [acc amt]
                                    (let [writes (get-in ops-map [:transfer account-id :debits-posted #{amt}])]
                                      (if (seq writes)
                                        (set/union acc writes)
                                        acc)))
                                  (sorted-set)))
        after-reads  (let [reads-vals (get-in ops-map [:read account-id :debits-posted])]
                       (->> reads-vals
                            (reduce (fn [acc [amts ids]]
                                      (if (and (set/subset? debits-posted amts)
                                               (seq (set/difference amts debits-posted)))
                                        ; reads with my values and other values must have happened after
                                        (set/union acc ids)
                                        acc))
                                    (sorted-set))))]
    [[:happened-before (set/union before-writes before-reads)]
     [:read id]
     [:happened-after  (set/union after-writes  after-reads)]]))

(defn write-relations
  "Given a write operation and a history,
  will return [before-ops after-ops]."
  [{:keys [id amount debit-acct credit-acct] :as _w} history] ; TODO: credit-acct
  (let [ops-map       (ops-map history)
        before-writes nil ; cannot say anything
        before-reads  (let [reads-vals (get-in ops-map [:read debit-acct :debits-posted])]
                        (->> reads-vals
                             (reduce (fn [acc [amts ids]]
                                       (if (seq (set/difference amount amts))
                                         ; reads missing my values must have happened before
                                         (set/union acc ids)
                                         acc))
                                     (sorted-set))))
        after-writes nil ; cannot say anything
        after-reads  (let [reads-vals (get-in ops-map [:read debit-acct :debits-posted])]
                       (->> reads-vals
                            (reduce (fn [acc [amts ids]]
                                      (if (set/subset? amount amts)
                                         ; reads with my values must have happened after
                                        (set/union acc ids)
                                        acc))
                                    (sorted-set))))]
    [[:happened-before (set/union before-writes before-reads)]
     [:write id]
     [:happened-after  (set/union after-writes  after-reads)]]))

(def all-open-transfers
  "A map to seed an all transfers generator.
   ```clj
   {:debits-posted  {bit  #{accts}
                     bit' #{accts}
                     ...}
    :credits-posted {bit  #{accts}
                     bit' #{accts}
                     ...}}
   ```"
  (->> all-bits
       (reduce (fn [all-ops bit]
                 (-> all-ops
                     (assoc-in [:debits-posted  bit] (into (sorted-set) all-accounts))
                     (assoc-in [:credits-posted bit] (into (sorted-set) all-accounts))))
               {:debits-posted  (sorted-map)
                :credits-posted (sorted-map)})))

(defn all-transfers-generator
  "A random lazy sequence of all possible transfers for all accounts.
   Returns nil when exhausted."
  ([] (all-transfers-generator {:transfer nil :state all-open-transfers}))
  ([{:keys [_transfer _state] :as opts}]
   (->> (iterate
         (fn [opts]
           (loop [{:keys [_transfer state] {:keys [debits-posted credits-posted]} :state} opts]
             (cond
               ; exhausted possible transfers?
               (not (seq debits-posted))
               nil

               :else
               ; pick a debit random bit and account, look for a matching available credit
               (let [bit (->> debits-posted
                              keys
                              rand-nth)
                     debit-accts  (get debits-posted  bit)
                     credit-accts (get credits-posted bit)]
                 (assert (seq debit-accts) "state :debits-posted not being maintained correctly")
                 ; no available credits accts for this bit mean debits for bit also are not possible
                 (if (not (seq credit-accts))
                   (recur {:transfer nil
                           :state (-> state
                                      (update :debits-posted dissoc bit))})
                   ; look for possible debit/credit w/different accounts
                   (let [[debit-acct credit-acct] (->> debit-accts
                                                       seq shuffle
                                                       (reduce (fn [_ debit-acct]
                                                                 (let [credit-acct (->> (disj credit-accts debit-acct)
                                                                                        seq rand-nth)]
                                                                   ; matching credit-acct available?
                                                                   (when credit-acct
                                                                     (reduced [debit-acct credit-acct]))))
                                                               nil))]
                     ; found a match?
                     (if (and debit-acct credit-acct)
                       {:transfer {:f     :transfer
                                   :value [{:debit-acct  debit-acct
                                            :credit-acct credit-acct
                                            :amount      #{bit}}]}
                        :state (-> state
                                   (update-in [:debits-posted bit] disj debit-acct)
                                   (update :debits-posted (fn [x]
                                                            (if (seq (get x bit))
                                                              x
                                                              (dissoc x bit))))
                                   (update-in [:credits-posted bit] disj credit-acct)
                                   (update :credits-posted (fn [x]
                                                             (if (seq (get x bit))
                                                               x
                                                               (dissoc x bit)))))}
                       ; no possible match for this bit, so remove both debits and credits
                       (recur {:transfer nil
                               :state (-> state
                                          (update :debits-posted  dissoc bit)
                                          (update :credits-posted dissoc bit))}))))))))
         (assoc opts :init true))
        (drop 1)
        (map (fn [{:keys [transfer _state]}] transfer)))))
