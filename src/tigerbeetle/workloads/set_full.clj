(ns tigerbeetle.workloads.set-full
  "Model TigerBeetle account creation/lookup as a grow only set.
   Use [[jepsen.checker/set-full]] with `:linearizable? true`.
   
   The set key is the ledger.
   The set elements are accounts."
  (:require [clojure.set :as set]
            [clojure.tools.logging :refer [info]]
            [jepsen
             [checker :as checker]
             [client :as client]
             [generator :as gen]
             [independent :as independent]
             [util :as u]]
            [slingshot.slingshot :refer [throw+]]
            [tigerbeetle.tigerbeetle :as tb]
            [knossos.op :as op]))

; TODO: use flags.linked for linked transactions
; TODO: use flags.pending for pending transactions

(defn adds
  "A lazy sequence of account add operations.
   TigerBeetle account ids must be > 0."
  ([keys] adds keys 1)
  ([keys init-acct]
   (map (fn [v]
          (let [k (rand-nth keys)]
            {:type  :invoke,
             :f     :add,
             :value (independent/tuple k v)}))
        (drop init-acct (range)))))

(defn reads
  "A lazy sequence of account read operations.
   If `final?`, then operations include `:final? true`."
  ([keys] (reads keys false))
  ([keys final?]
   (repeatedly (fn []
                 (let [k (rand-nth keys)]
                   (merge
                    {:type  :invoke,
                     :f     :read,
                     :value (independent/tuple k nil)}
                    (when final? {:final? true})))))))

(def attempted-adds
  "All accounts, by ledger, where an attempt was made to add it to TigerBeetle."
  (atom {}))

(defn read-all-invoked-adds
  "Did final reads read all invoked add values?"
  []
  (reify checker/Checker
    (check [_this _test history _opts]
      (let [all-invoked-adds (->> history
                                  (filter (comp #{:add} :f))
                                  (filter op/invoke?)
                                  (reduce (fn [acc {:keys [value] :as _op}]
                                            (conj acc value))
                                          #{}))
            final-reads (->> history
                             (filter (comp #{:read} :f))
                             (filter op/ok?)
                             (filter :final?))
            suspect-final-reads (->> final-reads
                                     (filter (fn [{:keys [value] :as _op}]
                                               (seq (set/difference all-invoked-adds value))))
                                     (map (fn [{:keys [index value] :as _op}]
                                            [index (set/difference all-invoked-adds value)])))]
        (merge
         {:valid? true}
         (when (seq suspect-final-reads)
           {:valid? false
            :suspect-final-reads suspect-final-reads}))))))

(defrecord SetClient [conn]
  client/Client
  (open! [this _test node]
    (let [[client-num concurrency-num client] (tb/get-tb-client)]
      (info "SetClient/open: " [client-num concurrency-num] " (" node ")")
      (assoc this
             :node node
             :conn client
             :client-num client-num
             :concurrency-num concurrency-num)))

  (setup! [_this _test]
    ; no-op
    )

  (invoke! [{:keys [conn client-num concurrency-num node] :as _this}
            _test
            {:keys [f value] :as op}]
    (let [op (assoc op
                    :node   node
                    :client [client-num concurrency-num])
          [ledger id] value]
      (case f
        :add (let [_ (swap! attempted-adds update ledger (fn [x] (if x
                                                                   (conj x id)
                                                                   #{id})))
                   value   [[:a id {:ledger ledger}]]
                   results (u/timeout tb/tb-timeout :timeout
                                      (tb/create-accounts conn value))]
               (cond
                 (= results :timeout)
                 (assoc op
                        :type  :info
                        :error :timeout)

                 :else
                 (let [[_:a id {:keys [ledger]}] (first results)]
                   (assoc op
                          :type :ok
                          :value (independent/tuple ledger id)))))

        :read (let [results (u/timeout tb/tb-timeout :timeout
                                       (tb/lookup-accounts conn (->> (get @attempted-adds ledger)
                                                                     (map (fn [id] [:r id {:ledger ledger}])))))]
                (cond
                  (= :timeout results)
                  (assoc op
                         :type  :info
                         :error :timeout)

                  :else
                  (let [results (->> results
                                     (reduce (fn [acc [_:r id _amounts]]
                                               (conj acc id))
                                             (sorted-set)))]
                    (assoc op
                           :type  :ok
                           :value (independent/tuple ledger results))))))))

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
   for a set-full test, given options from the CLI test constructor."
  [{:keys [keys nodes accounts rate] :as _opts}]
  (let [keys (or keys (drop 1 (range (+ 1 (count nodes)))))]
    {:keys            keys
     :total-amount    0
     :client          (SetClient. nil)
     :checker         (independent/checker
                       (checker/compose
                        {:set-full              (checker/set-full {:linearizable? true})
                         :read-all-invoked-adds (read-all-invoked-adds)}))
     :generator       (->> (gen/mix [(adds keys (->> accounts count (+ 1))) (reads keys)])
                           (gen/stagger (/ rate)))
     :final-generator (gen/phases
                       (gen/log "Quiesce...")
                       (gen/sleep 5)
                       (gen/log "Final reads...")
                       (->> keys
                            (map (fn [k]
                                   (gen/once (reads [k] true))))
                            (gen/each-thread)
                            (gen/clients)
                            (gen/stagger (/ rate))))}))
