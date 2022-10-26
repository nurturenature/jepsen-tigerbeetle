(ns tigerbeetle.workloads.set-full
  "Model TigerBeetle account creation/lookup as a grow only set.
   Use [[jepsen.checker/set-full]] with `:linearizable? true`.
   
   The set key is the ledger.
   The set elements are accounts."
  (:require [clojure.tools.logging :refer [info]]
            [jepsen
             [checker :as checker]
             [client :as client]
             [generator :as gen]
             [independent :as independent]
             [util :as u]]
            [slingshot.slingshot :refer [throw+]]
            [tigerbeetle.tigerbeetle :as tb]))

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

(defrecord SetClient [conn]
  client/Client
  (open! [this {:keys [nodes] :as _test} node]
    (info "SetClient/open (" node "): " (tb/tb-replica-addresses nodes))
    (let [conn  ; (u/timeout tb/tb-timeout :timeout
                ;            (tb/new-tb-client nodes))
          :pool-placeholder]
      (if (= :timeout conn)
        (throw+ [:client-open node :error :timeout])
        (assoc this
               :conn conn
               :node node))))

  (setup! [_this _test]
    ; no-op
    )

  (invoke! [{:keys [conn node] :as _this}
            _test
            {:keys [f value] :as op}]
    (assert (= :pool-placeholder conn))
    (let [[idx client] (tb/rand-tb-client)
          [ledger id]  value
          op           (assoc op
                              :node   node
                              :client idx)]
      (case f
        :add (let [_       (swap! attempted-adds update ledger (fn [x] (if x
                                                                         (conj x id)
                                                                         #{id})))
                   value   [[:a id {:ledger ledger}]]
                   results (u/timeout tb/tb-timeout :timeout
                                      (tb/create-accounts client value))]
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
                                       (tb/lookup-accounts client (->> (get @attempted-adds ledger)
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

  (close! [{:keys [conn] :as _this} _test]
    (assert (= :pool-placeholder conn))
    ; no-op
    ))

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
                       (checker/set-full {:linearizable? true}))
     :generator       (->> (gen/mix [(adds keys (->> accounts count (+ 1))) (reads keys)])
                           (gen/stagger (/ rate)))
     :final-generator (gen/phases
                       (gen/log "No quiesce...")
                       (gen/log "Final reads...")
                       (->> keys
                            (map (fn [k]
                                   (gen/once (reads [k] true))))
                            (gen/each-thread)
                            (gen/clients)
                            (gen/stagger (/ rate))))}))
