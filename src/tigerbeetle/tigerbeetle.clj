(ns tigerbeetle.tigerbeetle
  (:require [clojure.set :as set]
            [clojure.string :refer [join]]
            [clojure.tools.logging :refer [info warn]]
            [jepsen
             [util :as u]]
            [jepsen.control.net :as cn]
            [slingshot.slingshot :refer [try+]])
  (:import (com.tigerbeetle AccountBatch Client IdBatch TransferBatch UInt128)))

(def tb-max-num-clients
  "Maximum num of TigerBeetle clients."
  32)

(def tb-timeout
  "Timeout value in ms for Tigerbeetle transactions."
  100000)

(def tb-cluster
  "TigerBeetle Cluster number."
  0)

(def tb-port
  "TigerBeetle port."
  3000)

(defn tb-replica
  "Return the TigerBeetle replica number for the node."
  [node]
  (- (->> node
          (re-find #"\d")
          read-string)
     1))

(defn tb-data
  "Return name of the TigerBeetle data file for the node."
  [node]
  (str tb-cluster "_" (tb-replica node) ".tigerbeetle"))

(defn tb-replica-addresses
  "Return a vector of strings of all TigerBeetle nodes, ip:port, in the cluster."
  [nodes]
  (->> nodes
       sort
       (map (fn [node]
              (str (cn/ip node) ":" tb-port)))
       vec))

(defn tb-addresses
  "Return a comma separated string of all TigerBeetle nodes, ip:port, in the cluster."
  [nodes]
  (->> nodes
       tb-replica-addresses
       (join ",")))

(def tb-code
  "User defined category."
  42)

(def tb-ledger
  "TigerBeetle ledger."
  720)

(defn new-tb-client
  "Create a new TigerBeetle client for the cluster of nodes.
  Returns a new java Object or nil if client could not be created."
  [{:keys [nodes tigerbeetle-client-max-concurrency] :as _test}]
  (try+
   (if tigerbeetle-client-max-concurrency
     (Client. tb-cluster (into-array String (tb-replica-addresses nodes)) tigerbeetle-client-max-concurrency)
     (Client. tb-cluster (into-array String (tb-replica-addresses nodes))))
   (catch [] {}
     nil)))

(defn close-tb-client
  "Closes a TigerBeetle client."
  [client]
  (try+
   (.close client)
   (catch [] {})))

(def tb-client-pool
  "A set of `[client-num concurrency-num Client]` TigerBeetle clients as an Atom."
  (atom #{}))

(defn get-tb-client
  "Returns a TigerBeetle client from the pool as [client-num concurrency-num client].
   Clients are multithreaded and may be shared."
  []
  (let [[old new] (swap-vals! tb-client-pool (fn [pool]
                                               (let [client-spec (->> pool seq rand-nth)]
                                                 (disj pool client-spec))))]
    (->> (set/difference old new) seq first)))

(defn put-tb-client
  "Puts a TigerBeetle client into the pool as [client-num concurrency-num client]."
  [[client-num concurrency-num client]]
  (swap! tb-client-pool conj [client-num concurrency-num client]))

(defn fill-client-pool
  "Create TigerBeetle clients and put them in the pool.
   Returns the number of clients created."
  [{:keys [tigerbeetle-num-clients tigerbeetle-client-max-concurrency] :as test}]
  (assert (<= tigerbeetle-num-clients tb-max-num-clients))
  (dotimes [client-num tigerbeetle-num-clients]
    (let [client (new-tb-client test)]
      (when client
        (dotimes [concurrency-num tigerbeetle-client-max-concurrency]
          (put-tb-client [client-num concurrency-num client])))))
  (count @tb-client-pool))

(defn drain-client-pool
  "Closes every client and removes it from the pool."
  []
  (let [[_client-num concurrency-num client] (get-tb-client)]
    (when client
      (when (= 0 concurrency-num)
        (u/timeout 1000 :timeout (close-tb-client client)))
      (drain-client-pool))))

(defn create-accounts
  "Takes a seq of `[:a id {account values}]` of accounts to create in a TigerBeetle ledger.
   Returns a lazy sequence of created accounts."
  ([client adds]
   (when (seq adds)
     (let [batch (AccountBatch. (count adds))
           _     (doseq [[_:a id {:keys [ledger]}] adds]
                   (.add batch)
                   (.setId     batch id 0)
                   (.setCode   batch tb-code)
                   (.setLedger batch ledger))
           errors (.createAccounts client batch)
           errors (reduce (fn [acc _]
                            (.next errors)
                            (let [i  (.getIndex    errors)
                                  _  (.setPosition batch i)
                                  id (.getId batch UInt128/LeastSignificant)]
                              (conj acc id)))
                          #{}
                          (range (.getLength errors)))]
       (->> adds
            (remove (fn [[_:a id _values]]
                      (contains? errors id))))))))

(defn lookup-accounts
  "Takes a sequence of `[:r id nil]` and looks them up in TigerBeetle.
   Returns a lazy sequence of `[:r id {:credits-posted ... :debits-posted ...}]`
   for the accounts that were found."
  [client reads]
  (when (seq reads)
    (let [batch (IdBatch. (count reads))
          _     (doseq [[_:r id _nil] reads]
                  (.add batch)
                  (.setId batch id 0))
          results (.lookupAccounts client batch)]
      (repeatedly (.getLength results)
                  (fn []
                    (.next results)
                    (let [id             (.getId results UInt128/LeastSignificant)
                          credits-posted (.getCreditsPosted results)
                          debits-posted  (.getDebitsPosted  results)]
                      [:r id {:credits-posted credits-posted
                              :debits-posted  debits-posted}]))))))

(defn create-transfers
  "Takes a sequence of 
   [:t id {:debit-acct ... :credit-acct ...:amount ...}]
   and creates them in TigerBeetle.
   Returns a lazy sequence of all transfers that were created."
  [client transfers]
  (when (seq transfers)
    (let [batch (TransferBatch. (count transfers))
          _     (doseq [[_:t id {:keys [debit-acct credit-acct amount]}] transfers]
                  (.add batch)
                  ; TODO: create linked/pending transfers
                  (.setId              batch id 0)
                  (.setCreditAccountId batch credit-acct 0)
                  (.setDebitAccountId  batch debit-acct 0)
                  (.setCode            batch tb-code)
                  (.setAmount          batch amount)
                  (.setLedger          batch tb-ledger))
          errors (.createTransfers client batch)
          errors (reduce (fn [acc _]
                           (.next errors)
                           (let [i  (.getIndex    errors)
                                 _  (.setPosition batch i)
                                 id (.getId batch UInt128/LeastSignificant)]
                             (conj acc id)))
                         #{}
                         (range (.getLength errors)))]
      (->> transfers
           (remove (fn [[_:t id _value]]
                     (contains? errors id)))))))

(defn lookup-transfers
  "Takes a sequence of `[:l-t id nil]` and looks them up in TigerBeetle.
   Returns a lazy sequence of `[:l-t id {:debit-acct ... :credit-acct ...:amount ...}]`
   for the transfers that were found."
  [client lookups]
  (when (seq lookups)
    (let [batch (IdBatch. (count lookups))
          _     (doseq [[_:l-t id _nil] lookups]
                  (.add batch)
                  (.setId batch id 0))
          results (.lookupTransfers client batch)]
      (repeatedly (.getLength results)
                  (fn []
                    (.next results)
                    (let [id          (.getId              results UInt128/LeastSignificant)
                          debit-acct  (.getDebitAccountId  results UInt128/LeastSignificant)
                          credit-acct (.getCreditAccountId results UInt128/LeastSignificant)
                          amount      (.getAmount          results)]
                      [:l-t id {:debit-acct debit-acct :credit-acct credit-acct :amount amount}]))))))
