(ns tigerbeetle.tigerbeetle
  (:require [clojure.string :refer [join]]
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
  1e+9)

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

(defn num-tb-clients
  "How many TigerBeetle clients should be in this test."
  [{:keys [tigerbeetle-num-clients concurrency] :as _test}]
  (or tigerbeetle-num-clients concurrency 3))

(def tb-client-pool
  "A sorted map, [id client], of TigerBeetle clients as an Atom."
  (atom (sorted-map)))

(defn fill-client-pool
  "Create TigerBeetle clients and put them in the pool.
   Returns the number of clients created."
  [test]
  (let [num-clients (num-tb-clients test)]
    (assert (<= num-clients tb-max-num-clients))
    (dotimes [i num-clients]
      (let [client (new-tb-client test)]
        (when client
          (swap! tb-client-pool assoc i client))))
    (count @tb-client-pool)))

(defn drain-client-pool
  "Closes every client and removes it from the pool."
  []
  (doseq [[idx client] @tb-client-pool]
    (u/timeout 1000 :timeout (close-tb-client client))
    (swap! tb-client-pool dissoc idx)))

(defn rand-tb-client
  "Returns a random TigerBeetle client from the pool as [id client].
   Clients are multithreaded and may be shared."
  []
  (->> @tb-client-pool seq rand-nth))

(defn with-tb-client
  "Using a random TigerBeetle client,
   passes it to `(f client & args), and returns the results."
  [f & args]
  (let [[_ client] (rand-tb-client)
        results    (apply f client args)]
    results))

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
   [:t ledger {:id ... :debit-acct ... :credit-acct ...:amount ...}]
   and creates them in TigerBeetle.
   Returns a lazy sequence of all transfers that were created."
  [client transfers]
  (when (seq transfers)
    (let [batch (TransferBatch. (count transfers))
          _     (doseq [[_:t ledger {:keys [id debit-acct credit-acct amount]}] transfers]
                  (.add batch)
                  ; TODO: create linked/pending transfers
                  (.setId              batch id 0)
                  (.setCreditAccountId batch credit-acct 0)
                  (.setDebitAccountId  batch debit-acct 0)
                  (.setCode            batch tb-code)
                  (.setAmount          batch amount)
                  (.setLedger          batch ledger))
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
           (remove (fn [[_:t _ledger {:keys [id]}]]
                     (contains? errors id)))))))
