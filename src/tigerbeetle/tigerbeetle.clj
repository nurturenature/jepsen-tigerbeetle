(ns tigerbeetle.tigerbeetle
  (:require [clojure.string :refer [join]]
            [jepsen
             [util :as u]]
            [jepsen.control.net :as cn]
            [slingshot.slingshot :refer [try+]]
            [jepsen.client :as client])
  (:import (com.tigerbeetle AccountBatch Client IdBatch TransferBatch UInt128)))

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

(def tb-timeout
  "Timeout value in ms for Tigerbeetle transactions."
  10000)

; TODO: use flags.linked for linked transactions
; TODO: use flags.pending for pending transactions

(defn new-tb-client
  "Create a new TigerBeetle client for the cluster of nodes.
  Returns a new java Object or nil if client could not be created."
  [nodes]
  (try+
   (Client. tb-cluster (into-array String (tb-replica-addresses nodes)))
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
  [{:keys [nodes] :as test}]
  (let [num-clients (num-tb-clients test)]
    (dotimes [i num-clients]
      (let [client (new-tb-client nodes)]
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
  "Takes a seq of accounts to create in a TigerBeetle ledger.
   Returns a lazy sequence of any errors `{account error}`."
  ([client accounts] (create-accounts client accounts tb-ledger))
  ([client accounts ledger]
   (when (seq accounts)
     (let [batch (AccountBatch. (count accounts))
           _     (doseq [account accounts]
                   (.add batch)
                   (.setId     batch account 0)
                   (.setCode   batch tb-code)
                   (.setLedger batch ledger))
           errors (.createAccounts client batch)]
       (repeatedly (.getLength errors)
                   (fn []
                     (.next errors)
                     (let [i (.getIndex errors)
                           r (.toString (.getResult errors))
                           a (do (.setPosition batch i)
                                 (.getId batch UInt128/LeastSignificant))]
                       {a r})))))))

(defn lookup-accounts
  "Takes a sequence of accounts and looks them up in TigerBeetle.
   Returns a lazy sequence of `{:id :credits-posted :debits-posted}`
   for the accounts that were found."
  [client accounts]
  (when (seq accounts)
    (let [batch (IdBatch. (count accounts))
          _     (doseq [account accounts]
                  (.add batch)
                  (.setId batch account 0))
          results (.lookupAccounts client batch)]
      (repeatedly (.getLength results)
                  (fn []
                    (.next results)
                    {:id             (.getId results UInt128/LeastSignificant)
                     :credits-posted (.getCreditsPosted results)
                     :debits-posted  (.getDebitsPosted  results)})))))

(defn create-transfers
  "Takes a sequence of transfers and creates them in TigerBeetle.
   Returns a lazy sequence of any errors `{:id :error :from :to :amount}`."
  [client transfers]
  (when (seq transfers)
    (let [batch (TransferBatch. (count transfers))
          _     (doseq [{:keys [from to amount] :as _transfer} transfers]
                  (.add batch)
                ; TODO: create linked/pending transfers
                  (.setId              batch (u/rand-distribution {:min 1, :max 1e+8}) 0)
                  (.setCreditAccountId batch to 0)
                  (.setDebitAccountId  batch from 0)
                  (.setCode            batch tb-code)
                  (.setAmount          batch amount)
                  (.setLedger          batch tb-ledger))
          errors (.createTransfers client batch)]
      (repeatedly (.getLength errors)
                  (fn []
                    (.next errors)
                    (let [i (.getIndex  errors)
                          r (.getResult errors)]
                      (.setPosition batch i)
                      {:id     (.getId              batch UInt128/LeastSignificant)
                       :error  r
                       :from   (.getDebitAccountId  batch UInt128/LeastSignificant)
                       :to     (.getCreditAccountId batch UInt128/LeastSignificant)
                       :amount (.getAmount          batch)}))))))
