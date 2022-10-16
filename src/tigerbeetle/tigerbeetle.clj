(ns tigerbeetle.tigerbeetle
  (:require [clojure.string :refer [join]]
            [jepsen
             [util :as u]]
            [jepsen.control.net :as cn])
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
  It is a new java Object."
  [nodes]
  (Client. tb-cluster (into-array String (tb-replica-addresses nodes))))

(defn close-tb-client
  "Closes a TigerBeetle client."
  [client]
  (.close client))

(defn with-tb-client
  "Creates a TigerBeetle client for the given nodes,
   passes it to `(f client & args), closes client, and returns the results."
  [nodes f & args]
  (let [client  (new-tb-client nodes)
        results (apply f client args)]
    (close-tb-client client)
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
                           r (.getResult errors)
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
