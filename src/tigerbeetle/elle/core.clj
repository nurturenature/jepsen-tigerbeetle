(ns tigerbeetle.elle.core
  (:require [clojure.pprint :refer [pprint]]
            [clojure.set :as set]
            [clojure.tools.logging :refer [info warn]]
            [elle
             [core  :as ec]
             [graph :as eg]]
            [knossos.op :as op]))

;; Monotonic keys!

(defrecord MonotonicKeyExplainer []
  ec/DataExplainer
  (explain-pair-data [_ a b]
    (let [a (:value a)
          b (:value b)]
      ; Find keys in common
      (->> (keys a)
           (filter b)
           (reduce (fn [_ k]
                     (let [a (get a k)
                           b (get b k)]
                       (when (and a b (< a b))
                         (reduced {:type   :monotonic
                                   :key    k
                                   :value  a
                                   :value' b}))))
                   nil))))

  (render-explanation [_ {:keys [key value value']} a-name b-name]
    (str a-name " observed " (pr-str key) " = "
         (pr-str value) ", and " b-name
         " observed a higher value "
         (pr-str value'))))

(defn monotonic-key-order
  "Given a key, and a history where ops are maps of keys to values, constructs
  a partial order graph over ops reading successive values of key k."
  [k history]
  ; Construct an index of values for k to all ops with that value.
  (let [index (as-> history x
                (group-by (fn [op] (get (:value op) k ::not-found)) x)
                (dissoc x ::not-found))]
    (->> index
         ; Take successive pairs of keys
         (sort-by key)
         (partition 2 1)
         ; And build a graph out of them
         (reduce (fn [g [[v1 ops1] [v2 ops2]]]
                   (eg/link-all-to-all g ops1 ops2 :monotonic-key))
                 (eg/linear (eg/digraph)))
         eg/forked)))

(defn monotonic-key-graph
  "Analyzes ops where the :value of each op is a map of keys to values. Assumes
  keys are monotonically increasing, and derives relationships between ops
  based on those values."
  [history]
  (let [history (filter op/ok? history)
        graph (->> history
                   (mapcat (comp keys :value))
                   distinct
                   (map (fn [k] (monotonic-key-order k history)))
                   (reduce eg/digraph-union))]
    {:graph     graph
     :explainer (MonotonicKeyExplainer.)}))
