(ns tigerbeetle.core
  (:require
   [clojure.string :as str]
   [clojure.tools.logging :refer [info warn]]
   [jepsen
    [checker :as checker]
    [cli :as cli]
    [generator :as gen]
    [tests :as tests]
    [util :as u]]
   [jepsen.checker.timeline :as timeline]
   [jepsen.nemesis.combined :as nc]
   [jepsen.os.debian :as debian]
   [tigerbeetle
    [db :as db]
    [tigerbeetle :as tb]]
   [tigerbeetle.workloads
    [bank :as bank]
    [set-full :as set-full]]))

(def workloads
  "A map of workload names to functions that construct workloads, given opts."
  {:bank      bank/workload
   :set-full  set-full/workload})

(def nemeses
  "The types of faults our nemesis can produce"
  #{:partition :pause :kill :packet :file-corruption})

(def special-nemeses
  "A map of special nemesis names to collections of faults"
  {:none      []
   :standard  [:partition :packet]
   :process   [:pause :kill]
   :all       [:partition :packet :pause :kill :file-corruption]})

(def test-all-nemeses
  "A collection of partial options maps for various nemeses we want to run as a
  part of test-all."
  [; {:nemesis nil}
   {:nemesis #{:partition}}
   {:nemesis #{:packet}}
   {:nemesis #{:pause}}
   {:nemesis #{:partition :packet}}
   {:nemesis #{:partition :pause}}
   {:nemesis #{:packet :pause}}
   ;;  {:nemesis #{:kill}}
   ;;  {:nemesis #{:file-corruption}}
   ;;  {:nemesis #{:file-corruption :kill}}
   ])

(def partition-targets
  "Valid targets for partition nemesis operations."
  ; #{:majority :minority-third :one :majorities-ring :primaries}
  #{:one :majority :majorities-ring})

(def db-targets
  "Valid targets for DB nemesis operations."
  ; #{:one :primaries :minority-third :minority :majority :all}
  #{:one :minority :majority :all})

(defn combine-workload-package-generators
  "Constructs a test generator by combining workload and package generators
   configured with CLI test opts"
  [opts workload package]

  (gen/phases
   (gen/log "Workload with nemesis")
   (->> (:generator workload)
        (gen/nemesis (:generator package))
        (gen/time-limit (:time-limit opts)))

   (gen/log "Final nemesis")
   (gen/nemesis (:final-generator package))

   (gen/log "Final workload")
   (:final-generator workload)))

(defn test-name
  "Meaningful test name."
  [{:keys [nodes workload nemesis concurrency
           tigerbeetle-update tigerbeetle-debug? tigerbeetle-client-max-concurrency] :as opts}]
  (str "TigerBeetle"
       " (r" (count nodes) "-"
       "c" (tb/num-tb-clients opts) (when tigerbeetle-client-max-concurrency (str ":" tigerbeetle-client-max-concurrency)) "-"
       "w" concurrency ")"
       " " workload
       " " (if (not (seq nemesis))
             (str ":no-faults")
             (str (seq nemesis)))
       (if tigerbeetle-update (str " :git-" (subs tigerbeetle-update 0 (min 8 (count tigerbeetle-update)))) "")
       (if tigerbeetle-debug? " :debug" "")))

(defn tigerbeetle-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
                      :concurrency, ...), constructs a test map."
  [opts]
  (let [db            (db/db :git)
        workload-name (:workload opts)
        {:keys [max-transfer
                total-amount
                accounts]
         :as workload} ((workloads workload-name) opts)
        package (nc/nemesis-package
                 {:db        db
                  :nodes     (:nodes opts)
                  :faults    (:nemesis opts)
                  :partition {:targets (:partition-targets opts)}
                  :packet    {:targets (:db-targets opts)
                              :behaviors [{:duplicate {:percent      :20%
                                                       :correlation  :75%}
                                           :reorder   {:percent      :20%
                                                       :correlation  :75%}}]}
                  :pause     {:targets (:db-targets opts)}
                  :kill      {:targets [:one :minority]}          ; (:db-targets opts)}
                  :file-corruption {:targets     [:one :minority] ;(:db-targets opts)
                                    :corruptions [{:type :bitflip
                                                   :file db/data-dir
                                                   :probability {:distribution :one-of :values [1e-2 1e-3 1e-4]}}
                                                  {:type :truncate
                                                   :file db/data-dir
                                                   :drop {:distribution :geometric :p 1e-3}}]}
                  :interval  (:nemesis-interval opts)})]

    (merge tests/noop-test
           {:max-transfer max-transfer
            :total-amount total-amount
            :accounts     accounts}
           opts
           {:plot {:nemeses (:perf package)}}
           {:name       (test-name opts)
            :os         debian/os
            :db         db
            :client     (:client workload)
            :nemesis    (:nemesis package)
            :generator  (combine-workload-package-generators opts workload package)
            :checker    (checker/compose
                         {:workload   (:checker workload)
                          :perf       (checker/perf
                                       {:nemeses (:perf package)})
                          :timeline   (timeline/html)
                          :stats      (checker/stats)
                          :exceptions (checker/unhandled-exceptions)
                          :logs       (checker/log-file-pattern #"panic\:" db/log-file)})})))

(def validate-non-neg
  [#(and (number? %) (not (neg? %))) "Must be non-negative"])

(defn parse-comma-kws
  "Takes a comma-separated string and returns a collection of keywords."
  [spec]
  (->> (str/split spec #",")
       (remove #{""})
       (map keyword)))

(defn parse-comma-longs
  "Takes a comma-separated string and returns a collection of longs."
  [spec]
  (->> (str/split spec #",")
       (remove #{""})
       (map parse-long)))

(defn parse-nemesis-spec
  "Takes a comma-separated nemesis string and returns a collection of keyword
  faults."
  [spec]
  (->> (parse-comma-kws spec)
       (mapcat #(get special-nemeses % [%]))
       set))

(def test-cli-opts
  "CLI options just for test"
  [[nil "--nemesis FAULTS" "A comma-separated list of nemesis faults to enable"
    :default  nil
    :parse-fn parse-nemesis-spec
    :validate [(partial every? (into nemeses (keys special-nemeses)))
               (str (cli/one-of nemeses) ", or " (cli/one-of special-nemeses))]]

   [nil "--tigerbeetle-num-clients INT" "How many TigerBeetle clients to create and use in a pool."
    ; :default is --concurrency
    :parse-fn parse-long]

   [nil "--tigerbeetle-num-workers INT" "How many Jepsen workers to run."
    ; :default is (count nodes)
    :parse-fn parse-long]

   ["-w" "--workload NAME" "What workload to run."
    :default :bank
    :parse-fn keyword
    :validate [workloads (cli/one-of workloads)]]])

(def test-all-cli-opts
  "CLI options just for test-all.
   Lack of :default value, e.g. will be nill, causes test-all to use all values"
  [[nil "--nemesis FAULTS" "A comma-separated list of nemesis faults to enable"
    :parse-fn parse-nemesis-spec
    :validate [(partial every? (into nemeses (keys special-nemeses)))
               (str (cli/one-of nemeses) ", or " (cli/one-of special-nemeses))]]

   [nil "--tigerbeetle-num-clients INT" "How many TigerBeetle clients to create and use in a pool."
    ; :default is --concurrency
    :parse-fn parse-comma-longs]

   [nil "--tigerbeetle-num-replicas INT" "How many TigerBeetle replicas in the cluster."
    ; :default is (count --nodes)
    :parse-fn parse-comma-longs]

   [nil "--tigerbeetle-num-workers INT" "How many Jepsen workers to run."
    ; :default is (count nodes)
    :parse-fn parse-comma-longs]

   ["-w" "--workload NAME" "What workload to run."
    :parse-fn parse-comma-kws
    :validate [(partial every? workloads) (cli/one-of workloads)]]])

(def cli-opts
  "Additional command line options."
  [[nil "--accounts [1,2,...]" "Vector of account numbers."
    :default (vec (range 1 9))
    :parse-fn read-string]

   [nil "--db-targets TARGETS" "A comma-separated list of nodes to target for db operations; e.g. one,all"
    :default (vec db-targets)
    :parse-fn parse-comma-kws
    :validate [(partial every? db-targets) (cli/one-of db-targets)]]

   [nil "--negative-balances? BOOLEAN" "Allow negative balances?"
    :default true
    :parse-fn boolean]

   [nil "--nemesis-interval SECONDS" "How long to wait between nemesis faults."
    :default  15
    :parse-fn read-string
    :validate [#(and (number? %) (pos? %)) "must be a positive number"]]

   [nil "--partition-targets TARGETS" "A comma-separated list of nodes to target for network partitions; e.g. one,all"
    :default (vec partition-targets)
    :parse-fn parse-comma-kws
    :validate [(partial every? partition-targets) (cli/one-of partition-targets)]]

   [nil "--rate HZ" "Target number of ops/sec"
    :default  10
    :parse-fn read-string
    :validate validate-non-neg]

   [nil "--rate-cycle? BOOLEAN" "Cycle the rate between rate/4, rate/2, rate/4, rate"
    :default  false
    :parse-fn parse-boolean]

   [nil "--tigerbeetle-client-max-concurrency INT" "Passed to Client new to configure TigerBeetle client."
    ; :default nil, use Client defaults
    :parse-fn parse-long]

   [nil "--tigerbeetle-debug? BOOLEAN" "Install Tigerbeetle with debugging."
    ; :default false
    :parse-fn parse-boolean]

   [nil "--tigerbeetle-log-level INT" "Configure Tigerbeetle log-level before installing."
    ; :default 3 ; 2 is normal, 3 is more logging
    :parse-fn parse-long]

   [nil "--tigerbeetle-update GIT-REVISION" "Update TigerBeetle from git at the revision and install."
    ; :default "main"
    ]])

(defn all-tests-inflate
  "Enhance `(test-all-)cli-opts` for `all-tests`."
  [{:keys [options] :as parsed}]
  (let [{:keys [tigerbeetle-num-replicas
                tigerbeetle-num-clients
                tigerbeetle-num-workers]} options
        tigerbeetle-num-replicas (or (u/coll tigerbeetle-num-replicas) [1 3 5])
        tigerbeetle-num-clients  (or (u/coll tigerbeetle-num-clients)  [1 3 5])
        tigerbeetle-num-workers  (or (u/coll tigerbeetle-num-workers)  (map (fn [n] (* 3 n)) tigerbeetle-num-clients))]
    (assoc parsed
           :options (assoc options
                           :tigerbeetle-num-replicas tigerbeetle-num-replicas
                           :tigerbeetle-num-clients  tigerbeetle-num-clients
                           :tigerbeetle-num-workers  tigerbeetle-num-workers))))

(defn all-tests
  "Takes parsed CLI options and constructs a sequence of tests:
     :topology or :workload or :nemesis
   = nil will iterate through all values
   running each configuration :test-count times."
  [{:keys [nodes workload nemesis test-count tigerbeetle-num-replicas tigerbeetle-num-clients tigerbeetle-num-workers] :as opts}]
  (let [workloads    (if-let [w workload]
                       (u/coll w)
                       (keys workloads))
        nemeses      (if-let [n nemesis]
                       [{:nemesis n}]
                       test-all-nemeses)
        num-replicas (u/coll tigerbeetle-num-replicas)
        num-clients  (u/coll tigerbeetle-num-clients)
        num-workers  (u/coll tigerbeetle-num-workers)
        counts       (range test-count)]
    (for [w workloads
          n nemeses
          num-replicas num-replicas
          num-clients  num-clients
          concurrency  num-workers
          _i counts]
      (let [test-map (-> opts
                         (assoc :nodes (take num-replicas nodes)
                                :workload w
                                :concurrency concurrency
                                :tigerbeetle-num-clients num-clients)
                         (merge n)
                         tigerbeetle-test)]
        test-map))))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
                browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn  tigerbeetle-test
                                         :opt-spec (into cli-opts
                                                         test-cli-opts)})
                   (cli/test-all-cmd    {:tests-fn all-tests
                                         :opt-spec (into cli-opts
                                                         test-all-cli-opts)
                                         :opt-fn all-tests-inflate})
                   (cli/serve-cmd))
            args))
