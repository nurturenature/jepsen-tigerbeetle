(ns tigerbeetle.core
  (:require
   [clojure.string :as str]
   [jepsen
    [checker :as checker]
    [cli :as cli]
    [generator :as gen]
    [net :as net]
    [tests :as tests]]
   [jepsen.checker.timeline :as timeline]
   [jepsen.nemesis.combined :as nc]
   [jepsen.os.debian :as debian]
   [tigerbeetle
    [db :as db]
    [tigerbeetle :as tb]]
   [tigerbeetle.tests.cycle
    [g-counter :as g-counter]]
   [tigerbeetle.workloads.bank :as bank]
   [tigerbeetle.workloads.set-full :as set-full]))

(def workloads
  "A map of workload names to functions that construct workloads, given opts."
  {:bank      bank/workload
   :set-full  set-full/workload
   :g-counter g-counter/workload})

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
  [{:nemesis nil}
   {:nemesis #{:partition}}
   {:nemesis #{:packet}}
   {:nemesis #{:pause}}
   {:nemesis #{:partition :packet :pause}}
   {:nemesis #{:kill}}
   {:nemesis #{:file-corruption}}
   {:nemesis #{:file-corruption :kill}}])

(def partition-targets
  "Valid targets for partition nemesis operations."
  #{:majority :minority-third :one :majorities-ring :primaries})

(def db-targets
  "Valid targets for DB nemesis operations."
  #{:one :primaries :minority-third :minority :majority :all})

(defn combine-workload-package-generators
  "Constructs a test generator by combining workload and package generators
   configured with CLI test opts"
  [opts workload package]

  (gen/phases
   (gen/log "Workload with nemesis")
   (->> (:generator workload)
        (gen/stagger (/ (:rate opts)))
        (gen/nemesis (:generator package))
        (gen/time-limit (:time-limit opts)))

   (gen/log "Final nemesis")
   (gen/nemesis (:final-generator package))

   (gen/log "Final workload")
   (:final-generator workload)))

(defn test-name
  "Meaningful test name."
  [{:keys [nodes workload nemesis rate tigerbeetle-update tigerbeetle-debug? concurrency] :as opts}]
  (str "TigerBeetle"
       " (" (count nodes) "r-" (tb/num-tb-clients opts) "c-" concurrency "w)"
       " " workload
       " " (if (not (seq nemesis))
             (str ":no-faults")
             (str (seq nemesis)))
       (if (not= 10 rate) (str " :rate-" rate) "")
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
        package       (nc/nemesis-package
                       {:db        db
                        :nodes     (:nodes opts)
                        :faults    (:nemesis opts)
                        :partition {:targets (:partition-targets opts)}
                        :packet    {:targets (:db-targets opts)
                                    :behaviors (for [_ (range 10)]
                                                 (->> net/all-packet-behaviors
                                                      (random-sample (/ (+ 1 (rand-int 3))
                                                                        (count net/all-packet-behaviors)))
                                                      (into {})))}
                        :pause     {:targets (:db-targets opts)}
                        :kill      {:targets (:db-targets opts)}
                        :file-corruption {:targets     (:db-targets opts)
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

   ["-w" "--workload NAME" "What workload to run."
    :parse-fn keyword
    :validate [workloads (cli/one-of workloads)]]])

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

   [nil "--tigerbeetle-debug? BOOLEAN" "Install Tigerbeetle with debugging."
    ; :default false
    :parse-fn parse-boolean]

   [nil "--tigerbeetle-log-level INT" "Configure Tigerbeetle log-level before installing."
    ; :default 3 ; 2 is normal, 3 is more logging
    :parse-fn parse-long]

   [nil "--tigerbeetle-num-clients INT" "How many TigerBeetle clients to create and use in a pool."
    ; :default is --concurrency
    :parse-fn parse-long]

   [nil "--tigerbeetle-update GIT-REVISION" "Update TigerBeetle from git at the revision and install."
    ; :default "main"
    ]])

(defn all-tests
  "Takes parsed CLI options and constructs a sequence of tests:
     :topology or :workload or :nemesis
   = nil will iterate through all values
   running each configuration :test-count times."
  [opts]
  (let [workloads (if-let [w (:workload opts)]
                    [w]
                    (keys workloads))
        nemeses   (if-let [n (:nemesis opts)]
                    [{:nemesis n}]
                    test-all-nemeses)
        counts    (range (:test-count opts))]
    (for [w workloads, n nemeses, _i counts]
      (-> opts
          (assoc :workload w)
          (merge n)
          tigerbeetle-test))))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
                browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn  tigerbeetle-test
                                         :opt-spec (into cli-opts
                                                         test-cli-opts)})
                   (cli/test-all-cmd    {:tests-fn all-tests
                                         :opt-spec (into cli-opts
                                                         test-all-cli-opts)})
                   (cli/serve-cmd))
            args))
