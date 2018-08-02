(ns scratch
  (:require [medley.core :refer :all]
            [manifold.stream :as s]
            [cheshire.core :as json]
            [clojure.string :as string]
            [clojure.pprint :refer [pprint]]
            [clojure.core.async :as async]
            [taoensso.timbre :as log]
            [clojure.edn :as edn]
            [clojure.repl :as repl]))

(log/set-level! :info)

(require '[arb :as arb])

(require '[exch :as exch :refer :all])

(require '[bitfinex :as bitfinex])
(require '[gdax :as gdax])

(defn create-connection-fn [exch-kw]
  (case exch-kw
    :gdax gdax/create-connection
    :bitfinex bitfinex/create-connection
    (do
      (log/error "Unrecognized exchange " exch-kw)
      (throw (str "Unrecognized echange " exch-kw)))))

(defn top-of-book [{bids :bids asks :asks :as book}]
  (-> book
      (assoc :bids (->> bids (take 5) (into {})))
      (assoc :asks (->> asks (take 5) (into {})))))

(defn run [{exch1 :exch1
            exch2 :exch2
            tickers :tickers
            level :log
            :as opt}]
  (when level (log/set-level! level))
  (log/info "Starting arbitrager with config " opt)
  (let [tickers (->> tickers sort dedupe (map ticker))
        create-connection-1 (create-connection-fn exch1)
        create-connection-2 (create-connection-fn exch2)
        conn1 (create-connection-1)
        exch1 (create-exch conn1)
        conn2 (create-connection-2)
        exch2 (create-exch conn2)]

    (doseq [tick tickers]
      (start-standard-msg-handlers exch1 tick)
      (start-standard-msg-handlers exch2 tick))

    ;; TODO awkward, want (connect exch) to work. Do I even need separate conn
    ;; and exch?
    (connect conn1)
    (connect conn2)

    ;; Gotta force lazy seq since we actually run-arb is side-effecting and we
    ;; actually want it to run!
    (let [stop-fns (doall
                     (map
                       (fn [tick]
                         (arb/run-arb tick exch1 exch2))
                       tickers))]
      {:exch1 exch1
       :exch2 exch2
       :tickers tickers
       :stop-fns stop-fns})))

(defn -main [arg]
  (let [{:keys [exch1 exch2 tickers stop-fns]}
        (run (edn/read-string arg))

        shutdown
        (fn [& args]
          (doseq [stop stop-fns]
            (stop :unsub? true :disconnect true))
          ;; Latest books
          (log/info "Shutdown ARBITRAGER requested")
          ;; (log/with-level :debug
          ;;   (log/debug (with-out-str
          ;;                (pprint
          ;;                  [(top-of-book (book-snapshot (get-book exch1 ticker)))
          ;;                   (top-of-book (book-snapshot (get-book exch2 ticker)))]))))
          ;; (flush)
          (System/exit 0))]



    ;; Send SIGINT to shutdown gracefully
    (repl/set-break-handler! shutdown)

    ;; If "debug" file exists set log-level to :debug, else reset it to the
    ;; initial level. Poor man's introspection.
    (async/go
      (let [level (:level log/*config*)]
        (loop []
          (async/<! (async/timeout 10000))
          ;; TODO Looks up in the project dir. Where does it look when a .jar?
          (if (.exists (clojure.java.io/file "debug"))
            (when (not (= (:level log/*config*) :debug)) (log/set-level! :debug))
            (when (not (= (:level log/*config*) level))  (log/set-level! level)))
          (recur))))

    ;; block thread
    (read-line)
    (shutdown)))
