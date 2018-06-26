(ns generic-example
  (:require [medley.core :refer :all]
            [manifold.stream :as s]
            [aleph.http :as http]
            [cheshire.core :as json]
            [clojure.string :as string]
            [clojure.pprint :refer [pprint]]
            [clojure.core.async :as async]
            [taoensso.timbre :as log]))

(require '[exch :refer :all] :reload)
(require '[generic :as ge] :reload)

(defn -main []

  (def tick (ticker :usd/btc))
  (def c (ge/create-connection))

  (log/info "Connected?" (connected? c))

  (connect c)

  (log/info "Connected?" (connected? c))

  (log/info "Attempting to subscribe")
  (send-out c [:subscribe tick])

  (def e (create-exch c))

  (start-standard-msg-handlers e tick)

  (send-in c [:subscribed {:ticker tick}])

  (watch (get-book e tick)
         :logger
         (fn [_ new-state]
           (println "Book updated:")
           (pprint new-state)))

  ;; snapshot
  (send-in c
           [:snapshot
            {:ticker tick
             :snapshot
             '{:asks ([7.59M 23.39216286M] [6.96M 7.23746288M] [5.5M 12.3M])
               :bids ([5M 65.64632441M] [2.51M 0.93194317M] [4M 0.93194317M])}}])

  ;; update
  (send-in c
           [:update
            {:ticker tick
             :update
             '{:asks ([7.59M 23M] [6.96M 7M] [5.5M 0M])
               :bids ([5M 0M] [2.51M 1M])}}])

  ;; unsubscribed
  (send-in c
           [:unsubscribed {:ticker tick}])

  ;; pong
  (send-in c [:pong {:cid 1234 :ts (timestamp)}])

  (Thread/sleep 5000)

  (System/exit 0))
