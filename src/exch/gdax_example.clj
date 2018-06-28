(ns gdax-example
  (:require [medley.core :refer :all]
            [manifold.stream :as s]
            [aleph.http :as http]
            [cheshire.core :as json]
            [clojure.string :as string]
            [clojure.pprint :refer [pprint]]
            [clojure.core.async :as async]
            [taoensso.timbre :as log]))

(require '[exch :refer :all] :reload)
(require '[gdax :as gdax] :reload)

(def tick (ticker :usd/btc))

(log/info "Creating connection")
(def c (gdax/create-connection))

(log/info "Creating exch")
(def e (create-exch c))

(log/info "Start standard handlers")
(def handlers-ch (start-standard-msg-handlers e tick))

(defn top-of-book [{bids :bids asks :asks :as book}]
  (-> book
      (assoc :bids (->> bids (take 5) (into {})))
      (assoc :asks (->> asks (take 5) (into {})))))

(defn -main []
  (log/info "Connecting to exchange")
  (connect c)

  (def book (get-book e tick))
  (log/info "Book subscribed? " (book-subscribed? book))
  (book-sub book)

  (send-to-exch e [:heartbeat tick])

  (def result
    (async/thread
      (let [result
            (vector
              (do (Thread/sleep 3000)
                  (book-snapshot book))
              (do (Thread/sleep 2000)
                  (book-snapshot book)))]
        (log/info "Book subscribed? " (book-subscribed? book))
        (book-unsub book)
        (Thread/sleep 2000)
        result)))

  (pprint
    (map top-of-book
         (async/<!! result)))

  (log/info "Book subscribed? " (book-subscribed? book))
  (disconnect c)

  (System/exit 0))

(comment
  (send-from-exch
    e
    (first
      (convert-incomming-msg
        c
        (json/encode
          {"type" "subscriptions"
           "channels" [{"name" "level2"
                        "product_ids" ["BTC-USD"]}]}))))

  (send-from-exch
    e
    (first
      (convert-incomming-msg
        c
        (json/encode
          {"type" "snapshot"
           "product_id" "BTC-USD"
           "bids" [["6500.11" "0.45054140"]]
           "asks" [["6500.15" "0.57753524"]
                   ["6504.38" "0.5"]]}))))

  (send-from-exch
    e
    (first
      (convert-incomming-msg
        c
        (json/encode
          {"type" "l2update"
           "product_id" "BTC-USD"
           "changes"
           [["buy" "6500.09" "0.84702376"]
            ["sell" "6507.00" "1.88933140"]
            ["sell" "6505.54" "1.12386524"]
            ["sell" "6504.38" "0"]]})))))
