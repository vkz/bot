(ns bitfinex-example
  (:require [medley.core :refer :all]
            [manifold.stream :as s]
            [aleph.http :as http]
            [cheshire.core :as json]
            [clojure.string :as string]
            [clojure.pprint :refer [pprint]]
            [clojure.core.async :as async]
            [taoensso.timbre :as log]))

(require '[exch :refer :all] :reload)
(require '[bitfinex :as bx] :reload)

(def tick (ticker :usd/btc))

(log/info "Creating connection")
(def c (bx/create-connection))

(log/info "Creating exch")
(def e (create-exch c))

(log/info "Start standard handlers")
(def handlers-ch (start-standard-msg-handlers e tick))

(defn -main []
  (log/info "Connecting to exchange")
  (connect c)

  (send-to-exch e [:ping])

  (def book (get-book e tick))
  (log/info "Book subscribed? " (book-subscribed? book))
  (book-sub book)

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
    (async/<!! result))

  (log/info "Book subscribed? " (book-subscribed? book))

  (System/exit 0))
