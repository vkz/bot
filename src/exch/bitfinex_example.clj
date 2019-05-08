;; Copyright (C) 2018, 2019 by Vlad Kozin

(ns bitfinex-example
  (:require [medley.core :refer :all]
            [manifold.stream :as s]
            [aleph.http :as http]
            [cheshire.core :as json]
            [clojure.string :as string]
            [clojure.pprint :refer [pprint]]
            [clojure.core.async :as async]
            [taoensso.timbre :as log]
            [clojure.edn :as edn]))

(require '[exch :refer :all] :reload)
(require '[bitfinex :as bx] :reload)

(def tick (ticker :usd/eth))

(log/info "Creating connection")
(def c (bx/create-connection))

(log/info "Creating exch")
(def e (create-exch c))

(log/info "Start standard handlers")
(def handlers-ch (start-standard-msg-handlers e tick))

(defn run-stub []
  (connect c)

  (defadvice print-before-convert :before :incomming [conn msg]
    (println "Before convert:")
    (pprint msg)
    msg)

  (defadvice print-after-convert :after :incomming [conn msg]
    (println "After convert:")
    (pprint msg)
    msg)

  (advise c print-before-convert)
  (advise c print-after-convert)

  (async/put!
    (:stub-in c)
    (json/encode
      {"event" "subscribed"
       "chanId" 10961
       "pair" "ETHUSD"}))

  #_(apply-advice c [:before :incomming] {:foo :bar})

  (async/put!
    (:stub-in c)
    (json/encode
      '(10961
         [[584.58 11 65.64632441]
          [584.51 1 0.93194317]
          [584.59 4 -23.39216286]
          [584.96 1 -7.23746288]
          [584.97 1 -12.3]])))

  (async/put! (:stub-in c) (json/encode '(10961 [583.75 1 1])))
  (async/put! (:stub-in c) (json/encode '(10961 [584.97 0 -1])))
  (async/put! (:stub-in c) (json/encode '(10961 [584.59 4 -23.34100906])))
  (async/put! (:stub-in c) (json/encode '(10961 [586.94 1 -29.9])))
  (async/put! (:stub-in c) (json/encode '(10961 [583.75 0 1])))

  (Thread/sleep 3000)

  (unadvise c print-before-convert)
  (unadvise c print-after-convert)

  (disconnect c))

(defn run-exch []
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
  (disconnect c))

(defn -main [arg]
  (case (edn/read-string arg)
    run-stub (run-stub)
    run-exch (run-exch)))
