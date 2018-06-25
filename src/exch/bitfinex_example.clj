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

  (send-out c [:ping])

  (send-out c [:subscribe tick])

  (def result
    (async/thread
      (let [result
            (vector
              (do (Thread/sleep 3000)
                  (snapshot
                    (get-book e tick)))
              (do (Thread/sleep 2000)
                  (snapshot
                    (get-book e tick))))]
        (send-out c [:unsubscribe tick])
        (Thread/sleep 2000)
        result)))

  (pprint
    (async/<!! result))

  (System/exit 0))
