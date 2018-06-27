(ns gdax
  (:require [clojure.spec.alpha :as spec]
            [clojure.spec.gen.alpha :as gen]
            [medley.core :refer :all]
            [aleph.http :as http]
            [clj-http.client :as http-client]
            [manifold.deferred :as d]
            [manifold.stream :as s]
            [byte-streams :as bs]
            [cheshire.core :as json]
            [clojure.string :as string]
            [clojure.pprint :refer [pprint]]
            [clojure.core.async :as a]
            [clojure.core.async.impl.protocols :refer [Channel]]
            [taoensso.timbre :as log]))

(require '[exch :as exch
           :refer
           [ticker ticker-kw base commodity currency
            timestamp decimal conj-some
            convert-incomming-msg
            convert-outgoing-msg]]
         :reload)

;;* Utils & constants

(def ^:private NSNAME (str (ns-name *ns*)))

(defn- ns-keywordize [str]
  (keyword NSNAME str))

(def ^:private URL "wss://ws-feed.gdax.com")

;; TODO Products vs Tickers part repeats almost verbatim for different exchanges.
;; Dry this.
(def ^:private PRODUCTS
  (->>
    ["BTC-USD" "ETH-USD" "ETH-BTC" "LTC-USD" "LTC-BTC"
     "ETH-BTC" "LTC-BTC" "BTC-EUR" "ETH-EUR" "ETH-BTC"
     "LTC-EUR" "LTC-BTC" "BTC-GBP" "BTC-EUR" "ETH-BTC"
     "ETH-EUR" "LTC-BTC" "LTC-EUR" "ETH-BTC" "LTC-BTC"]
    (map ->Product)
    (into #{})))

(extend-protocol exch/ITicker

  Product

  (ticker [{sym :symbol
            :as p}]
    (let [kw
          (comp keyword
                string/lower-case)

          [base qt]
          (string/split sym #"-")]
      (exch/map->Ticker
        {:base (kw base)
         :quote (kw qt)})))
  (base [p] (:base (ticker p)))
  (currency [p] (:quote (ticker p)))
  (commodity [p] (:base (ticker p)))
  (ticker-kw [p] (ticker-kw (ticker p))))

(def ^:private TICKERS-PRODUCTS
  (->> PRODUCTS
       (map #(vector (ticker %) %))
       (into {})))

(defn- product [ticker]
  (if-some [product (TICKERS-PRODUCTS ticker)]
    product
    (do
      (log/error "Ticker " ticker " does not match any product.")
      nil)))

;;* State

;;* Connection

;;* Message specs

(defn map-json-map [msg]
  (-> msg
      (json/encode)
      (json/decode ns-keywordize)))

{"type" "subscribe"
 "product_ids" ["ETH-BTC"]
 "channels" ["level2"
             "heartbeat"
             {"name" "ticker"
              "product_ids" ["ETH-BTC"]}]}

(defn decimal-str? [v]
  (try
    (decimal v)
    (catch NumberFormatException _
      :clojure.spec.alpha/invalid)))

(defn product-symbol->ticker? [v]
  (let [p (Product. v)]
    (if (get PRODUCTS p)
      (ticker p)
      :clojure.spec.alpha/invalid)))

;; TODO While capturing :price and :size and converting to decimal with
;; spec/conformer trick is nice, we essentially end up transforming back in the
;; -convert-incomming-msg, then why do it here? E.g. we end-up doing:
;; [price size] => {:price price :size size} => [price size]

(spec/def ::bid
  (spec/spec
    (spec/cat :price (spec/conformer decimal-str?)
              :size (spec/conformer decimal-str?))))
#_
(spec/def ::bid
  (spec/spec
    (spec/tuple (spec/conformer decimal-str?)
                (spec/conformer decimal-str?))))

#_(spec/conform
    ::bid
    ["6500.11" "0.45054140"])

(spec/def ::ask
  (spec/spec
    (spec/cat :price (spec/conformer decimal-str?)
              :size (spec/conformer decimal-str?))))

(spec/def ::bids
  (spec/* ::bid))

(spec/def ::asks
  (spec/* ::ask))

(spec/def ::change
  (spec/spec
    (spec/cat :side #{"buy" "sell"}
              :price (spec/conformer decimal-str?)
              :size (spec/conformer decimal-str?))))

(spec/def ::changes
  (spec/* ::change))

(spec/def ::message string?)

(spec/def ::product_id
  (spec/conformer product-symbol->ticker?))

(spec/def ::product_ids
  (spec/* ::product_id))

;; (spec/def ::name #{"level2" "heartbeat" "ticker"})

(spec/def ::channel
  (spec/keys :req [::name ::product_ids]))

(spec/def ::channels
  (spec/* ::channel))

(defmulti msg-type ::type)

(defmethod msg-type "snapshot" [_]
  (spec/keys :req [::type ::product_id ::bids ::asks]))

(defmethod msg-type "l2update" [_]
  (spec/keys :req [::type ::product_id ::changes]))

(defmethod msg-type "error" [_]
  (spec/keys :req [::type ::message]))

(defmethod msg-type "subscriptions" [_]
  (spec/keys :req [::type ::channels]))

(defmethod msg-type "heartbeat" [_]
  (spec/keys :req [::type ::product_id ::time]
             :opt [::sequence ::last_trade_id]))

(spec/def ::gdax-message
  (spec/multi-spec msg-type ::type))


;; // Request
;; // Subscribe to ETH-USD and ETH-EUR with the level2, heartbeat and ticker channels,
;; // plus receive the ticker entries for ETH-BTC and ETH-USD
;; {
;;     "type": "subscribe",
;;     "product_ids": [
;;         "ETH-USD",
;;         "ETH-EUR"
;;     ],
;;     "channels": [
;;         "level2",
;;         "heartbeat",
;;         {
;;             "name": "ticker",
;;             "product_ids": [
;;                 "ETH-BTC",
;;                 "ETH-USD"
;;             ]
;;         }
;;     ]
;;  }

;; // Response
;; {
;;     "type": "subscriptions",
;;     "channels": [
;;         {
;;             "name": "level2",
;;             "product_ids": [
;;                 "ETH-USD",
;;                 "ETH-EUR"
;;             ],
;;         },
;;         {
;;             "name": "heartbeat",
;;             "product_ids": [
;;                 "ETH-USD",
;;                 "ETH-EUR"
;;             ],
;;         },
;;         {
;;             "name": "ticker",
;;             "product_ids": [
;;                 "ETH-USD",
;;                 "ETH-EUR",
;;                 "ETH-BTC"
;;             ]
;;         }
;;     ]
;;  }

;; // Request
;; {
;;     "type": "unsubscribe",
;;     "product_ids": [
;;         "ETH-USD",
;;         "ETH-EUR"
;;     ],
;;     "channels": ["ticker"]
;;  }

;; // Request
;; {
;;     "type": "unsubscribe",
;;     "channels": ["heartbeat"]
;;  }

;; // Request
;; {
;;     "type": "subscribe",
;;     "channels": [{ "name": "heartbeat", "product_ids": ["ETH-EUR"] }]
;;  }

;; // Heartbeat message
;; {
;;     "type": "heartbeat",
;;     "sequence": 90,
;;     "last_trade_id": 20,
;;     "product_id": "BTC-USD",
;;     "time": "2014-11-07T08:19:28.464459Z"
;;  }
