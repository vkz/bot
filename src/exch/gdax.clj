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

(defrecord Product [symbol])

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

;;* Convert incomming msg

(defmulti -convert-incomming-msg (fn [conn {tag ::type}] tag))

(defmethod -convert-incomming-msg "snapshot"
  [conn {bids ::bids
         asks ::asks
         ticker ::product_id}]
  [:snapshot
   {:ticker ticker
    :snapshot
    {:bids (map #(vector (:price %) (:size %)) bids)
     :asks (map #(vector (:price %) (:size %)) asks)}}])

(defmethod -convert-incomming-msg "l2update"
  [conn {ticker ::product_id
         changes ::changes}]
  (let [{bids "buy"
         asks "sell"}
        (group-by :side changes)]
    [:update
     {:ticker ticker
      :update
      {:bids (map #(vector (:price %) (:size %)) bids)
       :asks (map #(vector (:price %) (:size %)) asks)}}]))

(defmethod -convert-incomming-msg "error"
  [conn {message ::message}]
  [:error {:message message}])

(defn channel->subscribed-msgs
  [{channel ::name
    tickers ::product_ids}]
  (letfn [(ticker->subscribed [ticker]
            [:subscribed
             {:ticker ticker
              :channel channel}])]
    (map ticker->subscribed tickers)))

(defmethod -convert-incomming-msg "subscriptions"
  [conn {channels ::channels}]
  ;; GDAX allows to subscribe to multiple channels and products in one request, so
  ;; the subscribtion confo may in general report multiple results. Every such
  ;; result should generate one :subscribed msg. Although we handle GDAX's
  ;; multiple case here our API should only ever allow one subscribtion per
  ;; request i.e. ::channels key will be a sequence of just one confirmation,
  ;; whose ::product_ids key will be a sequence of just one ticker.

  ;; TODO either we return a single subscribed msg here (the first and only one)
  ;; and report an error if there're more than one, or we handle a multi-msg
  ;; return and sequentially put them on the wire somewhere in the (connect ...)
  (mapcat channel->subscribed-msgs channels))

(defmethod -convert-incomming-msg "heartbeat"
  [conn {ticker ::product_id :as msg}]
  [:heartbeat (assoc msg :ticker ticker)])

(comment
  (-convert-incomming-msg
    'conn
    (spec/conform
      ::gdax-message
      (map-json-map
        {"type" "snapshot"
         "product_id" "BTC-EUR"
         "bids" [["6500.11" "0.45054140"]]
         "asks" [["6500.15" "0.57753524"]
                 ["6504.38" "0.5"]]})))

  (-convert-incomming-msg
    'conn
    (spec/conform
      ::gdax-message
      (map-json-map
        {"type" "l2update"
         "product_id" "BTC-EUR"
         "changes"
         [["buy" "6500.09" "0.84702376"]
          ["sell" "6507.00" "1.88933140"]
          ["sell" "6505.54" "1.12386524"]
          ["sell" "6504.38" "0"]]})))

  (-convert-incomming-msg
    'conn
    (spec/conform
      ::gdax-message
      (map-json-map
        {"type" "error"
         "message" "error message"})))

  (-convert-incomming-msg
    'conn
    (spec/conform
      ::gdax-message
      (map-json-map
        {"type" "subscriptions"
         "channels" [{"name" "level2"
                      "product_ids" ["ETH-USD" "ETH-EUR"]}
                     {"name" "heartbeat"
                      "product_ids" ["ETH-USD" "ETH-EUR"]}
                     {"name" "ticker"
                      "product_ids" ["ETH-USD" "ETH-EUR" "ETH-BTC"]}]})))

  (-convert-incomming-msg
    'conn
    (spec/conform
      ::gdax-message
      (map-json-map
        {"type" "heartbeat"
         "sequence" 90
         "last_trade_id" 20
         "product_id" "BTC-USD"
         "time" "2014-11-07T22:19:28.578544Z"}))))

;;* Convert outgoing msg

;; TODO I can actually define both -convert-incomming-msg and
;; -convert-outgoing-msg as defmulti in (ns exch). Then each exchange specific ns
;; would extend them with methods. I'd need a dispatch that takes exchange into
;; account not merely tag: [(exch-name conn) tag]?
(defmulti -convert-outgoing-msg (fn [conn [tag _]] tag))

(defmethod -convert-outgoing-msg :subscribe
  [conn [_ ticker]]
  (json/encode
    {:type "subscribe"
     :product_ids [(-> ticker product :symbol)]
     :channels ["level2"]}))

(defmethod -convert-outgoing-msg :heartbeat
  [conn [_ ticker]]
  (json/encode
    {:type "subscribe"
     :product_ids [(-> ticker product :symbol)]
     :channels ["heartbeat"]}))

#_
(defmethod -convert-outgoing-msg :heartbeat
  [conn [_ ticker]]
  (json/encode
    {:type "subscribe"
     :channels [{:name "heartbeat"
                 :product_ids [(-> ticker product :symbol)]}]}))

(defmethod -convert-outgoing-msg :unsubscribe
  [conn [_ ticker]]
  (json/encode
    {:type "unsubscribe"
     :product_ids [(-> ticker product :symbol)]
     :channels ["level2"
                "heartbeat"]}))
