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
            convert-outgoing-msg
            advise unadvise apply-advice]])

;;* Utils & constants

(def ^:private NSNAME (str (ns-name *ns*)))

(defn- ns-keywordize [str]
  (keyword NSNAME str))

(def ^:private URL "wss://ws-feed.gdax.com")

(defrecord Product [symbol])

;; TODO Products vs Tickers part repeats almost verbatim for different exchanges.
;; Dry this.
(def ^:private PRODUCTS
  (->> ["ETH-BTC"
        "BCH-BTC"
        "LTC-BTC"
        "BTC-USD"
        "BTC-EUR"
        "BTC-GBP"
        "ETH-USD"
        "ETH-EUR"
        "LTC-USD"
        "LTC-EUR"]
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

(defprotocol StateProtocol
  (set-stream [state stream])
  (toggle-status [state] [state status])
  (state-advise [state advice])
  (state-unadvise [state advice]))

(defrecord State [stream status ads]
  StateProtocol
  (set-stream [state stream] (State. stream status ads))
  (toggle-status [state] (State. stream (not status) ads))
  (toggle-status [state status] (State. stream status ads))
  (state-advise [state advice] (State. stream status (update-in ads (:path advice) conj advice)))
  (state-unadvise [state advice] (State. stream status (update-in ads (:path advice) (fn [ad-vec] (->> ad-vec (remove #(= % advice)) vec))))))

(defn clean-state []
  (State.
    nil
    false
    {:before {:incomming []
              :outgoing []}
     :after {:incomming []
             :outgoing []}}))

;;* Connection
(declare
  -convert-incomming-msg
  -convert-outgoing-msg)

(defrecord Connection [in out state stub-in]
  StateProtocol
  (set-stream [conn stream] (swap! state set-stream stream) conn)
  (toggle-status [conn] (swap! state toggle-status))
  (toggle-status [conn status] (swap! state toggle-status status))
  (state-advise [conn advice] (swap! state state-advise advice))
  (state-unadvise [conn advice] (swap! state state-unadvise advice))

  exch/ConnectionProtocol
  (convert-outgoing-msg [conn msg] (-convert-outgoing-msg conn msg))
  (convert-incomming-msg [conn msg]
    (let [msg (json/decode msg ns-keywordize)
          exch-msg (spec/conform ::incomming-message msg)]

      (if (spec/invalid? exch-msg)

        (log/error
          "Received message did not conform to spec\n"
          (with-out-str
            (spec/explain-data
              ::incomming-message
              msg)))

        (-convert-incomming-msg conn exch-msg))))

  (advise [conn advice] (state-advise conn advice) conn)
  (unadvise [conn advice] (state-unadvise conn advice) conn)
  (apply-advice [conn ad-path msg]
    (reduce (fn ad-do-it [msg advice] ((:fn advice) conn msg))
            msg
            (get-in @state
                    (case ad-path
                      [:before :incomming]
                      [:ads :before :incomming]

                      [:after :incomming]
                      [:ads :after :incomming]

                      [:before :outgoing]
                      [:ads :before :outgoing]

                      [:after :outgoing]
                      [:ads :after :outgoing]))))

  (connect [conn]
    (let [exch-stream
          @(http/websocket-client
             URL
             {:max-frame-payload 1e6
              :max-frame-size 1e6})

          in-stream
          ;; sink of msgs from exchange for users to consume
          (s/->sink in)

          out-stream
          ;; source of user msgs to send to exchange
          (s/->source out)

          ;; NOTE connect-via requires an explicit s/put! in the via-fn!

          _
          ;; exchange <= out <= user
          (s/connect-via out-stream
                         (fn [msg]
                           (->> msg
                                ;; exch/msg
                                (apply-advice conn [:before :outgoing])
                                (convert-outgoing-msg conn)
                                (apply-advice conn [:after :outgoing])
                                ;; json
                                (s/put! exch-stream)))
                         exch-stream)

          _
          ;; exchange => in => user
          (s/connect-via exch-stream
                         (fn [msg]
                           (->> msg
                                ;; json
                                (apply-advice conn [:before :incomming])
                                (convert-incomming-msg conn)
                                ;; => [msg ...], therefore
                                (mapv #(apply-advice conn [:after :incomming] %))
                                ;; exch/msg
                                (s/put-all! in-stream)))
                         in-stream
                         { ;; close exch-stream if in-stream is closed
                          :upstream? true
                          ;; do not close in-stream if exch-stream source
                          :downstream? false})

          stub-exch-source
          (s/->source stub-in)

          _
          (s/connect-via stub-exch-source
                         (fn [msg]
                           (->> msg
                                ;; json
                                (apply-advice conn [:before :incomming])
                                (convert-incomming-msg conn)
                                ;; => [msg ...], therefore
                                (mapv #(apply-advice conn [:after :incomming] %))
                                ;; exch/msg
                                (s/put-all! in-stream)))
                         in-stream
                         { ;; close exch-stream if in-stream is closed
                          :upstream? true
                          ;; do not close in-stream if exch-stream source
                          :downstream? false})]
      ;; TODO Maybe have a proc sending out [:ping] to keep alive
      (-> conn
          (set-stream exch-stream)
          ;; TODO add stub-exch-source to State
          ;; (set-stub-exch-source stub-exch-source)
          (toggle-status))))
  (disconnect [conn] (a/close! in) (toggle-status conn))
  (connected? [conn] (:status @state))
  (send-out [conn msg] (a/put! (:out conn) msg))
  (send-in [conn msg] (a/put! (:in conn) msg))
  (conn-name [conn] (keyword NSNAME)))

(defn create-connection []
  (let [in (a/chan 1)
        out (a/chan 1)
        stub-in (a/chan 1)]
    (Connection. in out (atom (clean-state)) stub-in)))

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

(spec/def ::incomming-message
  (spec/multi-spec msg-type ::type))

;;* Convert incomming msg

;; Because incomming msg may encode multiple exch-msgs e.g. gdax "subscriptions"
;; we'll return a sequence of messages to be more robust even if in most cases
;; incomming msg will produce a single exch-msg. Signature is therefore:
;; (-> msg [exch-msg ...])
(defmulti -convert-incomming-msg (fn [conn {tag ::type}] tag))

(defmethod -convert-incomming-msg "snapshot"
  [conn {bids ::bids
         asks ::asks
         ticker ::product_id}]
  (vector
    [:snapshot
     {:ticker ticker
      :snapshot
      {:bids (map #(vector (:price %) (:size %)) bids)
       :asks (map #(vector (:price %) (:size %)) asks)}}]))

(defmethod -convert-incomming-msg "l2update"
  [conn {ticker ::product_id
         changes ::changes}]
  (let [{bids "buy"
         asks "sell"}
        (group-by :side changes)]
    (vector
      [:update
       {:ticker ticker
        :update
        {:bids (map #(vector (:price %) (:size %)) bids)
         :asks (map #(vector (:price %) (:size %)) asks)}}])))

(defmethod -convert-incomming-msg "error"
  [conn {message ::message}]
  (vector
    [:error {:message message}]))

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
  (vector
    [:heartbeat (assoc msg :ticker ticker)]))

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
        {"type" "subscriptions"
         "channels" [{"name" "level2"
                      "product_ids" ["BTC-USD"]}]})))

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

;; TODO Be consistent and put out ns ::keyed maps, then
;; (json/encode {::foo "foo" ::bar "bar"} {:key-fn name})

(defmethod -convert-outgoing-msg :subscribe
  [conn [_ ticker]]
  (json/encode
    {:type "subscribe"
     :product_ids [(-> ticker product :symbol)]
     :channels ["level2"]}))

#_
(defmethod -convert-outgoing-msg :heartbeat
  [conn [_ ticker]]
  (json/encode
    {:type "subscribe"
     :product_ids [(-> ticker product :symbol)]
     :channels ["heartbeat"]}))

;; TODO doesn't seem to work
(defmethod -convert-outgoing-msg :heartbeat
  [conn [_ ticker]]
  (json/encode
    {:type "subscribe"
     :channels [{:name "heartbeat"
                 :product_ids [(-> ticker product :symbol)]}]}))

(defmethod -convert-outgoing-msg :unsubscribe
  [conn [_ ticker]]
  ;; HACK Since GDAX doesn't appear to send :unsubscribed confo, we have to
  ;; generate one for standard-handlers to handle.
  (exch/send-in conn [:unsubscribed {:ticker ticker}])

  (json/encode
    {:type "unsubscribe"
     :product_ids [(-> ticker product :symbol)]
     :channels ["level2" "heartbeat"]}))
