(ns bitfinex
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
            timestamp decimal conj-some]]
         :reload)

;;* Utils & constants

(def ^:private NSNAME (str (ns-name *ns*)))

(def ^:private URL "wss://api.bitfinex.com/ws/2")

(def ^:private pairs
  #{"BTCUSD" "LTCUSD" "LTCBTC" "ETHUSD" "ETHBTC"
    "ETCBTC" "ETCUSD" "RRTUSD" "RRTBTC" "ZECUSD"
    "ZECBTC" "XMRUSD" "XMRBTC" "DSHUSD" "DSHBTC"
    "BTCEUR" "XRPUSD" "XRPBTC" "IOTUSD" "IOTBTC"
    "IOTETH" "EOSUSD" "EOSBTC" "EOSETH" "SANUSD"
    "SANBTC" "SANETH" "OMGUSD" "OMGBTC" "OMGETH"
    "BCHUSD" "BCHBTC" "BCHETH" "NEOUSD" "NEOBTC"
    "NEOETH" "ETPUSD" "ETPBTC" "ETPETH" "QTMUSD"
    "QTMBTC" "QTMETH" "AVTUSD" "AVTBTC" "AVTETH"
    "EDOUSD" "EDOBTC" "EDOETH" "BTGUSD" "BTGBTC"
    "DATUSD" "DATBTC" "DATETH" "QSHUSD" "QSHBTC"
    "QSHETH" "YYWUSD" "YYWBTC" "YYWETH" "GNTUSD"
    "GNTBTC" "GNTETH" "SNTUSD" "SNTBTC" "SNTETH"
    "IOTEUR" "BATUSD" "BATBTC" "BATETH" "MNAUSD"
    "MNABTC" "MNAETH" "FUNUSD" "FUNBTC" "FUNETH"
    "ZRXUSD" "ZRXBTC" "ZRXETH" "TNBUSD" "TNBBTC"
    "TNBETH" "SPKUSD" "SPKBTC" "SPKETH" "TRXUSD"
    "TRXBTC" "TRXETH" "RCNUSD" "RCNBTC" "RCNETH"
    "RLCUSD" "RLCBTC" "RLCETH" "AIDUSD" "AIDBTC"
    "AIDETH" "SNGUSD" "SNGBTC" "SNGETH" "REPUSD"
    "REPBTC" "REPETH" "ELFUSD" "ELFBTC" "ELFETH"})

(defrecord Product [symbol])
(defrecord Pair [symbol])
;; TODO Install reader printer to print e.g. #:eth/btc or something like that

(def ^:private PAIRS
  (->> pairs
       (map ->Pair)
       (into #{})))

(def ^:private PRODUCTS
  (->> pairs
       (map #(str "t" %))
       (map ->Product)
       (into #{})))

(extend-protocol exch/ITicker

  Pair

  (ticker [{sym :symbol
            :as p}]
    (let [kw
          (comp keyword
                string/lower-case)

          [base qt]
          [(subs sym 0 3)
           (subs sym 3)]]
      (exch/map->Ticker
        {:base (kw base)
         :quote (kw qt)})))
  (base [p] (:base (ticker p)))
  (currency [p] (:quote (ticker p)))
  (commodity [p] (:base (ticker p)))
  (ticker-kw [p] (ticker-kw (ticker p)))

  Product

  (ticker [{sym :symbol
            :as p}]
    (let [kw
          (comp keyword
                string/lower-case)

          [base qt]
          [(subs sym 1 4)
           (subs sym 4)]]
      (exch/map->Ticker
        {:base (kw base)
         :quote (kw qt)})))
  (base [p] (:base (ticker p)))
  (currency [p] (:quote (ticker p)))
  (commodity [p] (:base (ticker p)))
  (ticker-kw [p] (ticker-kw (ticker p))))

(def ^:private TICKERS-PAIRS
  (->> PAIRS
       (map #(vector (ticker %) %))
       (into {})))

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

#_((juxt ticker ticker-kw base commodity currency) (->Product "tETHBTC"))
#_((juxt ticker ticker-kw base commodity currency) (->Pair "ETHBTC"))

;;* State

(declare send-msg receive-msg)

(defprotocol StateProtocol
  (chan-of-ticker [state ticker])
  (ticker-of-chan [state chan])
  (add-chan [state chan ticker])
  (rm-chan [state chan])
  (toggle-status [state]))

(defrecord State [stream channels tickers status]
  StateProtocol
  (chan-of-ticker [state ticker] (get tickers ticker))
  (ticker-of-chan [state chan] (get channels chan))
  (add-chan [state chan ticker]
    (State.
      stream
      (assoc channels chan ticker)
      (assoc tickers ticker chan)
      status))
  (rm-chan [state chan]
    (let [ticker (get channels chan)]
      (State.
        stream
        (dissoc channels chan)
        (dissoc tickers ticker)
        status)))
  (toggle-status [state] (State. stream channels tickers (not status))))

(defn clean-state [] (State. nil {} {} false))

;;* Connection

(defrecord Connection [in out state]
  StateProtocol
  (chan-of-ticker [conn ticker] (chan-of-ticker @state ticker))
  (ticker-of-chan [conn chan] (ticker-of-chan @state chan))
  (add-chan [conn chan ticker] (swap! state add-chan chan ticker) conn)
  (rm-chan [conn chan] (swap! state rm-chan chan) conn)
  (toggle-status [conn] (swap! state toggle-status) conn)

  exch/ConnectionProtocol
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

          _
          ;; exchange <= out <= user
          (s/connect-via out-stream json/encode exch-stream)

          _
          ;; exchange => in => user
          (s/connect-via exch-stream
                         (fn [msg]
                           (receive-msg
                             conn
                             (json/decode
                               #(keyword NSNAME %))))
                         in-stream)]
      (Connection.
        in
        out
        (atom
          (State. exch-stream {} {} true)))))
  (disconnect [conn]
    (a/close! in)
    (toggle-status conn))
  (connected? [conn] (:status @state))
  (send-out [conn msg]
    (send-msg conn msg))
  ;; for testing: puts msg on the internal :in port
  (send-in [conn msg]
    (a/put!
      (:in conn)
      (receive-msg conn msg)))
  (conn-name [conn] (keyword NSNAME)))

(defn create-connection []
  (let [in (a/chan 1)
        out (a/chan 1)]
    (Connection. in out (atom (clean-state)))))

;;* Message specs

;; NOTE Caution with spec/def registry names. If it ever matches a key in a map
;; that you conform against spec will be checked recursively which may lead to
;; bizarre behaviour and errors. Suppose the idea is to treat spec/defs as
;; keywords with validation wherever said keywords may turn up.

(defn map-json-map [msg]
  (-> msg
      (json/encode)
      (json/decode
        ;; TODO whould *ns* work as expected when this fn is requried elsewhere?
        ;; E.g. gdax namespace etc.
        #(keyword NSNAME %))))

;;** - update
(spec/def ::update
  (spec/spec
    ;; nested
    (spec/cat :price number?
              :orders number?
              :size number?)))

(spec/def ::update-msg
  (spec/cat
    :channel number?
    :update ::update))

;;** - snapshot
(spec/def ::snapshot
  (spec/spec
    ;; nested
    (spec/* ::update)))

(spec/def ::snapshot-msg
  (spec/cat
    :channel number?
    :snapshot ::snapshot))

;;** - heartbeat
(spec/def ::hb-msg
  (spec/cat :channel number?
            :hb #{"hb"}))

;;** - event
(spec/def ::code (spec/and pos-int? #(< % 100000)))
(spec/def ::msg string?)

(defmulti event-type ::event)

(defmethod event-type "pong" [_]
  (spec/keys :req [::event]))

(defmethod event-type "info" [_]
  (spec/keys :req [::event]
             :opt [::code ::msg]))

(defmethod event-type "subscribed" [_]
  (spec/keys :req [::event ::pair ::chanId]))

(defmethod event-type "unsubscribed" [_]
  (spec/keys :req [::event ::status ::chanId]))

(defmethod event-type "error" [_]
  (spec/keys :req [::event ::code ::msg]))

(spec/def ::event-msg
  (spec/multi-spec event-type ::event))

;;** - any
(spec/def ::message
  (spec/or
    :event ::event-msg
    :heartbeat ::hb-msg
    :snapshot ::snapshot-msg
    :update ::update-msg))

;;* Message receive
(declare
  convert-msg
  convert-event-msg)


(defn receive-msg [conn message]
  (let [msg
        (spec/conform
          ::message
          message)]

    (if (spec/invalid? msg)

      (do
        ;; log error unrecognized message and drop it - don't break the system
        (log/error
          "Received message did not conform to spec\n"
          (with-out-str
            (spec/explain-data
              ::message
              message))))

      (convert-msg conn msg))))

;;** - dispatch by tag

(defmulti convert-msg (fn [conn [tag]] (println "tag " tag) tag))

(defn add-ticker-or-drop
  [conn [tag {channel :channel
              :as m}]]
  (if-let [ticker (ticker-of-chan conn channel)]

    [tag (assoc m :ticker ticker)]

    (let [reason (str "Ticker for channel " channel " not found")]
      (log/error reason)
      [:drop (-> m
                 (assoc :tag tag)
                 (assoc :reason reason))])))

(defmethod convert-msg :heartbeat
  [conn msg]
  (add-ticker-or-drop conn msg))

(defn- snapshot->bids-asks [payload]
  (let [{:keys [bids asks]}
        (group-by (fn [{:keys [price size]}]
                    (if (neg? size)
                      :asks
                      :bids))
                  payload)

        bids
        (map (fn [{:keys [price size]}]
               [(decimal price)
                (decimal size)])
             bids)

        asks
        (map (fn [{:keys [price size]}]
               [(decimal price)
                (decimal (- 0 size))])
             asks)]
    {:bids bids
     :asks asks}))

(defn- update->bids-asks [payload]
  (snapshot->bids-asks payload))

#_
(=
  (snapshot->bids-asks
    [{:price 584.58, :orders 11, :size 65.64632441}
     {:price 584.51, :orders 1, :size 0.93194317}
     {:price 584.59, :orders 4, :size -23.39216286}
     {:price 584.96, :orders 1, :size -7.23746288}
     {:price 584.97, :orders 1, :size -12.3}])

  '{:bids ([584.58M 65.64632441M]
           [584.51M 0.93194317M])
    :asks ([584.59M 23.39216286M]
           [584.96M 7.23746288M]
           [584.97M 12.3M])})

(defmethod convert-msg :snapshot
  [conn [tag payload]]
  (add-ticker-or-drop
    conn
    [tag (update
           payload
           :snapshot snapshot->bids-asks)]))

(defmethod convert-msg :update
  [conn [_ {update :update
            :as m}]]
  (add-ticker-or-drop
    conn
    [:update
     (assoc m :update
            (update->bids-asks [update]))]))

(defmethod convert-msg :event [conn [_ m]]
  (convert-event-msg conn m))

;;** - dispatch by event type
(defmulti convert-event-msg (fn [conn msg] (::event msg)))

(defmethod convert-event-msg "pong"
  [conn {ts ::ts cid ::cid :as m}]
  [:pong
   (-> m
       (assoc :timestamp (timestamp ts))
       (assoc :id cid))])

(defmethod convert-event-msg "info"
  [conn {event ::event code ::code msg ::msg :as m}]
  (let [tag
        (case code
          20051 :reconnect
          20060 :pause
          20061 :resume
          nil :info
          ;; else
          (do
            (log/error
              "Received message of type event => " event
              " with unrecognized code " code)
            :drop))

        [orig-tag reason]
        (when (= tag :drop)
          [:event
           (str "Unrecognized code " code
                " in message")])

        payload
        (-> m
            (assoc-some :message msg)
            (assoc-some :reason reason)
            (assoc-some :tag orig-tag))]

    [tag payload]))

(defmethod convert-event-msg "subscribed"
  [conn {pair ::pair channel ::chanId :as m}]
  (let [ticker
        (ticker
          (->Pair pair))]

    (add-chan conn channel ticker)
    (log/info "Subscribed to ticker " ticker)
    [:subscribed
     (assoc m :ticker ticker)]))

(defmethod convert-event-msg "unsubscribed"
  [conn {channel ::chanId :as m}]
  (if-let [ticker (ticker-of-chan conn channel)]

    (do
      (rm-chan conn channel)
      (log/info "Unsubscribed ticker " ticker)
      [:unsubscribed
       (assoc m :ticker ticker)])

    (do
      (let [reason (str "Ticker for channel " channel " not found")]
        (log/error reason)
        [:drop
         (-> m
             (assoc :tag :unsubscribed)
             (assoc :reason reason))]))))

(defmethod convert-event-msg "error"
  [conn {code ::code msg ::msg :as m}]
  (let [tag
        :error

        payload
        (-> m
            (assoc :message msg)
            (assoc :code code))]

    (log/error
      "Received error code from server:"
      payload)

    [tag payload]))

;;* Message send

;; TODO prob want a ISender protocol defined in exch that each connection can
;; implement. Then implementations would dispatch on the message tag and would
;; know if it needs to send it to a specific REST endpoint.
(defmulti send-msg (fn [conn [tag _]] tag))

(defmethod send-msg :subscribe
  [conn [_ ticker]]
  (a/put!
    (:out conn)
    {:event "subscribe"
     :channel "book"
     :symbol (:symbol (product ticker))
     :prec "P0"
     :freq "F0"}))

(defmethod send-msg :unsubscribe
  [conn [_ ticker]]
  (let [channel (chan-of-ticker conn ticker)]
    (a/put!
      (:out conn)
      {:event "unsubscribe"
       :chanId channel})))

(defmethod send-msg :default
  [conn msg]
  (a/put!
    (:out conn)
    msg))

;; Conf
;; {
;;   event: "conf",
;;   flags: FLAGS
;;  }
