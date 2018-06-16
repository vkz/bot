(ns bitfinex
  (:require [clojure.spec.alpha :as spec]
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
            [taoensso.timbre :as log]

            [exch :as exch
             :refer [ticker base commodity currency timestamp
                     decimal]]

            :reload))

(def URL "wss://api.bitfinex.com/ws/2")

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
  (commodity [p] (:base (ticker p))))

#_((juxt ticker base commodity currency) (->Product "tETHBTC"))
#_((juxt ticker base commodity currency) (->Pair "ETHBTC"))

(def ^:private CHANNELS (atom {}))

;;* Responses

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
        #(keyword (str (ns-name *ns*)) %))))

;;* Incomming message specs

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

;;* Incomming message parse
(declare
  dispatch-msg
  dispatch-event-msg)

(defn receive [message]
  (let [msg
        (spec/conform
          ::message
          message)]

    (if (spec/invalid? msg)
      (do
        (log/error
          "Received message did not conform to spec\n"
          (with-out-str
            (spec/explain
              ::message
              message))))
      (do
        (dispatch-msg msg)))))

;;** - dispatch by tag
(defmulti dispatch-msg first)
(defmethod dispatch-msg :heartbeat [hb] hb)
(defmethod dispatch-msg :snapshot [sn] sn)
(defmethod dispatch-msg :update [up] up)
(defmethod dispatch-msg :event [[_ m]]
  (dispatch-event-msg m))

;;** - dispatch by event type
(defmulti dispatch-event-msg ::event)

(defmethod dispatch-event-msg "pong"
  [{ts ::ts cid ::cid :as m}]
  [:pong
   {:timestamp (timestamp ts)
    :id cid
    :original m}])

(defmethod dispatch-event-msg "info"
  [{event ::event code ::code msg ::msg :as m}]
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

        payload
        (-> {}
            (assoc-some :message msg)
            (assoc :original m))]

    [tag payload]))

(defmethod dispatch-event-msg "subscribed"
  [{pair ::pair channel ::chanId :as m}]
  (let [ticker
        (ticker
          (->Pair pair))]

    (log/info "Subscribed to ticker " ticker)
    (swap! CHANNELS assoc channel ticker)
    [:subscribed
     {:ticker ticker
      :original m}]))

(defmethod dispatch-event-msg "unsubscribed"
  [{channel ::chanId :as m}]
  (let [ticker (get @CHANNELS channel)]
    (log/info "Unsubscribed ticker " ticker)
    (swap! CHANNELS dissoc channel)
    [:unsubscribed
     {:ticker ticker
      :original m}]))

(defmethod dispatch-event-msg "error"
  [{code ::code msg ::msg :as m}]
  (let [tag :error
        payload {:code code
                 :message msg
                 :original m}]

    (log/error
      "Received error code from server:"
      payload)

    [tag payload]))

(comment

  (receive
    '(10961
       [[584.58 11 65.64632441]
        [584.51 1 0.93194317]
        [584.59 4 -23.39216286]
        [584.96 1 -7.23746288]
        [584.97 1 -12.3]]))


  (receive
    '(10961
       [584.58 11 65.64632441]))


  (receive
    '(10961 "hb"))


  (receive
    (map-json-map
      {"event" "pong",
       "ts" 1511545528111,
       "cid" 1234}))


  (receive
    (map-json-map
      {"event""info",
       "code" 20060,
       "msg" "Entering Maintenance mode."}))


  (receive
    (map-json-map
      {"event""info",
       "code" 20017,
       "msg" "Foo"}))


  (receive
    (map-json-map
      {"event" "info",
       "version" 2,
       "serverId" "90788dae-4b28-4f4f-963f-364c33e587d2",
       "platform" {"status" 1}}))


  (do
    (reset! CHANNELS {})
    (receive
      (map-json-map
        {"event" "subscribed",
         "pair" "ETHUSD"
         "chanId" 5863})))


  (do
    (swap! CHANNELS assoc
           5863 #exch.Ticker{:base :eth, :quote :usd})
    (receive
      (map-json-map
        {"event" "unsubscribed",
         "status" "OK",
         "chanId" 5863})))


  (receive
    (map-json-map
      {"event" "error",
       "msg" "Unsubscription failed",
       "code" 10400})))


;;* Requests

;; Conf
;; {
;;   event: "conf",
;;   flags: FLAGS
;;  }

;; Subscribe

;; // request
;; {
;;   "event": "subscribe",
;;   "channel": "book",
;;   "symbol": "tBTCUSD",
;;   "prec": "P0",
;;   "freq": "F0",
;;   "len": 25
;;  }

;; Unsubscribe

;; // request
;; {
;;   "event": "unsubscribe",
;;   "chanId": 6
;; }

;;* Books
(defrecord Book [ticker asks bids channel])

(defn empty-book [ticker]
  (map->Book {:ticker ticker
              :asks (sorted-map-by <)
              :bids (sorted-map-by >)}))
