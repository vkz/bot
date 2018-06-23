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
            [taoensso.timbre :as log]

            [exch :as exch
             :refer [ticker base commodity currency timestamp
                     decimal conj-some]]))

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

(def ^:private TICKERS-PAIRS
  (->> PAIRS
       (map #(vector (ticker %) %))
       (into {})))

(def ^:private TICKERS-PRODUCTS
  (->> PRODUCTS
       (map #(vector (ticker %) %))
       (into {})))

(defn product [ticker]
  (if-some [product (TICKERS-PRODUCTS ticker)]
    product
    (do
      (log/error "Ticker " ticker " does not match any product.")
      nil)))

#_((juxt ticker base commodity currency) (->Product "tETHBTC"))
#_((juxt ticker base commodity currency) (->Pair "ETHBTC"))

(defn snapshot->bids-asks [payload]
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

(defn update->bids-asks [payload]
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
  convert-msg
  convert-event-msg)

(defn receive [message]
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

      (convert-msg msg))))

;;** - dispatch by tag

(defmulti convert-msg first)

(defn add-ticker-or-drop
  [[tag {channel :channel
         :as m}]]
  (if-let [ticker (get @CHANNELS channel)]

    [tag (assoc m :ticker ticker)]

    (let [reason (str "Ticker for channel " channel " not found")]
      (log/error reason)
      [:drop (-> m
                 (assoc :tag tag)
                 (assoc :reason reason))])))

(defmethod convert-msg :heartbeat
  [msg]
  (add-ticker-or-drop msg))

(defmethod convert-msg :snapshot
  [[tag payload]]
  (add-ticker-or-drop
    [tag (update
           payload
           :snapshot snapshot->bids-asks)]))

(defmethod convert-msg :update
  [[_ {update :update
       :as m}]]
  (add-ticker-or-drop
    [:update
     (assoc m :update
            (update->bids-asks [update]))]))

(defmethod convert-msg :event [[_ m]]
  (convert-event-msg m))

;;** - dispatch by event type
(defmulti convert-event-msg ::event)

(defmethod convert-event-msg "pong"
  [{ts ::ts cid ::cid :as m}]
  [:pong
   (-> m
       (assoc :timestamp (timestamp ts))
       (assoc :id cid))])

(defmethod convert-event-msg "info"
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
  [{pair ::pair channel ::chanId :as m}]
  (let [ticker
        (ticker
          (->Pair pair))]

    (swap! CHANNELS assoc channel ticker)
    (log/info "Subscribed to ticker " ticker)
    [:subscribed
     (assoc m :ticker ticker)]))

(defmethod convert-event-msg "unsubscribed"
  [{channel ::chanId :as m}]
  (if-let [ticker (get @CHANNELS channel)]

    (do
      (swap! CHANNELS dissoc channel)
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
  [{code ::code msg ::msg :as m}]
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

(comment

  (spec/conform
    :exch/message
    (receive
      '(5863
         [[584.58 11 65.64632441]
          [584.51 1 0.93194317]
          [584.59 4 -23.39216286]
          [584.96 1 -7.23746288]
          [584.97 1 -12.3]])))

  (receive
      '(5863
         [[584.58 11 65.64632441]
          [584.51 1 0.93194317]
          [584.59 4 -23.39216286]
          [584.96 1 -7.23746288]
          [584.97 1 -12.3]]))

  (receive
    '(5863
       [584.58 11 65.64632441]))

  (receive
    '(5863 "hb"))

  (receive
    (map-json-map
      {"event" "pong",
       "ts" 1511545528111,
       "cid" 1234}))

  (receive
    (map-json-map
      {"event" "info",
       "code" 20060,
       "msg" "Entering Maintenance mode."}))
  [:resume ...]
  [:reconnect ...]


  (receive
    (map-json-map
      {"event" "info",
       "code" 20017,
       "msg" "Foo"}))

  (receive
    (map-json-map
      {"event" "info",
       "version" 2,
       "serverId" "90788dae-4b28-4f4f-963f-364c33e587d2",
       "platform" {"status" 1}}))

  (receive
    (map-json-map
      {"event" "subscribed",
       "pair" "ETHUSD"
       "chanId" 5863}))

  (receive
    (map-json-map
      {"event" "unsubscribed",
       "status" "OK",
       "chanId" 5863}))

  (receive
    (map-json-map
      {"event" "error",
       "msg" "Unsubscription failed",
       "code" 10400})))

(def ^:private CONNECTION (atom nil))

(defn connection []
  (deref CONNECTION))

(defn connect []
  (or
    ;; already connected
    (connection)
    ;; fresh connection
    (let [in (a/chan)
          out (a/chan)
          pub (a/pub in (fn [[tag {ticker :ticker}]]
                          (conj-some [tag] ticker)))]

      ;; stub to handle outgoing messages
      (a/go-loop [msg (a/<! out)]
        (case (first msg)
          :stop (do
                  (reset! CONNECTION nil)
                  (log/info "Connection stopped."))
          ;; else
          (do
            (log/info "Message sent: " msg)
            (recur (a/<! out)))))

      (reset! CONNECTION {:in in
                          :out out
                          :pub pub}))))

;; TODO prob want a ISender protocol defined in exch that each connection can
;; implement. Then implementations would dispatch on the message tag and would
;; know if it needs to send it to a specific REST endpoint.
(defmulti send-msg (fn [[tag _]] tag))

(defmethod send-msg :subscribe
  [[_ ticker]]
  (a/put!
    (:out (connection))
    {:event "subscribe"
     :channel "book"
     :symbol (:symbol (product ticker))
     :prec "P0"
     :freq "F0"}))

(defmethod send-msg :unsubscribe
  [[_ ticker]]
  (let [channel
        (find-first
          (fn [[_ val]] (= val ticker))
          @CHANNELS)]
    (a/put!
      (:out (connection))
      {:event "unsubscribe"
       :chanId channel})))

(defmethod send-msg :default
  [msg]
  (a/put!
    (:out (connection))
    msg))

(defrecord Book [ticker agent ch])
#_(defrecord OrderBook [ticker bids asks])

(defn empty-book [ticker]
  {:ticker ticker
   :asks (sorted-map-by <)
   :bids (sorted-map-by >)})

(defn snapshot->book
  [old_book snapshot]
  {:ticker (old_book :ticker)
   :asks (into (sorted-map-by <) (:asks snapshot))
   :bids (into (sorted-map-by >) (:bids snapshot))})

(defn- update-book-entry [side [price size]]
  (if (zero? size)
    (dissoc side price)
    (assoc side price size)))

(defn update->book
  [book update]
  ;; TODO {:post check correct sort?}
  (-> book
      (assoc :bids
             (reduce update-book-entry
                     (:bids book)
                     (:bids update)))
      (assoc :asks
             (reduce update-book-entry
                     (:asks book)
                     (:asks update)))))

#_
(update->book
  (snapshot->book
    (empty-book (ticker :eth/btc))
    '{:asks ([7.59M 23.39216286M] [6.96M 7.23746288M] [5.5M 12.3M])
      :bids ([5M 65.64632441M] [2.51M 0.93194317M] [4M 0.93194317M])})
  '{:asks ([7.59M 23M] [6.96M 7M] [5.5M 0M])
    :bids ([5M 0M] [2.51M 1M])})

(def ^:private BOOKS
  ;; ticker => Book
  (atom {}))
;; Where each map entry is:
;;
;; {ticker (Book. ticker agent ch)}
;;
;; where agent is e.g.
;;
;; {:ticker ticker
;;  :asks   {6.96M 7M                      ;best ask
;;           7.59M 23M}
;;  :bids   {4M    0.9M                    ;best bid
;;           2.51M 1M}}

(defmulti book-sub-handle-msg (fn [[tag _] _] tag))

(defmethod book-sub-handle-msg :snapshot
  [[_ payload] book]
  (send (:agent book)
        snapshot->book
        (:snapshot payload))
  [:recur])

(defmethod book-sub-handle-msg :update
  [[_ payload] book]
  (send (:agent book)
        update->book
        (:update payload))
  [:recur])

(defmethod book-sub-handle-msg :subscribed [_ _]
  [:recur])

(defmethod book-sub-handle-msg :unsubscribed [_ {ticker :ticker
                                                 :as book}]
  (doseq [topic [[:subscribed ticker]
                 [:unsubscribed ticker]
                 [:snapshot ticker]
                 [:update ticker]]]
    (a/unsub (:pub (connection))
             topic
             (:ch book)))
  [:stop])

(defmethod book-sub-handle-msg :proc
  [[_ payload] {ticker :ticker
                :as book}]
  (case (:command payload)
    (:stop) (do
              (log/info
                "Book " book " subscribtion received :stop command.")
              ;; unsub so we don't accidentally block the pub
              (doseq [topic [[:subscribed ticker]
                             [:unsubscribed ticker]
                             [:snapshot ticker]
                             [:update ticker]]]
                (a/unsub (:pub (connection))
                         topic
                         (:ch book)))
              ;; inform exchange, but we won't handle the unsubscribed confo
              (send-msg [:unsubscribe ticker])
              ;; stop proc
              (conj-some [:stop] (:when payload)))

    ;; else
    (do
      (log/error "Unrecognized command in proc " (:command payload))
      [:recur])))

(defn subscribe-book
  [{ticker :ticker
    :as book}]

  (or
    ;; already subscribed
    (get @BOOKS ticker)

    ;; subscribe new book
    (do
      ;; start proc
      (a/go
        (loop [msg (a/<! (:ch book))
               stop-when (constantly false)]
          (log/info "Book received " msg)

          (let [[ret arg] (book-sub-handle-msg msg book)]
            (log/debug "ret and arg" [ret arg])
            (cond
              (nil? msg)
              (do
                (log/info
                  "Process updating " ticker " book stopped."
                  "Book pub was closed.")
                (swap! @BOOKS dissoc ticker)
                [:process "stopped"])

              (stop-when msg)
              (do
                (log/info "Process updating " ticker " book stopped."
                          "Last msg " msg " received.")
                (swap! @BOOKS dissoc ticker)
                [:process "stopped"])

              :handle-ret-and-continue
              (case ret
                :recur (recur
                         (a/<! (:ch book))
                         stop-when)
                :stop (if-some [stop-when arg]
                        ;; stop the next (stop-when msg) => true
                        (let [stop-when arg]
                          (recur
                            (a/<! (:ch book))
                            stop-when))
                        ;; stop immediately
                        (do
                          (log/info "Process updating " ticker " book stopped.")
                          (swap! @BOOKS dissoc ticker)
                          [:process "stopped"])))))))

      ;; sub to topics
      (doseq [topic [[:subscribed ticker]
                     [:unsubscribed ticker]
                     [:snapshot ticker]
                     [:update ticker]]]

        (a/sub (:pub (connection))
               topic
               (:ch book)))

      (swap! BOOKS assoc ticker book)

      ;; send subscribtion msg to exch
      (send-msg [:subscribe ticker])

      book)))

(defn -main []
  (let [conn (connect)
        ticker (ticker :usd/btc)

        book (subscribe-book
               (map->Book
                 {:ticker ticker
                  :agent (agent (empty-book ticker))
                  :ch (a/chan 100)}))]
    (add-watch
      (:agent book)
      :book-watcher (fn [_ _ _ new-state]
                      (println "Book updated:")
                      (pprint new-state)))

    (a/put!
      (:in (connection))
      [:subscribed {:ticker ticker}])

    ;; snapshot
    (a/put!
      (:in (connection))
      [:snapshot
       {:ticker ticker
        :snapshot
        '{:asks ([7.59M 23.39216286M] [6.96M 7.23746288M] [5.5M 12.3M])
          :bids ([5M 65.64632441M] [2.51M 0.93194317M] [4M 0.93194317M])}}])

    ;; update
    (a/put!
      (:in (connection))
      [:update
       {:ticker ticker
        :update
        '{:asks ([7.59M 23M] [6.96M 7M] [5.5M 0M])
          :bids ([5M 0M] [2.51M 1M])}}])

    (read-line)))

(comment

  (connect)
  (send-msg [:subscribe (ticker :btc/eth)])
  (send-msg [:unsubscribe (ticker :btc/eth)])

  (a/put! (:ch b) [:proc {:command :stop}])
  (a/put! (:ch b) [:proc {:command :stop
                          :when (fn [[tag]] (= tag :unsubscribed))}])
  (reset! BOOKS (atom {}))

  (subscribe-book
    (map->Book
      {:ticker (ticker :usd/btc)
       :agent (agent (empty-book (ticker :usd/btc)))
       :ch (a/chan)}))


  (def b (get @BOOKS (ticker :usd/btc)))

  (defn topicfn [[tag {ticker :ticker}]]
    (conj-some [tag] ticker))

  (a/put!
    (:in (connection))
    [:subscribed {:ticker (ticker :usd/btc)}])

  ;; snapshot
  (a/put!
    (:in (connection))
    [:snapshot
     {:ticker (ticker :usd/btc)
      :snapshot
      '{:asks ([7.59M 23.39216286M] [6.96M 7.23746288M] [5.5M 12.3M])
        :bids ([5M 65.64632441M] [2.51M 0.93194317M] [4M 0.93194317M])}}])

  ;; update
  (a/put!
    (:in (connection))
    [:update
     {:ticker (ticker :usd/btc)
      :update
      '{:asks ([7.59M 23M] [6.96M 7M] [5.5M 0M])
        :bids ([5M 0M] [2.51M 1M])}}])

  (update->book
    (snapshot->book
      (empty-book (ticker :eth/btc))
      '{:asks ([7.59M 23.39216286M] [6.96M 7.23746288M] [5.5M 12.3M])
        :bids ([5M 65.64632441M] [2.51M 0.93194317M] [4M 0.93194317M])})
    '{:asks ([7.59M 23M] [6.96M 7M] [5.5M 0M])
      :bids ([5M 0M] [2.51M 1M])}))


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
