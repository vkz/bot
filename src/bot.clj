(ns bot
  (:require [cheshire.core :as json]
            [clojure.pprint :refer [pprint]]
            [clojure.repl :as repl]

            [aleph.http :as http]
            [manifold.stream :as s]
            [manifold.deferred :as d]
            [clojure.core.async :as a]

            [clojure.string :as string]
            [clojure.walk :refer [postwalk postwalk-replace]]

            [clojure.test :refer [deftest
                                  with-test
                                  is are
                                  testing
                                  use-fixtures]]))

;;* Orderbook

;; TODO make it a protocol so it can be implemented for various objects
(defn ticker [{:keys [commodity currency ticker]
               :as obj}]
  (or ticker
      (keyword
       (name currency)
       (name commodity))))

(defn ticker-pair [ticker]
  {:commodity (keyword (name ticker))
   :currency (keyword (namespace ticker))})

(defrecord Book [ticker asks bids])

(defn empty-book [ticker]
  (map->Book {:ticker ticker
              :asks (sorted-map-by <)
              :bids (sorted-map-by >)}))

(defn snapshot->book [{:keys [asks
                              bids]
                       :as sn}]
  (map->Book {:ticker (ticker sn)
              :asks (into (sorted-map-by <) asks)
              :bids (into (sorted-map-by >) bids)}))

(defmulti update (fn [_ u] (or
                            (and (contains? u :changes)
                                 :update)
                            (first u))))

(defmethod update :buy [{bids :bids :as b}
                        [_ price size]]
  {:pre [(pos? price)
         (not (neg? size))]}
  (if (zero? size)
    (do
      (assert (contains? bids price)
              (str "Book " (:ticker b)
                   " appears out of sync: could not find entry for price "
                   price
                   " to be removed."))
      (assoc b :bids (dissoc bids price)))
    (assoc b :bids (assoc bids price size))))

(defmethod update :sell [{asks :asks :as b}
                         [_ price size]]
  {:pre [(pos? price)
         (not (neg? size))]}
  (if (zero? size)
    (do
      (assert (contains? asks price)
              (str "Book" (:ticker b)
                   " appears out of sync: could not find entry for price "
                   price
                   " to be removed."))
      (assoc b :asks (dissoc asks price)))
    (assoc b :asks (assoc asks price size))))

(defmethod update :update [b {changes :changes
                              :as u}]
  {:pre [(= (ticker b) (ticker u))]}
  (reduce update b changes))

;;* Connect

(def connections*
  (atom
   {:gdax     {:url "wss://ws-feed.gdax.com"
               :msg (json/encode
                     {"type"        "subscribe"
                      "product_ids" ["ETH-USD" "ETH-EUR"]
                      "channels"    ["level2"
                                     "heartbeat"
                                     {"name"        "ticker"
                                      "product_ids" ["ETH-BTC" "ETH-USD"]}]})
               :conn nil}
    :bitfinex {:url "wss://api.bitfinex.com/ws/2"
               :msg (json/encode
                     {:event   "subscribe"
                      :channel "book"
                      :symbol  "tETHUSD"
                      :prec    "P0"
                      :freq    "F0"})
               :conn nil}}))

(defn connect [ex]
  (let [{:keys [url conn]} (get @connections* ex)]
    (when (or (not conn)
              (s/closed? conn))
      (swap! connections*
             assoc-in [ex :conn]
             @(http/websocket-client url
                                     {:max-frame-payload 1e6
                                      :max-frame-size 1e6})))
    (get-in @connections* [ex :conn])))

(defn subscribe [ex]
  (let [{:keys [conn msg]} (get @connections* ex)]
    @(s/put! conn msg)
    conn))

;;* Filter

(defprotocol Money
  (decimal [this]))

(extend-protocol Money
  String
  (decimal [this] (BigDecimal. this))

  java.lang.Integer
  (decimal [this] (BigDecimal. this))

  Number
  (decimal [this] (BigDecimal/valueOf this)))

(defmulti message (fn [exchange msg] [exchange (get msg :type)]))

(defmethod message [:gdax nil]
  [_ {type "type"
      product "product_id"
      bids "bids"
      asks "asks"
      changes "changes"
      :as msg}]
  (letfn [(ticker-pair [pstr]
            (-> pstr
                (string/split #"-")
                (->>
                 (map string/lower-case)
                 (map keyword))))

          (decimals
            ([side price size]
             [(case side
                ("buy") :buy
                ("sell") :sell)
              (decimal price)
              (decimal size)])
            ([price size]
             [(decimal price)
              (decimal size)]))]
    (case type
      ("snapshot") {:type :snapshot
                    :commodity (first (ticker-pair product))
                    :currency (second (ticker-pair product))
                    :bids (map #(apply decimals %) bids)
                    :asks (map #(apply decimals %) asks)}

      ("l2update") {:type :update
                    :commodity (first (ticker-pair product))
                    :currency (second (ticker-pair product))
                    :changes (map #(apply decimals %) changes)}
      ;; default
      {:type :unknown
       :msg msg})))

(defmethod message [:bitfinex nil]
  [_ m]
  (let [{type "event" channel "chanId" pair "pair"}
        m

        channel
        (or channel
            (first m))

        payload
        (second m)

        multi-payload?
        (sequential?
         (first payload))

        heartbeat?
        (= payload "hb")

        event?
        type

        snapshot?
        multi-payload?

        update?
        (not
         (or event?
             heartbeat?
             snapshot?))]
    (cond
      event?
      (message :bitfinex
               (merge m {:type (keyword type)
                         :channel channel
                         :pair pair}))

      heartbeat?
      (message :bitfinex
               {:type :heartbeat
                :channel channel})

      snapshot?
      (message :bitfinex
               {:type :snapshot
                :channel channel
                :payload payload})

      update?
      (message :bitfinex
               {:type :update
                :channel channel
                :payload payload}))))


(defmethod message [:bitfinex :subscribed] [_ {:keys [pair]
                                               :as m}]
  (letfn [(pair->ticker-pair [s]
            (case (string/lower-case s)
              ;; best hardcode pairs of interest
              "ethusd" {:commodity :eth :currency :usd}
              "etheur" {:commodity :eth :currency :eur}
              ;; crude guess
              {:commodity (keyword (subs "ethusd" 0 3))
               :currency (keyword (subs "ethusd" 3))}))]
    (-> m
        (merge (pair->ticker-pair pair)))))

(defmethod message [:bitfinex :info] [_ m] m)

(defmethod message [:bitfinex :heartbeat] [_ m] m)

(defmethod message [:bitfinex :snapshot] [_ {:keys [channel payload]
                                             :as m}]
  (let [{:keys [bids asks]}
        (group-by (fn [[_ _ side]]
                    (if (neg? side)
                      :asks
                      :bids))
                  payload)

        bids
        (map (fn [[price _ size]]
               [(decimal price)
                (decimal size)])
             bids)

        asks
        (map (fn [[price _ size]]
               [(decimal price)
                (decimal (- 0 size))])
             asks)

        ;; We expect to find ticker for the channel, since it would've been
        ;; inserted by dispatching on the earlier "subscribed" message. This may
        ;; not work out if Aleph's introduces a race condition e.g. "subscribed"
        ;; message hasn't been dispatched and we already dealing with the
        ;; snapshot message.

        ticker
        (some-> @bitfinex*
                (get :channel->ticker)
                (get channel))

        {:keys [commodity currency]}
        (ticker-pair ticker)]
    (-> m
        (assoc :bids bids)
        (assoc :asks asks)
        (assoc :commodity commodity)
        (assoc :currency currency)
        ;; (dissoc :payload)
        )))

(defmethod message [:bitfinex :update] [_ {:keys [channel payload]
                                           :as m}]
  (let [[price count size]
        payload

        payload
        ;; [side price amount]
        [(if (neg? size) :sell :buy)
         (decimal price)
         (decimal
          (cond
            (zero? count) count
            (neg? size) (- 0 size)
            (not (neg? size)) size))]

        ticker
        (some-> @bitfinex*
                (get :channel->ticker)
                (get channel))

        {:keys [commodity currency]}
        (ticker-pair ticker)]
    (-> m
        (assoc :changes [payload])
        (assoc :commodity commodity)
        (assoc :currency currency)
        ;; (dissoc :payload)
        )))

;;* Consume

(defmulti dispatch (fn [exchange {message :type}] [exchange message]))

(defmethod dispatch [:gdax :snapshot]
  [_ m]
  (swap! gdax*
         assoc
         (ticker m)
         (snapshot->book m)))

(defmethod dispatch [:gdax :update]
  [_ m]
  (let [ticker (ticker m)
        book (or (get @gdax* ticker)
                 (empty-book ticker))]
    (swap! gdax*
           assoc
           ticker
           (update book m))))

(defmethod dispatch [:bitfinex :subscribed]
  [_ {:keys [channel
             commodity
             currency]
      :as m}]
  (let [ticker (ticker m)]
    (swap! bitfinex*
           #(-> %
                (assoc-in [:channel->ticker channel] ticker)
                (assoc-in [:ticker->channel ticker] channel)))))

(defmethod dispatch [:bitfinex :snapshot]
  [_ m]
  (swap! bitfinex*
         assoc
         (ticker m)
         (snapshot->book m)))

(defmethod dispatch [:bitfinex :update]
  [_ m]
  (let [ticker (ticker m)
        book (or (get @bitfinex* ticker)
                 (empty-book ticker))]
    (swap! bitfinex*
           assoc
           ticker
           (update book m))))

(defmethod dispatch [:bitfinex :info] [_ _]
  (println "Skipping Info message"))

(defmethod dispatch [:bitfinex :heartbeat] [_ _]
  (println "Skipping heartbeat message"))

(defmethod dispatch :default [_ m]
  ;; (println "Unknown dispatch")
  )

;;* System

;; TODO dynamic is only per thread, so yeah, this is temporary solution for tests.
;; DO NOT FORGET TO FIX THIS!

(def ^:dynamic gdax*
  (atom {}))

(def ^:dynamic bitfinex*
  (atom {:channel->ticker {}
         :ticker->channel {}}))

(defn spawn-exchange [exchange-key
                      & {msg :msg}]
  (let [thread-control-stream
        (s/stream)

        thread
        (Thread.
         (fn []
           (let [stream (do
                          (connect exchange-key)
                          (subscribe exchange-key))]
             (s/consume (fn [msg]
                          (->> msg
                               json/decode
                               (message exchange-key)
                               (dispatch exchange-key)))
                        stream)
             (let [kill-msg @(s/take! thread-control-stream)]
               (println
                (format "Killing %s thread" exchange-key))
               (s/close! thread-control-stream)
               (println "Closed thread control stream.")
               (s/close! stream)
               (println "Closed exchange stream.")))))]
    (.start thread)
    {:thread thread
     :control thread-control-stream}))

(defn spawn-arbitrager [book-ref ticker]
  (let [thread-control-stream
        (s/stream)

        watcher
        (gensym "watcher")

        arbitrage
        (fn arbitrage [arbitrager-ref]
          (loop []
            (let [{:keys [bids asks]}
                  (ticker @book-ref)

                  signal
                  (doall
                   [(some->> bids vals (apply +))
                    (some->> asks vals (apply +))])]
              (case @arbitrager-ref
                :late (do (println "late")
                          (flush)
                          (reset! arbitrager-ref nil)
                          (recur))
                :kill (println "Arbitrager stopped!")
                ;; else report signal and loop
                (do (println "signal")
                    (flush)
                    (reset! arbitrager-ref nil)
                    (recur))))))

        thread
        (Thread.
         (fn []
           (let [arbitrager-ref (atom :arbitrage)]
             ;; compute signal on separate thread
             (future (arbitrage arbitrager-ref))
             ;; notify arbitrager of late computations
             (add-watch book-ref :arbitrager
                        (fn [_ _ _ _]
                          (swap! arbitrager-ref
                                 (fn [v]
                                   (if (= :kill v)
                                     v
                                     :late)))))
             (loop []
               (case @(s/take! thread-control-stream)
                 (do
                   (println "Killing arbitrager thread")
                   (reset! arbitrager-ref :kill)
                   (remove-watch book-ref :arbitrager)
                   (s/close! thread-control-stream)
                   (println "Closed thread control stream.")))))))]
    (.start thread)
    {:thread thread
     :control thread-control-stream}))

#_(def gdax (spawn-exchange :gdax))
#_(deref gdax*)
#_(s/put! (:control gdax) 'done)

#_(def arbitrager (spawn-arbitrager gdax* :usd/eth))
#_(s/put! (:control arbitrager) :kill)

#_(def bitfinex (spawn-exchange :bitfinex))
#_(deref bitfinex*)
#_(s/put! (:control bitfinex) 'done)

(defn printer [stream & {print-fn :print
                         map-fn :map
                         filter-fn :filter
                         :or {print-fn pprint
                              filter-fn identity}}]
  (let [sink (s/stream)]
    (->> sink
         (s/filter filter-fn)
         (s/map map-fn)
         (s/consume print-fn))
    (s/connect stream
               sink
               {:description "attached: printer"})
    ;; create an intermediate stream as a custodian of any operations downstream
    ;; and return it. Cool thing is that now the caller can simply close that
    ;; sink, which in theory closes and eventually GSs everything everything we
    ;; connected to the original stream.
    sink))

;;* Tests

(defmacro with-book [[book-name book-atom-var] &
                     {before :before
                      test :test
                      after :after
                      :as args}]
  `(binding [~book-atom-var
             (if-let [before# ~before]
               (atom before#)
               ~book-atom-var)]
     ;; capture book-name
     (let [~book-name ~book-atom-var]
       ~test
       (is (=
            (-> ~book-atom-var
                (deref)
                (select-keys (keys ~after))
                (doall))
            (doall ~after))))))

(deftest gdax

  (let [snapshot-msg {"type" "snapshot"
                      "product_id" "BTC-EUR"
                      "bids" [["6500.11" "0.45054140"]]
                      "asks" [["6500.15" "0.57753524"]
                              ["6504.38" "0.5"]]}

        update-msg {"type" "l2update"
                    "product_id" "BTC-EUR"
                    "changes"
                    [["buy" "6500.09" "0.84702376"]
                     ["sell" "6507.00" "1.88933140"]
                     ["sell" "6505.54" "1.12386524"]
                     ["sell" "6504.38" "0"]]}]

    (is (= (message :gdax snapshot-msg)
           {:type :snapshot
            :commodity :btc
            :currency :eur
            :bids [[6500.11M 0.45054140M]]
            :asks [[6500.15M 0.57753524M]
                   [6504.38M 0.5M]]})
        "message: normalize snapshot")

    (is (= (message :gdax update-msg)
           {:type :update
            :commodity :btc
            :currency :eur
            :changes
            [[:buy 6500.09M 0.84702376M]
             [:sell 6507.00M 1.88933140M]
             [:sell 6505.54M 1.12386524M]
             [:sell 6504.38M 0M]]})
        "message: normalize update")

    (is (= (snapshot->book
            (message :gdax snapshot-msg))

           (map->Book
            {:ticker :eur/btc
             :asks {6500.15M 0.57753524M
                    6504.38M 0.5M}
             :bids {6500.11M 0.45054140M}}))
        "book: create from snapshot message")

    (is (= (update (snapshot->book
                    (message :gdax snapshot-msg))
                   (message :gdax update-msg))
           (map->Book
            {:ticker :eur/btc
             :asks {6500.15M 0.57753524M
                    6505.54M 1.12386524M
                    6507.00M 1.88933140M}
             :bids {6500.11M 0.45054140M
                    6500.09M 0.84702376M}}))
        "book: update from update message ")))

(deftest bitfinex
  (let [info {"event" "info"}
        subscribed {"event" "subscribed"
                    "chanId" 10961
                    "pair" "ETHUSD"}
        snapshot '(10961
                   [[584.58 11 65.64632441]
                    [584.51 1 0.93194317]
                    [584.59 4 -23.39216286]
                    [584.96 1 -7.23746288]
                    [584.97 1 -12.3]])
        hb '(10961 "hb")
        ;; message (partial message :bitfinex)
        ;; dispatch (partial dispatch :bitfinex)
        ]

    (testing "message"

      (binding [bitfinex* (atom
                           {:channel->ticker {10961 :usd/eth}
                            :ticker->channel {:usd/eth 10961}})]

        (is (= (-> (message :bitfinex info)
                   :type)

               :info))

        (is (= (-> (message :bitfinex subscribed)
                   (select-keys [:type
                                 :channel
                                 :pair
                                 :commodity
                                 :currency]))

               {:type :subscribed,
                :channel 10961,
                :pair "ETHUSD",
                :commodity :eth,
                :currency :usd}))

        (is (= (-> (message :bitfinex snapshot)
                   (select-keys [:bids :asks]))

               '{:bids ([584.58M 65.64632441M]
                        [584.51M 0.93194317M])
                 :asks ([584.59M 23.39216286M]
                        [584.96M 7.23746288M]
                        [584.97M 12.3M])}))

        (is (= (-> (message :bitfinex hb)
                   :type)

               :heartbeat))))

    (testing "dispatch"

      (with-book [b bitfinex*]
        :test (dispatch :bitfinex (message :bitfinex info))
        :after (deref b))

      (with-book [b bitfinex*]
        :test (dispatch :bitfinex (message :bitfinex subscribed))
        :after {:channel->ticker {10961 :usd/eth}
                :ticker->channel {:usd/eth 10961}})

      (with-book [b bitfinex*]
        :before {:channel->ticker {10961 :usd/eth}
                 :ticker->channel {:usd/eth 10961}}
        :test (dispatch :bitfinex
                        (message :bitfinex snapshot))
        :after {:usd/eth
                (map->Book
                 {:ticker :usd/eth,
                  :asks
                  {584.59M 23.39216286M, 584.96M 7.23746288M, 584.97M 12.3M},
                  :bids {584.58M 65.64632441M, 584.51M 0.93194317M}})})

      (with-book [b bitfinex*]
        :before {:channel->ticker {10961 :usd/eth}
                 :ticker->channel {:usd/eth 10961}
                 :usd/eth
                 (map->Book
                  {:ticker :usd/eth,
                   :asks {584.59M 23.39216286M
                          584.96M 7.23746288M
                          584.97M 12.3M}
                   :bids {584.58M 65.64632441M
                          584.51M 0.93194317M}})}
        :test (do
                (dispatch :bitfinex (message :bitfinex '(10961 [583.75 1 1])))
                (dispatch :bitfinex (message :bitfinex '(10961 [584.97 0 -1])))
                (dispatch :bitfinex (message :bitfinex '(10961 [584.59 4 -23.34100906])))
                (dispatch :bitfinex (message :bitfinex '(10961 [586.94 1 -29.9])))
                (dispatch :bitfinex (message :bitfinex '(10961 [583.75 0 1]))))
        :after {:usd/eth
                (map->Book
                 {:ticker :usd/eth
                  :asks {584.59M 23.34100906M
                         584.96M 7.23746288M
                         586.94M 29.9M}
                  :bids {584.58M 65.64632441M
                         584.51M 0.93194317M}})})))
  ;; end
  )

;;* Main

#_
(defn -main []
  (let [socket (gdax)]
    (repl/set-break-handler! (fn []
                               (println "Closing socket")
                               (ws/close socket)
                               (println "Closed!")))))
