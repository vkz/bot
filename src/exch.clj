(ns exch
  (:require [medley.core :refer :all]
            [clojure.spec.alpha :as spec]
            ;; aleph HTTPS client is broken for some HTTPS pages e.g. for GDAX
            ;; REST API
            [aleph.http :as http]
            ;; so we use clj-http instead
            [clj-http.client :as http-client]
            [manifold.deferred :as d]
            [manifold.stream :as s]
            [byte-streams :as bs]
            [cheshire.core :as json]
            [clojure.string :as string]
            [clojure.pprint :refer [pprint]]
            [clojure.core.async :as a]
            [clojure.core.async.impl.protocols :refer [Channel]]
            [taoensso.timbre :as log])

  (:import [java.text DateFormat SimpleDateFormat]
           [java.util Date]))

(defn timestamp
  ([ts]
   (.format
     (SimpleDateFormat. "HH:mm:ss:SSS")
     ts))
  ([]
   (timestamp (Date.))))

(comment
  (timestamp 1511545528111)
  (timestamp))

(defprotocol Money
  (decimal [this]))

(extend-protocol Money
  String
  (decimal [this] (BigDecimal. this))

  java.lang.Integer
  (decimal [this] (BigDecimal. this))

  Number
  (decimal [this] (BigDecimal/valueOf this)))

;;* Ticker

(defrecord Ticker [base quote])

(defprotocol ITicker
  (ticker [t])
  (base [t])
  (commodity [t])
  (currency [t]))

(extend-type clojure.lang.Keyword

    ITicker

    (ticker [k]
      (let [base (name k)
            currency (namespace k)
            kw (comp keyword
                     string/lower-case)]

        (if (and base
                 currency)

          (map->Ticker
            {:base (kw base)
             :quote (kw currency)})

          ;; TODO validate against all known pairs
          (throw
            (ex-info
              "Malformed ticker"
              {:ticker k})))))

    (base [k]
      (:base (ticker k)))

    (currency [k]
      (:quote (ticker k)))

    (commodity [k]
      (base k)))

#_
((juxt ticker base commodity currency) :btc/eth)

;;* Book

(defrecord OrderBook
    [ticker
     bids
     asks])

;;* Proc

;; TODO could be worth having a pub for each process where you can substribe to
;; peek at the result of each iteration. With no subscribers it will just sit
;; there.
(defrecord Proc
    [in
     cmd
     ret
     timeout
     api])

#_
(defprotocol IProc
  (send [p])
  (command [p])
  (kill [p])
  (subscribe ([p]) ([p timeframe]))
  ;; ----------------------------
  (take [p n])
  (take-while []))

;; Process
;; - reads msgs from in, which can be a channel or a thunk that returns a channel,
;; - reads tick from timout, which can be a channel or a thunk that returns a
;; channel,
;; - reads commands from cmd channel,
;; - puts the value of final expression in its body into ret channel,
;; - api a map of :command => (fn [& args] ...)
;;
;; Upon launch process starts a loop [in cmd timeout] where
;; - it (alts!! (in) cmd (timeout)) and conds on the channel
;; -- if in-ch it executes (body-fn msg) with msg from in-ch and recurs,
;; -- if cmd it invokes relevant cmd and recurs unless :kill, then it exits,
;; -- if timeout it exits.
;; - when done with the loop it logs reason for exit,
;; - returns

(defn create-proc
  [& {:keys [name in cmd timeout api body done]
      :as opts
      :or {timeout (* 30 1000)}}]
  (let [proc
        (atom nil)

        api
        ;; every command receives proc as its first argument
        (merge
          {:kill (fn kill [proc & args] "[kill]")}
          api)

        in-fn
        (cond
          (fn? in)
          in

          (satisfies? Channel in)
          (constantly in)

          :else
          (throw
            (ex-info
              "Expected channel or thunk."
              {:in in})))

        timeout-fn
        (cond

          (number? timeout)
          (fn [] (a/timeout timeout))

          (fn? timeout)
          timeout

          (satisfies? Channel timeout)
          (constantly timeout))

        cmd
        (or cmd (a/chan))

        ret-ch
        ;; TODO (Thread. thunk "Thread name") this way I at least have access to
        ;; my thread and can spot it among others by name.
        (a/thread
          (try
            (let [reason
                  (loop [in (in-fn)
                         cmd cmd
                         timeout (timeout-fn)]
                    (let [[v ch]
                          (a/alts!!
                            [in cmd timeout])]
                      (condp = ch
                        in (do
                             (body v)
                             (recur (in-fn)
                                    cmd
                                    (timeout-fn)))

                        cmd (let [[c args] v
                                  command (api c)
                                  kill? (= c :kill)]

                              (cond

                                kill?
                                (apply command @proc args)

                                command
                                (do
                                  (apply command @proc args)
                                  (recur (in-fn) cmd (timeout-fn)))

                                :unknown-command
                                (do
                                  (log/error
                                    (str "Proc " @proc
                                         " received unrecognized command " c))
                                  (recur (in-fn) cmd (timeout-fn)))))

                        timeout "[timeout]")))]
              (log/info
                (format "Shutting down process %s because %s."
                        @proc
                        reason))

              (done reason))
            (catch
                java.lang.Throwable
                ex
                (log/error
                  "Exception in proc " @proc
                  (ex-info (ex-message ex)
                           {}
                           (ex-cause ex)))
                (throw ex))))]

    (reset! proc
            (map->Proc
              {:in in
               :cmd cmd
               :ret ret-ch
               :timeout timeout
               :api api}))
    (fn get-proc []
      (deref proc))))

(defmacro let-proc
  ([bindings body]
   `(let-proc ~bindings
      ~body
      (done [reason#] (log/info "Process stopped because " reason#))))
  ([bindings body done]
   (let [bindings
         (->> bindings
              (apply hash-map)
              (map-keys keyword))

         [do? [msg] & body]
         body

         [done? [reason] done]
         done

         opts
         (-> bindings
             (assoc :body `(fn [~msg] ~@body))
             (assoc :done `(fn [~reason] ~done)))]
     ;; check do?
     ;; check done?
     ;; check bindings are what we expect
     `(create-proc ~@(mapcat identity opts)))))

(comment

  (def p
    (create-proc
      :api
      {:log (fn [] (log/info "log"))}
      :done
      (fn [reason] (println "done because " reason))
      :timeout
      (* 30 1000)
      :body
      (fn [msg] (println "received " msg))
      :in
      (a/chan)))

  (def p
    (let-proc [in (a/chan)
               timeout (* 30 1000)
               api {:log (fn [] (log/info "log"))}]
      (do [msg] (println "received " msg))
      (done [reason] (println "done because " reason))))

  (a/put! (:cmd (p)) [:kill])
  (a/go
    (println (a/<! (:ret (p))))))

(comment
  ;; Proc examples

  (def-proc eth-btc-proc [msg]
    (let-proc [in (a/chan)              ;or e.g. (fn [] (a/timeout 2000))
               cmd (a/chan)
               api {}
               timeout (* 60 1000)]
      (update order-book msg)))

  (def-proc eth-btc-poll-proc [msg]
    (let-proc [in (a/chan)
               cmd (a/chan)
               api {}
               timeout (* 60 1000)]
      (update order-book msg)))

  (def-proc hb-proc [hb]
    (let-proc [in (a/chan)
               cmd (a/chan)
               api {}
               timeout (* 60 1000)]
      (log/info "Heartbeat")))

  ;; "global" timer to subscribe to
  (let [timer-ch (a/chan)
        pub (pub timer-ch identity)]
    {:proc (def-proc timer-proc [t]
             (let-proc [in (fn [] (a/timeout 2000))]
               (log/info "tick")
               (>! timer-ch :tick)))

     :pub-timer pub})

  ;; one-off timer proc
  (defn fresh-timer-proc [period]
    (let [timer-ch (a/chan)]
      (def-proc timer-proc [t]
        (let-proc [in (fn [] (a/timeout 2000))]
          (log/info "tick")
          (>! timer-ch :tick)))))

  ;; effectively a (while true ...) proc
  (let [const-ch (a/chan)]
    ;; TODO I suspect this requires priority in alts else (in) will always win but
    ;; we want cmd to have the chance to break process.
    (let-proc [in (fn [] (a/put! 1))]
      (do [msg] something)
      (done [reason] done)))

  ;; no way to shutdown
  (defn create-polling-proc [period queue]
    (future
      (while true
        (Thread/sleep period)
        (when-let [msg (pop queue)]
          (send msg))))))

(defrecord Connection
    [in
     out
     pub
     transform-in
     transform-out
     proc])

(def ^:private CONNECTION (atom nil))

;;* WS

(defn connect
  [& {in :in
      out :out
      transform-in :transform-in
      transform-out :transform-out
      :or {in (a/chan)
           out (a/chan)
           transform-in identity
           transform-out identity}
      :as conn}]

  (if (some-> @CONNECTION
              (:proc)
              ;; TODO generic closed?
              (s/closed?)
              (not))

    ;; connection already exists
    (fn [] (deref CONNECTION))

    ;; fresh connection
    (let [connection-stream
          ;; NOTE the 2 connect-via below work only because this here returns a
          ;; DUPLEX stream. Regular stream would act an async channel, so the two
          ;; connections below would deadlock. (s/splice sink source) creates a
          ;; duplex stream.
          @(http/websocket-client
             ;; TODO abstract connection endpoint
             "wss://ws-feed.gdax.com"
             {:max-frame-payload 1e6
              :max-frame-size 1e6})

          in-stream
          ;; sink of msgs from exchange for users to consume
          (s/->sink in)

          out-stream
          ;; source of user msgs to send to exchange
          (s/->source out)

          _
          ;; exchange <= transform-out <= out <= user
          (s/connect-via out-stream
                         (fn [msg]
                           ;; TODO validate msg properly
                           (if (or (get msg :type)
                                   (get msg "type"))
                             (s/put! connection-stream
                                     (json/encode
                                       (transform-out msg)))
                             (let [err (d/success-deferred "Malformed msg")]
                               (log/error (ex-info "Malformed msg"
                                                   {:msg msg}))
                               err)))
                         connection-stream)

          _
          ;; exchange => transform-in => in => user
          (s/connect-via connection-stream
                         (fn [msg]
                           (let [msg (transform-in
                                       (json/decode msg))]
                             (if (or (get msg :type)
                                     (get msg "type"))
                               (s/put! in-stream msg)
                               (let [err (d/success-deferred "Maslformed msg")]
                                 (log/error
                                   (ex-info "Malformed msg"
                                            {:msg msg}))
                                 err))))
                         in-stream)

          pub
          (a/pub
            in
            (fn [msg]
              (select-keys msg
                           [:type
                            :ticker])))]

      (reset! CONNECTION (map->Connection
                           {:in in
                            :out out
                            :pub pub
                            :transform-in transform-in
                            :transform-out transform-out
                            ;; for REST it should be a thread?
                            :proc connection-stream}))
      (fn [] (deref CONNECTION)))))

#_
(defn orderbook-derefer
  [ticker]
  (fn orderbook []
    (-> @BOOKS
        ticker
        ;; (agent OrderBook)
        deref)))

#_
(defn subscribe
  [{in :ch
    ticker :ticker
    :as book}]

  (when-not (subscribed? book)
    (let [conn (connect)
          pub (:pub (conn))
          snapshot {:type :snapshot :ticker ticker}
          updates {:type :update :ticker ticker}
          agent (swap! BOOKS assoc ticker (agent book))
          proc (let-proc [in in]
                 (do [msg]
                     (send agent update-book msg)))]

      (a/sub pub snapshots in)
      (a/sub pub updates in)

      (a/put! (:out (conn))
              {:type :subscribe
               :product (product ticker)
               :topic :update})

      (send agent assoc :proc proc)))

  (orderbook-derefer ticker))

(comment

  (def conn
    (connect :transform-in
             (fn [msg]
               (println "Received: ")
               (pprint msg)
               msg)

             :transform-out
             (fn [msg]
               (println "Sending: ")
               (pprint msg)
               msg)))

  ;; subscribe hb
  (a/put! (:out (conn)) {"type" "subscribe"
                         "product_ids" ["ETH-BTC"]
                         "channels" ["heartbeat"]})
  ;; unsubscribe hb
  (a/put! (:out (conn)) {"type" "unsubscribe"
                         "product_ids" ["ETH-BTC"]
                         "channels" ["heartbeat"]})

  (a/put! (:out (conn)) {:a 1})
  (s/close! (:proc (conn))))

;;* REST

(defn get-book [ticker]
  (let [url (str
              "https://api.gdax.com"
              (format
                "/products/%s/book?level=2"
                ticker))]
    (-> url
        ;; TODO should it be async?
        http-client/get
        :body
        json/decode)))

(defmulti send-msg :type)
(defmethod send-msg :book [{ticker :ticker}]
  (get-book ticker))

(def QUEUE (atom (queue)))

;; TODO can I just extend some protocol to have conj, peek, pop work for an (atom
;; queue)?

(defn conjq! [v]
  (swap! QUEUE conj v))

(defn popq! []
  (when (peek @QUEUE)
    (first
      (deref-swap! QUEUE pop))))

(defn create-polling-proc [stream period]
  ;; TODO make it local to this proc, expose only conj, pop, count or some such.
  (reset! QUEUE (queue))
  (let-proc [in (fn [] (a/timeout period))]
    (do [_]
        (let [len (count @QUEUE)]
          (when (> len 1)
            (log/info "Queue count: " len)))
        (when-let [msg (popq!)]
          (s/put! stream (send-msg msg))))))

(defn connect-rest
  [& {in :in
      out :out
      transform-in :transform-in
      transform-out :transform-out
      :or {in (a/chan)
           out (a/chan)
           transform-in identity
           transform-out identity}
      :as conn}]

  (let [exchange-stream
        (s/stream)

        poller
        (create-polling-proc exchange-stream 5000)

        in-stream
        ;; sink of msgs from exchange for users to consume
        (s/->sink in)

        out-stream
        ;; source of user msgs to send to exchange
        (s/->source out)

        _
        ;; exchange <= transform-out <= out <= user
        (s/consume (fn [msg]
                     (conjq! (transform-out msg)))
                   out-stream)

        _
        ;; exchange => transform-in => in => user
        (s/connect-via exchange-stream
                       (fn [msg]
                         (s/put! in-stream
                                 (transform-in msg)))
                       in-stream)

        pub
        (a/pub
          in
          (fn [msg]
            (select-keys msg
                         [:type
                          :ticker])))]

    (reset! CONNECTION (map->Connection
                         {:in in
                          :out out
                          :pub pub
                          :transform-in transform-in
                          :transform-out transform-out
                          :proc poller}))
    (fn [] (deref CONNECTION))))

(defn subscribe-rest [ticker period]
  (let-proc [in (fn [] (a/timeout period))]
    (do [_]
        (conjq! {:type :book
                 :ticker ticker}))))

(comment

  (def conn
    (connect-rest
      :transform-in
      (fn [msg]
        (println "Received: ")
        (pprint msg)
        msg)

      :transform-out
      (fn [msg]
        (println "Sending: ")
        (pprint msg)
        msg)))

  (a/put! (:out (conn)) {:type :book
                         :ticker "BTC-USD"})

  (a/put! (:cmd ((:proc (conn)))) [:kill])

  (a/go (pprint
          (a/<! (:cmd ((:proc (conn)))))))

  (deref QUEUE)

  (def btc-usd-rest
    (subscribe-rest "BTC-USD" 3000))

  (a/put! (:cmd (btc-usd-rest)) [:kill])

  ;; end
  )


#_
(defmethod disconnect :gdax
  [exch]

  (swap! at-connections disj exch)

  (s/close! (:process exch))
  (a/close! (:in exch))
  (a/close! (:out exch))

  (doseq [sub (:subscribers exch)]
    (close sub))

  (-> exch
      (assoc :in nil)
      (assoc :out nil)
      (assoc :process nil)))

(comment

  (defn print-threads [& {:keys [headers pre-fn]
                          :or {pre-fn identity}}]
    (let [thread-set (keys (Thread/getAllStackTraces))
          thread-data (mapv bean thread-set)
          headers (or headers (-> thread-data first keys))]
      (clojure.pprint/print-table headers (pre-fn thread-data))))

  (defn print-threads-str [& args]
    (with-out-str (apply print-threads args)))
  ;;print all properties for all threads
  (print-threads)

  ;;print name,state,alive,daemon properties for all threads
  (print-threads :headers [:name :state :alive :daemon])

  ;;print name,state,alive,daemon properties for non-daemon threads
  (print-threads :headers [:name :state :alive :daemon] :pre-fn (partial filter #(false? (:daemon %))))

  ;;log name,state,alive,daemon properties for non-daemon threads
  (log/debug (print-threads-str :headers [:name :state :alive :daemon] :pre-fn (partial filter #(false? (:daemon %))))))

;;* Design
(comment

  (defn connect
    "Establish connection with the exchange.
  WS API
  - connection is persistent,
  - we subscribe to the heartbeat messages,
  - received messages are transformed to a unified schema,
  - we create a pub from in-ch with a topic-fn for unified schema,
  - in-ch, pub, heartbeat subscriber added to the state.

  REST API
  - we send a test msg and verify exch server response,
  - start a \"polling\" process with period T,
  - polling proccess has in-ch where it puts msgs received from exchange,
  - polling process has queue from which it sends msg every T sec,
  - polling process has pub from in-ch
  - we subscribe to heartbeat msgs (proc that adds HB msg to polling queue every 10sec)
  - received messages are transformed to a unified schema,
  - we create a pub from channel with a topic-fn for unified schema,
  - in-ch, queue, pub, heartbeat subscriber added to the state.

  Return
  - true on success,
  - nil on failure.

  Connect is idempotent: it should check if connection is already open."
    [])

  (defn close
    "
  (unsub-all (:pub Connection))
  Close (:in Connection)
  Close (:out Connection)
  Stop (:proc Connection)
  Reset CONN"
    [Connection])

  (defn create-orderbook
    "
  If OrderBook record for this ticker exists in state, use that.

  Else add fresh OrderBook for ticker with channel ch to state.
  Create process that updates OrderBook atom every time it gets a msg on ch.
  Set proc field in OrderBook.

  Return (orderbook-derefer ticker).
  "
    ([ticker])
    ;; custom channel e.g. backed by NoisySlidingBuffer
    ([ticker ch]))

  (defn create-heartbeat
    "
  If HEARTBEAT use that.

  Else add fresh HeartBeat with channel ch to HEARTBEAT.
  Create process that updates timestamp in HeartBeat atom every item it gets a mesg on ch.
  Set proc field in HeartBeat.

  Return (heartbeat-derefer).")

  (defn close
    "
  Close (:ch OrderBook).
  Remove OrderBook from BOOKS.
  "
    [OrderBook])

  (defn close
    "
  Close (:ch HeartBeat).
  Reset HEARTBEAT."
    [HeartBeat])

  (defn subscribe
    "Subscribe OrderBook for updates from Exchange:

  WS API
  - send request for orderbook updates to exch,
  - (sub pub :topic (:ch OrderBook)),
  - add subscriber to state.

  REST API
  - Create proc that adds orderbook update request to polling q every T sec,
  - (sub pub :topic (:ch OrderBook))
  - add subscriber to state."
    ([OrderBook])
    ([OrderBook T]))

  (defn send
    ""
    [msg])

  (defn unsubscribe
    "
  WS
  - send request to stop orderbook updates to exch,
  - (unsub pub :topic (:ch OrderBook))
  - remove subscriber from state.

  REST
  - stop proc that enqueues orderbook update requests,
  - (unsub pub :topic (:ch OrderBook)),
  - remove subscriber from state."
    [OrderBook])

  (defn subscribe
    "
  WS API
  - send :hb subscribtion message to exch,
  - (sub pub :hb (:ch HeartBeat)),
  - add subscriber to state.

  REST API
  - Create proc that adds HB msg to the polling queue every N sec,
  - (sub pub :hb (:ch HeartBeat)),
  - add subscriber to state."
    [HeartBeat])

  (defn unsubscribe
    "
  WS API
  - send :hb unsubscribe msg to exch,
  - (unsub-all pub :hb),
  - remove subscriber from state.

  REST API
  - stop proc that queues HB msgs,
  - (unsub pub :hb (:ch HeartBeat))
  - remove subscriber from state."
    [HeartBeat]))
