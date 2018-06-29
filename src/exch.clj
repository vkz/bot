(ns exch
  (:require [medley.core :refer :all]
            [clojure.spec.alpha :as spec]
            [clojure.spec.gen.alpha :as gen]
            [clojure.spec.test.alpha :as stest]
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
            [taoensso.timbre :as log]

            [clojure.test
             :refer
             [deftest with-test is are testing use-fixtures]])

  (:import [java.text DateFormat SimpleDateFormat]
           [java.util Date]
           [java.time Instant]))


;; GDAX uses zoned TS with microseconds which old classes like SimpleDateFormat
;; DateFormat and Date don't really handle. We need to use a more modern java.time
;; library that ships with Jave 8 and above. E.g. this parses GDAX ISO 8601 ts
;; without a problem:
;;
;; (Instant/parse "2014-11-07T22:19:28.578544Z")
;;
;; TODO Maybe worth switching to java.time all time work?
;; https://docs.oracle.com/javase/9/docs/api/java/time/package-summary.html


;;* Utils
(defn conj-some
  "Conj a value to a vector, if and only if the value is not nil."
  ([v val]
   (if (nil? val) v (conj v val)))
  ([v val & vals]
   (reduce conj-some
           (conj-some v val)
           vals)))

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

;;* Money
(defprotocol Money
  (decimal [this])
  (decimal-with-precision [this precision]))

(extend-protocol Money
  String
  (decimal [this]
    (BigDecimal. this))
  (decimal-with-precision
    [this precision]
    (BigDecimal. this (java.math.MathContext. precision)))

  java.lang.Integer
  (decimal [this]
    (BigDecimal. this))
  (decimal-with-precision
    [this precision]
    (BigDecimal. this (java.math.MathContext. precision)))

  Number
  (decimal [this]
    (decimal (str this)))
  (decimal-with-precision [this precision]
    (decimal-with-precision (str this) precision)))

;;* Ticker

(defprotocol ITicker
  (ticker [t])
  (base [t])
  (commodity [t])
  (currency [t])
  (ticker-kw [t]))

(defrecord Ticker [base quote]
  ITicker
  (ticker [t] t)
  (base [t] (:base t))
  (currency [t] (:quote t))
  (commodity [t] (:base t))
  (ticker-kw [t] (keyword (name (:quote t)) (name (:base t)))))

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
  (base [k] (:base (ticker k)))
  (currency [k] (:quote (ticker k)))
  (commodity [k] (base k))
  (ticker-kw [k] k))


#_((juxt ticker-kw ticker base commodity currency) :usd/btc)
#_((juxt ticker-kw ticker base commodity currency) (ticker :usd/btc))

;;* Connection

(defrecord Advice [path fn])
;; (Advice. [:before :incomming] (fn [conn msg]))
;; (Advice. [:after :incomming] (fn [conn msg]))
;; (Advice. [:before :outgoing] (fn [conn msg]))
;; (Advice. [:after :outgoing] (fn [conn msg]))

(spec/def ::defadvice-args
  (spec/cat :name symbol?
            :path (spec/* keyword?)
            :bindings vector?
            :body (spec/* (constantly true))))

(defmacro defadvice [& args]
  (let [{:keys [name path bindings body]}
        (spec/conform ::defadvice-args args)]
    `(def ~(symbol name)
       (Advice.
         ~path
         (fn ~bindings ~@body)))))

;; TODO spec/fdef for defadvice

#_
(defadvice print-advice :before :incomming [conn msg]
  (pprint "hello")
  msg)

#_((:fn print-advice) 'conn :foo)

(defprotocol ConnectionProtocol
  "Public methods that every exchange connection record must implement.
  Conn is a record, type or class that represents exchange connection.
  Every exchange is expected to define a Connection type that implements these
  methods."
  (connect [Conn]
    "Establish connection with an exchange performing any idiosyncratic setup.")
  (disconnect [Conn]
    "Break connection with an exchange performing any idiosyncratic setup.")
  (connected? [Conn]
    "Are we connected to the exchange?")
  (send-out [Conn msg]
    "Send :exch/msg to the exchange. We expect the msg to be converted to exchange
    specific format somewhere after send-out. Typically send-out would simply put
    an :exch/msg onto the (:out Connection) channel.")
  (send-in [Conn msg]
    "Send :exch/msg \"from\" the exchange. Typically it simply puts :exch/msg onto
    the (:in Connection) channel as if it was send by the exchange and converted
    to :exch/msg format. This method is intended for testing.")

  ;; json => exch/msg
  (convert-incomming-msg [Conn msg]
    "Convert JSON str sent by the exchange to :exch/msg format. Return :exch/msg.")
  ;; exch/msg => json
  (convert-outgoing-msg [Conn msg]
    "Convert :exch/msg to the msg format of the exchange. Return JSON string.")
  (conn-name [Conn]
    "Return connection name e.g. :bitfinex.")
  (advise [Conn advice])
  (unadvise [Conn advice])
  (apply-advice [Conn ad-path msg]))

;;* Book
(defn empty-book [ticker]
  {:ticker ticker
   :status false
   :asks (sorted-map-by <)
   :bids (sorted-map-by >)})

(defn snapshot->book
  [book snapshot]
  (-> book
      (assoc :asks (into (sorted-map-by <) (:asks snapshot)))
      (assoc :bids (into (sorted-map-by >) (:bids snapshot)))))

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

(defprotocol BookProtocol
  (-book-apply-snapshot [Book snapshot]
    "Update bids/asks from :exch/snapshot msg. Return Book.")
  (-book-apply-update [Book update]
    "Update bids/asks from :exch/update msg. Return Book.")
  (book-subscribed? [Book]
    "Is book subscribed to exchange L2 updates?")
  (book-toggle-status [Book] [Book status]
    "Toggle book subscribtion status (true | false) or set it to status.")
  (book-sub [Book]
    "Subscribed book to exchange L2 updates. Return Book.")
  (book-unsub [Book]
    "Unsubscribe book from exchange L2 updates. Return Book.")
  (book-snapshot [Book]
    "Return book snapshot where :bids and :asks are sorted best price to worse:

{:bids {highest_bid amount
        ...         ...}
 :asks {lowest_ask amount
        ...        ...}}")
  (book-watch [Book key callback]
    "Register a callback (fn [old_snapshot new_snapshot]) to be called every time
    the book receives an update. Return Book.")
  (book-unwatch [Book key]
    "Unregister a callback corresponding to the key from being called on book
    updates. Return Book."))

(defrecord Book [ticker conn agent]
  BookProtocol
  (-book-apply-snapshot [book snapshot] (send agent snapshot->book snapshot) book)
  (-book-apply-update [book update] (send agent update->book update) book)
  (book-subscribed? [book] (get @agent :status))
  (book-toggle-status [book] (send agent update :status not))
  (book-toggle-status [book status] (send agent assoc :status status))
  (book-sub [book] (when-not (book-subscribed? book) (send-out conn [:subscribe ticker])) book)
  (book-unsub [book] (when (book-subscribed? book) (send-out conn [:unsubscribe ticker])) book)
  (book-snapshot [book] @agent)
  (book-watch [book key callback]
    (add-watch
      agent
      key
      (fn [_ _ old-book new-book]
        (callback old-book new-book)))
    book)
  (book-unwatch [book key]
    (remove-watch agent key)
    book))

(defn create-book [conn ticker]
  (Book. ticker
         conn
         (agent (empty-book ticker))))

;;* State

(defprotocol StateProtocol
  "Private methods to manage the value of Exch state Atom - the State record. These
  methods work on the state value itself, not the ref.

  ExchProtocol works with the state Atom by calling StateProtocol methods to swap
  the Atom's current value."
  (-state-sub [this topic chan]
    "Add chan to topic subscribers. Subscribers as a map of topic => #{chan}.")
  (-state-unsub [this topic chan]
    "Remove chan from topic subscribers. Subscribers as a map of topic =>
    #{chan}.")
  (-state-get-book [this ticker]
    "Return the Book for ticker or nil if not in the State.")
  (-state-add-book [this book]
    "Add book to the State.")
  (-state-rm-book [this book]
    "Remove book from the State."))

;; books: {ticker => Book}
;; pub:   (async/pub (conn :in) topic-fn)
;; subs:  {topic => #{chan}}
(defrecord State [books pub subs]
  StateProtocol
  (-state-sub [state topic chan]
    (State. books
            pub
            (assoc subs
                   topic
                   (conj
                     (or (get subs topic) #{})
                     chan))))
  (-state-unsub [this topic chan]
    (State. books
            pub
            (assoc-some subs
                        topic
                        (disj
                          (get subs topic)
                          chan))))
  (-state-get-book [this ticker] (get books ticker))
  (-state-add-book [this book] (State. (assoc books (:ticker book) book) pub subs))
  (-state-rm-book [this book] (State. (dissoc books (:ticker book)) pub subs)))

;;* Exch

(defprotocol ExchProtocol
  (sub [Exch topic chan]
    "Duplicate incomming topic msgs to chan. Return Exch.")
  (unsub [Exch topic chan]
    "Stop duplicating incomming topic msgs to chan. Return Exch.")
  (add-book [Exch ticker]
    "Create and add a Book for ticker (e.g. (ticker :usd/btc)) to exch state.
If state already has a book for that ticker, do nothing. Return Exch.")
  (rm-book [Exch ticker]
    "Remove a Book for ticker from the Exch state. Return Exch.")
  (get-book [Exch ticker]
    "Return a Book associated with ticker. Create and add it to State if it doesn't exist.")
  (get-name [Exch]
    "Return Exch's name. E.g. :bitfinex."))

(defrecord Exch [conn state]
  ExchProtocol
  (sub [exch topic chan]
    (a/sub (:pub @state) topic chan)
    (swap! state -state-sub topic chan)
    exch)
  (unsub [exch topic chan]
    (swap! state -state-unsub topic chan)
    (a/unsub (:pub @state) topic chan)
    exch)
  (add-book [exch ticker]
    (when-not (-state-get-book @state ticker)
      (swap! state -state-add-book (create-book conn ticker)))
    exch)
  (rm-book [exch ticker]
    ;; TODO if book doesn't exist get-book will create one to be immediately
    ;; removed. I wonder if this is ok? Is there a chance of race with another
    ;; thread that does get-book concurrently?
    (let [book (get-book exch ticker)]
      (book-unsub book)
      (swap! state -state-rm-book book))
    exch)
  ;; TODO Is it really worth creating a book if it doesn't exist?
  (get-book [exch ticker]
    (or (-state-get-book @state ticker)
        (do (add-book exch ticker)
            (get-book exch ticker))))
  (get-name [exch] (conn-name conn)))

(defn send-to-exch [exch msg]
  (send-out (:conn exch) msg))

(defn send-from-exch [exch msg]
  (send-in (:conn exch) msg))

(defn create-exch [conn]
  (letfn [(topic-fn [[tag payload]]
            (let [topic (conj-some [tag] (:ticker payload))]
              topic))]
    (Exch. conn
           (atom
             (State.
               ;; books
               {}
               ;; pub
               (a/pub (:in conn) topic-fn)
               ;; subs
               {})))))

;;* Standard handlers

(defmulti handle-msg (fn [exch [tag {ticker :ticker}]]
                       (log/info
                         (format
                           "[%s] handling msg: %s"
                           (get-name exch)
                           (conj-some [tag] ticker)))
                       tag))

(defmethod handle-msg :pong [exch [_ {cid :cid ts :ts}]]
  (log/info "Received "
            (with-out-str
              (pprint
                (conj-some
                  [:pong]
                  (-> nil
                      (assoc-some :ts ts)
                      (assoc-some :cid cid)))))))

(defmethod handle-msg :subscribed [exch [_ {ticker :ticker}]]
  (-> exch
      (add-book ticker)
      (get-book ticker)
      (book-toggle-status true)))

(defmethod handle-msg :snapshot [exch [_ {ticker :ticker
                                          snapshot :snapshot}]]
  (-book-apply-snapshot (get-book exch ticker) snapshot))

(defmethod handle-msg :update [exch [_ {ticker :ticker
                                        update :update}]]
  (-book-apply-update (get-book exch ticker) update))

(defmethod handle-msg :unsubscribed [exch [_ {ticker :ticker}]]
  (-> exch
      (get-book ticker)
      (book-toggle-status false)))

(defmethod handle-msg :default [exch msg]
  (log/error "No standard handler for msg " msg))

(defn start-standard-msg-handlers [exch ticker]
  (let [chan (a/chan 1)]
    ;; TODO too easy to forget to subscribe a new type of msg. There must be a 1
    ;; to 1 between handle-msg methods and standard subs.
    (sub exch [:subscribed ticker] chan)
    (sub exch [:snapshot ticker] chan)
    (sub exch [:update ticker] chan)
    (sub exch [:unsubscribed ticker] chan)
    (sub exch [:pong] chan)
    (a/go-loop []
      (when-let [msg (a/<! chan)]
        (handle-msg exch msg)
        (recur)))))

;;* Messages

(def any (constantly true))

(defn gen-decimal []
  (gen/fmap
    #(decimal-with-precision % 7)
    (spec/gen
      (spec/double-in
        :min 10.0
        :max 500.0
        :NaN? false
        :infinite? false))))

(defn gen-ticker []
  (gen/fmap
    #(ticker %)
    (spec/gen #{:btc/ltc :usd/btc
                :eur/eth :btc/zec})))

(spec/def ::ticker
  (spec/with-gen (partial instance? Ticker)
    gen-ticker))

(spec/def ::price
  (spec/with-gen (partial instance? BigDecimal)
    gen-decimal))

(spec/def ::size
  (spec/with-gen (partial instance? BigDecimal)
    gen-decimal))

(spec/def ::bid
  (spec/spec
    (spec/with-gen
      (spec/cat :price ::price
                :size ::size)
      (fn []
        (gen/tuple
          (spec/gen ::price)
          (spec/gen ::size))))))

(spec/def ::ask
  (spec/spec
    (spec/with-gen
      (spec/cat :price ::price
                :size ::size)
      (fn []
        (gen/tuple
          (spec/gen ::price)
          (spec/gen ::size))))))

(spec/def ::bids (spec/with-gen
                   (spec/* ::bid)
                   (fn []
                     (gen/vector
                       (spec/gen ::bid)))))

(spec/def ::asks (spec/with-gen
                   (spec/* ::ask)
                   (fn []
                     (gen/vector
                       (spec/gen ::ask)))))

(spec/def ::snapshot
  (spec/keys :req-un [::bids ::asks]))

(spec/def ::update ::snapshot)

;; TODO I could use multi-spec here, but for what benefit?

(def msg-tags
  #{:snapshot :update :heartbeat
    :error :pause :resume :reconnect
    :info :subscribed :unsubscribed :pong :drop})

(defn tag= [tag]
  (spec/and msg-tags #(= % tag)))

(spec/def ::snapshot-msg
  (spec/cat :tag (tag= :snapshot)
            :payload (spec/keys :req-un [::ticker ::snapshot])))

(spec/def ::update-msg
  (spec/cat :tag (tag= :update)
            :payload (spec/keys :req-un [::ticker ::update])))

(defn tagged-spec
  ([tag]
   (spec/cat :tag (tag= tag)
             :payload (spec/?
                        (spec/with-gen any
                          (fn []
                            (gen/map (spec/gen #{:foo :bar :baz})
                                     (gen/string-alphanumeric)))))))
  ([tag payload-spec]
   (spec/cat :tag (tag= tag)
             :payload payload-spec)))

(spec/def ::heartbeat-msg (tagged-spec :heartbeat))
(spec/def ::error-msg (tagged-spec :error))
(spec/def ::pause-msg (tagged-spec :pause))
(spec/def ::resume-msg (tagged-spec :resume))
(spec/def ::reconnect-msg (tagged-spec :reconnect))
(spec/def ::info-msg (tagged-spec :info))

(spec/def ::subscribed-msg (tagged-spec :subscribed (spec/keys :req-un [::ticker])))
(spec/def ::unsubscribed-msg (tagged-spec :unsubscribed (spec/keys :req-un [::ticker])))
(spec/def ::pong-msg (tagged-spec :pong))

(spec/def ::ack-msg
  (spec/or :subscribed ::subscribed-msg
           :unsubscribed ::unsubscribed-msg
           :pong ::pong-msg))

(spec/def ::reason
  (spec/with-gen string?
    (fn []
      (gen/fmap
        #(str "Because " %)
        (gen/string-alphanumeric)))))

(spec/def ::drop-msg (tagged-spec :drop (spec/keys :req-un [::reason])))

(spec/def ::message
  (spec/or :snapshot ::snapshot-msg
           :update ::update-msg
           :heartbeat ::heartbeat-msg
           :error ::error-msg
           :info ::info-msg
           :ack ::ack-msg
           :drop ::drop-msg))

(def message-gen (gen/fmap vec (spec/gen ::message)))

#_
(gen/sample
  (spec/gen ::message))

#_
(spec/exercise ::message)
