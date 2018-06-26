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
           [java.util Date]))

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
(defprotocol ConnectionProtocol
  (connect [Conn])
  (disconnect [Conn])
  (connected? [Conn])
  (send-out [Conn msg])
  ;; for testing: puts msg on the internal :in port
  (send-in [Conn msg])
  ;; json => exch/msg
  (convert-incomming-msg [Conn msg])
  ;; exch/msg => json
  (convert-outgoing-msg [Conn msg])
  (conn-name [Conn]))

;;* Book
(defn empty-book [ticker]
  {:ticker ticker
   :asks (sorted-map-by <)
   :bids (sorted-map-by >)})

(defn snapshot->book
  [book snapshot]
  {:ticker (:ticker book)
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

(defprotocol BookProtocol
  (-book-sub [Book])
  (-book-unsub [Book])
  (-book-subscribed? [Book])
  (-book-apply-snapshot [Book snapshot])
  (-book-apply-update [Book update])
  (snapshot [Book])
  (bids [Book])
  (asks [Book])
  ;; callback: fn [old-val new-val]
  (watch [Book key callback])
  (unwatch [Book key]))

(defrecord Book [ticker conn agent status]
  BookProtocol
  ;; TODO I never actually use these
  (-book-sub [book] (send-out conn [:subscribe ticker]))
  (-book-unsub [book] (send-out conn [:unsubscribe ticker]))
  ;; TODO status never gets toggled anywhere
  (-book-subscribed? [book] status)
  (-book-apply-snapshot [book snapshot] (send agent snapshot->book snapshot) book)
  (-book-apply-update [book update] (send agent update->book update) book)
  (snapshot [book] @agent)
  (bids [book] (get @agent :bids))
  (asks [book] (get @agent :asks))
  (watch [book key callback]
    (add-watch
      agent
      key
      (fn [_ _ old-book new-book]
        (callback old-book new-book))))
  (unwatch [book key]
    (remove-watch agent key)))

(defn create-book [conn ticker]
  (Book. ticker
         conn
         (agent (empty-book ticker))
         false))

;;* State

(defprotocol StateProtocol
  (-state-sub [this topic chan])
  (-state-unsub [this topic chan])
  (-state-book [this ticker])
  (-state-add-book [this book])
  (-state-rm-book [this book]))

;; books:
;; {ticker => Book}
;; pub:
;; (async/pub (conn :in) topic-fn)
;; subs:
;; {topic => #{chan}}
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
  (-state-book [this ticker] (get books ticker))
  (-state-add-book [this book] (State. (assoc books (:ticker book) book) pub subs))
  (-state-rm-book [this book] (State. (dissoc books (:ticker book)) pub subs)))

;;* Exch

(defprotocol ExchProtocol
  (sub [Exch topic chan])
  (unsub [Exch topic chan])
  (add-book [Exch ticker])
  (rm-book [Exch ticker])
  (get-book [Exch ticker])
  (exch-name [Exch]))

(defrecord Exch [connection state]
  ExchProtocol
  (sub [exch topic chan]
    (a/sub (:pub @state) topic chan)
    (swap! state -state-sub topic chan))
  (unsub [exch topic chan]
    (swap! state -state-unsub topic chan)
    (a/unsub (:pub @state) topic chan))
  (add-book [exch ticker]
    (when-not (-state-book @state ticker)
      (swap! state -state-add-book (create-book connection ticker)))
    exch)
  (rm-book [exch ticker] (swap! state -state-rm-book (get-book exch ticker)) exch)
  (get-book [exch ticker] (or (-state-book @state ticker)
                              (do (add-book exch ticker)
                                  (get-book exch ticker))))
  (exch-name [exch] (conn-name connection)))

(defn send-to-exch [exch msg]
  (send-out (:connection exch) msg))

(defn send-from-exch [exch msg]
  (send-in (:connection exch) msg))

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
                           "[Exchange %s] handling msg: %s"
                           (exch-name exch)
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
  (add-book exch ticker))

(defmethod handle-msg :snapshot [exch [_ {ticker :ticker
                                          snapshot :snapshot}]]
  (-book-apply-snapshot (get-book exch ticker) snapshot))

(defmethod handle-msg :update [exch [_ {ticker :ticker
                                        update :update}]]
  (-book-apply-update (get-book exch ticker) update))

(defmethod handle-msg :unsubscribed [exch [_ {ticker :ticker}]]
  (rm-book exch ticker))

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

