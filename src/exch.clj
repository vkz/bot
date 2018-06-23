(ns exch
  (:require [medley.core :refer :all]
            [clojure.spec.alpha :as spec]
            [clojure.spec.gen.alpha :as gen]
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
    (spec/cat :price ::price
              :size ::size)))

(spec/def ::ask
  (spec/spec
    (spec/cat :price ::price
              :size ::size)))

(spec/def ::bids (spec/* ::bid))
(spec/def ::asks (spec/* ::ask))

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

#_
(gen/sample
  (spec/gen ::message))

