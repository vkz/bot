;; Copyright (C) 2018, 2019 by Vlad Kozin

(ns kraken
  (:require [clojure.spec.alpha :as spec]
            [clojure.spec.gen.alpha :as gen]
            [medley.core :refer :all]
            [clj-http.client :as http-client]
            [manifold.stream :as s]
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

(def ^:private URL "https://api.kraken.com/0/public")

(defrecord Product [sym base quote altname])

(defn get-public [resource & query-params]
  (let [query-params (apply array-map query-params)
        params (cond-> {}
                 (not-empty query-params) (assoc :query-params query-params)
                 :as (assoc :as :json))
        url (str URL resource)]
    (let [{error :error
           result :result
           :as
           response}
          (-> url
              (http-client/get params)
              :body)]
      (if (not-empty error)
        (log/error
          (format "Requesting %s with params %s returned an error %s"
                  url
                  params
                  error))
        result))))

(defn get-products []
  (->> (get-public"/AssetPairs")
       (map-kv
         (fn [prod {:keys [base quote altname]}]
           (letfn [(currency-triple [s]
                     (if (and (> (.length s) 3)
                              (or (string/starts-with? s "X")
                                  (string/starts-with? s "Z")))
                       (subs s 1)
                       s))]
             [(name prod)
              {:sym (name prod)
               :base (currency-triple base)
               :quote (currency-triple quote)
               :altname altname}])))
       (filter-keys #(not (string/ends-with? % ".d")))
       (vals)))

(def ^:private PRODUCTS
  (->> (get-products)
       (map map->Product)
       (into #{})))

(extend-protocol exch/ITicker

  Product

  (ticker [{base :base quote :quote}]
    (let [kw (comp keyword string/lower-case)]
      (exch/map->Ticker
        {:base (kw base)
         :quote (kw quote)})))
  (base [p] (:base (ticker p)))
  (currency [p] (:quote (ticker p)))
  (commodity [p] (:base (ticker p)))
  (ticker-kw [p] (ticker-kw (ticker p))))

(def ^:private TICKERS-PRODUCTS
  (->> PRODUCTS
       (map #(vector (ticker %) %))
       (into {})))

(defn- product-of-sym [symbol]
  (find-first
    #(= (if (keyword? symbol)
          (name symbol)
          symbol)
        (:sym %)) PRODUCTS))

(defn- product [ticker]
  (if-some [product (TICKERS-PRODUCTS ticker)]
    product
    (do
      (log/error "Ticker " ticker " does not match any product.")
      nil)))

(defn decimal-str? [v]
  (try
    (decimal v)
    (catch NumberFormatException _
      :clojure.spec.alpha/invalid)))

(defn product-symbol->ticker? [v]
  (let [p (product-of-sym v)]
    (if (get PRODUCTS p)
      (ticker p)
      :clojure.spec.alpha/invalid)))

(spec/def ::bid
  (spec/spec
    (spec/cat :price (spec/conformer decimal-str?)
              :size (spec/conformer decimal-str?)
              :timestamp number?)))

(spec/def ::ask
  (spec/spec
    (spec/cat :price (spec/conformer decimal-str?)
              :size (spec/conformer decimal-str?)
              :timestamp number?)))

(spec/def ::bids
  (spec/* ::bid))

(spec/def ::asks
  (spec/* ::ask))

(spec/def ::snapshot (spec/keys :req-un [::bids ::asks]))

(spec/def ::order-book
  (spec/cat :ticker (spec/conformer product-symbol->ticker?)
            :snapshot ::snapshot))

#_
(spec/conform ::order-book
              [:XETHXXBT
               {:bids [["0.058540" "38.457" 1533574698]
                       ["0.058500" "36.090" 1533574687]
                       ["0.058490" "2.759" 1533574714]]
                :asks [["0.058550" "1.500" 1533574618]
                       ["0.058560" "0.487" 1533574694]
                       ["0.058580" "2.000" 1533574688]]}])

;; One possible way to speed things up:
;; - request order book limited to best bid and offer i.e. count=1
;; - establish if arb exist,
;; - if arb then request larger book.

;; curl "https://api.kraken.com/0/public/Depth?pair=XETHXXBT&count=3"
;; =>

;; order book limited to max 3 entries
;; {
;;    "result" : {
;;       "XETHXXBT" : {
;;          "bids" : [
;;             [
;;                "0.058540",
;;                "38.457",
;;                1533574698
;;             ],
;;             [
;;                "0.058500",
;;                "36.090",
;;                1533574687
;;             ],
;;             [
;;                "0.058490",
;;                "2.759",
;;                1533574714
;;             ]
;;          ],
;;          "asks" : [
;;             [
;;                "0.058550",
;;                "1.500",
;;                1533574618
;;             ],
;;             [
;;                "0.058560",
;;                "0.487",
;;                1533574694
;;             ],
;;             [
;;                "0.058580",
;;                "2.000",
;;                1533574688
;;             ]
;;          ]
;;       }
;;    },
;;    "error" : []
;; }
