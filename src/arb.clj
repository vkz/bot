(ns arb
  (:require
   [clojure.test :refer [deftest is are]]))

(require '[exch :as exch
           :refer
           [ticker ticker-kw base commodity currency
            timestamp decimal conj-some]]
         :reload)

;;* Arb

;; TODO This is exchange dependent, so really I should pass exchange structures
;; around and extract relevant books.

(defn with-cost [book side price]
  ;; TODO exchange+book => fee
  ;; hardcoding for now
  (let [taker-fee (/ 0.3 100)
        ;; withdrawal-fee 0
        ]
    (decimal
     (case side
       (:buy :ask) (* price (+ 1 taker-fee))
       (:sell :bid) (* price (- 1 taker-fee))))))

;; TODO too slow
(defn arb-step [bid-book ask-book]

  (if (not
       (= (:ticker bid-book)
          (:ticker ask-book)))

    ;; Can happen if arbitrager thread starts before we book snapshots from
    ;; exchanges. Should I simply Thread/sleep here?
    (do (println
         (str "Ticker mismatch:\n"
              "- Bid book ticker: " (:ticker bid-book) "\n"
              "- Ask book ticker: " (:ticker ask-book) "\n"
              "skipping"))
        (flush)
        nil)

    (let [ticker
          (:ticker bid-book)

          {bids :bids}
          bid-book

          [bid & bids]
          bids

          [bid-price bid-size]
          bid

          {asks :asks}
          ask-book

          [ask & asks]
          asks

          [ask-price ask-size]
          ask]

      ;; TODO assumes cost per 1 unit of size e.g. for :btc/eth that would be cost
      ;; in btc per 1 eth traded. Unless this assumption holds, may need to fix.
      ;; E.g. cost maybe proportional to a total value traded that is to price*size.

      (cond

        (and (some? bid-price)
             (some? ask-price)
             (pos?
              (- (with-cost bid-book :bid bid-price)
                 (with-cost ask-book :ask ask-price))))
        (let [trade-size
              (min bid-size
                   ask-size)

              bid-update
              {:bids [[(decimal bid-price) (decimal (- bid-size trade-size))]]
               :asks []}

              ask-update
              {:bids []
               :asks [[(decimal ask-price) (decimal (- ask-size trade-size))]]}

              bid-book
              (exch/update->book bid-book bid-update)

              ask-book
              (exch/update->book ask-book ask-update)

              buy-at
              ask-price

              sell-at
              bid-price]

          {:trade-size trade-size
           :buy-at buy-at
           :sell-at sell-at
           :bid-book bid-book
           :ask-book ask-book})

        :else
        nil))))

(defn arb [bid-book ask-book]
  (loop [bid-book bid-book
         ask-book ask-book
         trades []]
    (if-let [trade (arb-step bid-book
                             ask-book)]
      (recur (:bid-book trade)
             (:ask-book trade)
             (conj trades trade))
      trades)))

(defn arb? [bid-book ask-book]
  (when-let [ticker
             (and (:ticker bid-book)
                  (:ticker ask-book))]
    (let [{bids :bids}
          bid-book

          [bid & bids]
          bids

          [bid-price bid-size]
          bid

          {asks :asks}
          ask-book

          [ask & asks]
          asks

          [ask-price ask-size]
          ask]

      (and (some? bid-price)
           (some? ask-price)
           (pos?
            (- bid-price
               ask-price))
           (- bid-price
              ask-price)))))

(defn match [arb-trades]
  {:size    (->> arb-trades
                 (map :trade-size)
                 (apply +))
   :buy-at  (->> arb-trades
                 (map :buy-at)
                 (apply max))
   :sell-at (->> arb-trades
                 (map :sell-at)
                 (apply min))
   :books   (select-keys (last arb-trades)
                         [:bid-book :ask-book])})

(defn expect-profit [arb-trades]
  (if (empty? arb-trades)
    0
    (let [{bid-book :bid-book
           ask-book :ask-book}
          (first arb-trades)

          currency
          (currency
            (:ticker bid-book))]
      [(->> arb-trades
            (map (fn [{:keys [sell-at
                             buy-at
                             trade-size]}]
                   (-> (- (with-cost bid-book :sell sell-at)
                          (with-cost ask-book :buy buy-at))
                       (* trade-size))))
            (apply +))
       currency])))

(deftest arbitrage
  (let [ticker (ticker :btc/eth)

        bid-book (exch/snapshot->book
                   (exch/empty-book ticker)
                   {:bids [[6 1]
                           [5 4]
                           [4 1]
                           [3 10]]
                    :asks []
                    :ticker ticker})
        ask-book (exch/snapshot->book
                   (exch/empty-book ticker)
                   {:bids []
                    :asks [[4 2]
                           [4.5 4]
                           [5 5]
                           [6 10]]
                    :ticker ticker})]


    ;; calculating expected profit by hand for the above two books
    ;; * size (- sell buy)
    (is (= 4.3575M
           (+
             (* 1 (- (with-cost 'any :sell 6)
                     (with-cost 'any :buy 4)))
             (* 1 (- (with-cost 'any :sell 5)
                     (with-cost 'any :buy 4)))
             (* 3 (- (with-cost 'any :sell 5)
                     (with-cost 'any :buy 4.5))))))
    ;; NOTE incidentally were I to perform this calculation in Clojure's default
    ;; double instead of decimal I'd get a result with rounding error:
    ;; 4.3575000000000035, so yeah, don't use float for money!

    (is (= (with-cost 'b :ask 4)
           (with-cost 'b :buy 4)))


    (is (= (with-cost 'b :sell 5)
           (with-cost 'b :bid 5)))

    (is (arb? bid-book ask-book))

    (is (=
          [4.3575M :btc]
          (expect-profit
            (arb bid-book
                 ask-book))))))
