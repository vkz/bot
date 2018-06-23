(ns arb)

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

              bid
              [:buy bid-price (- bid-size trade-size)]

              ask
              [:sell ask-price (- ask-size trade-size)]

              bid-book
              (update bid-book
                      {:type :update
                       :ticker ticker
                       :changes [bid]})

              ask-book
              (update ask-book
                      {:type :update
                       :ticker ticker
                       :changes [ask]})

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

          {currency :currency}
          (ticker-pair
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
