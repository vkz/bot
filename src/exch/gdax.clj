(ns gdax)

(def gdax-products
  (into
    #{}
    (dedupe
      ["BTC-USD" "ETH-USD" "ETH-BTC" "LTC-USD" "LTC-BTC"
       "ETH-BTC" "LTC-BTC" "BTC-EUR" "ETH-EUR" "ETH-BTC"
       "LTC-EUR" "LTC-BTC" "BTC-GBP" "BTC-EUR" "ETH-BTC"
       "ETH-EUR" "LTC-BTC" "LTC-EUR" "ETH-BTC" "LTC-BTC"])))
