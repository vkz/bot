(ns generic
  (:require
   [medley.core :refer :all]
   [manifold.stream :as s]
   [cheshire.core :as json]
   [clojure.string :as string]
   [clojure.pprint :refer [pprint]]
   [clojure.core.async :as async]
   [clojure.core.async.impl.protocols :refer [closed?] :rename {closed? chan-closed?}]
   [taoensso.timbre :as log]))

(require '[exch :as exch :refer [conj-some]] :reload)

(declare send-msg)

(defrecord Connection [in status]
  ;; [exchange-name Channel Atom]

  exch/ConnectionProtocol
  (connect [conn] (reset! status true) conn)
  (disconnect [conn] (reset! status false))
  (connected? [conn] @status)
  (send-out [conn msg] (send-msg conn msg))
  ;; TODO validate against out-message spec
  (send-in [conn msg] (async/put! in msg))
  (conn-name [conn] :generic))

(defn create-connection []
  (let [in (async/chan 1)]
    (Connection.
      in
      (atom false))))

(defn log-msg [msg]
  (log/info
    "send-msg:"
    (with-out-str
      (newline)
      (pprint msg))))

(defmulti send-msg (fn multi-send-msg [conn [tag]] tag))

(defmethod send-msg :subscribe [conn msg]
  (log-msg msg))

(defmethod send-msg :unsubscribe [conn msg]
  (log-msg msg))
