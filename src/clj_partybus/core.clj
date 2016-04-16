(ns clj-partybus.core
  (:import (javax.management.openmbean KeyAlreadyExistsException)))

(def queues (atom {}))

(defn register!
  "Registers a thread with the message bus."
  [key]
  (if (keyword? key)
    (if-not (contains? @queues key)
      (swap! queues assoc key clojure.lang.PersistentQueue/EMPTY)
      (throw
        (KeyAlreadyExistsException. "Queue already exists for this key.")))
    (throw
      (IllegalArgumentException. "Key must be a keyword."))))

(defn clear-queue!
  "Clears the queue belonging to a specific key."
  [key]
  (if (contains? @queues key)
    (swap! queues assoc key clojure.lang.PersistentQueue/EMPTY)
    (throw
      (IllegalArgumentException. "This key isn't registsterd."))))

(defn full-reset!
  "Clears all queues and keys."
  []
  (reset! queues {}))

(defn message-ready?
  "Returns true if there is a message ready for the given key."
  [key]
  (if (contains? @queues key)
    (not (nil? (peek (@queues key))))
    (throw
      (IllegalArgumentException. "This key isn't registered."))))

(defn send-message!
  "Adds a new message to the queue belonging to the given key."
  [key msg]
  (if (contains? @queues key)
    (swap! queues #(update % key conj msg))
    (throw
      (IllegalArgumentException. "There is no queue registered for this key."))))

(defn get-message!
  "Grabs the next avaliable message in key's queue."
  [key]
  (if (message-ready? key)
    (let [ret (peek (@queues key))]
      (swap! queues #(update % key pop))
      ret)
    (throw
      (IllegalArgumentException. "No message is avaliable for that key."))))