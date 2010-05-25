(ns karras.validations
  (:use [karras.document :only [EntityCallbacks default-callbacks]]
        clojure.contrib.error-kit
        [clojure.contrib.str-utils :only [str-join]]))

(deferror invalid-entity [] [e errors]
  {:entity e
   :errors errors
   :unhandled (fn [e] (throw (RuntimeException. (str-join " " (map str (:errors e))))))})

(defprotocol ValidationCallbacks
  (before-validate [e])
  (after-validate [e]))

(def default-validation-callbacks {:before-validate identity
                                   :after-validate identity})
(defonce validations (atom {}))

(defn clear-validations []
  (swap! validations {}))

(defn validate [e]
  (remove nil? (map #(% e) (get @validations (class e)))))

(defn make-validatable [type]
  (let [current-impls (or (-> EntityCallbacks :impls (get type)) default-callbacks)]
    (extend type
      ValidationCallbacks
      default-validation-callbacks
      EntityCallbacks
      (assoc current-impls
        :before-save (fn [e]                         
                       (let [e (before-validate e)]
                         (let [results (validate e)]
                           (when results
                             (raise invalid-entity e results))
                           (-> e after-validate (:before-save current-impls)))))))))

(defn add-validation [type f]
  (when-not (extends? ValidationCallbacks type)
    (make-validatable type))
  (swap! validations #(assoc % type (conj (get % type #{}) f))))



(defn is-present? [e field]
  (get e field))

(defn validates-pressence-of
  ([type field]
     (validates-pressence-of type field (str field " can't be blank.")))
  ([type field message]
     (add-validation type (fn [e]
                            (if-not (is-present? e field)
                              message)))))
