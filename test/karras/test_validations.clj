(ns karras.test-validations
  (:require [karras.core :as karras])
  (:use karras.sugar
        karras.entity
        karras.validations
        [karras.collection :only [drop-collection]]
        clojure.test))

(defentity Person
  [:first-name :last-name]
  (validates-pressence-of :first-name))

(defentity Thing
  [:name]
  (validates-pressence-of :name "name is required."))

(defonce db (karras/mongo-db :karras-testing))

(use-fixtures :each (fn [t]
                      (karras/with-mongo-request db
                        (drop-collection (collection-for Person))
                        (t))))

(deftest test-validations-in-fields
  (is (= [:validates-pressence-of]
           (keys (:validations (field-spec-of Person :first-name))))))

(deftest test-presence-of
  (is (= [":first-name can't be blank."] (validate (Person.))))
  (is (thrown-with-msg? RuntimeException #":first-name can't be blank."
        (create Person {:last-name "Smith"})))
  (is (empty? (validate (make Person {:first-name "John"}))))
  (is (= ["name is required."] (validate (Thing.)))))
