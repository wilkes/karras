(ns karras.test-validations
  (:require karras)
  (:use karras.sugar
        karras.document
        karras.validations
        clojure.test))

(defentity Person
  [:first-name :last-name]
  (validates-pressence-of :first-name))

(defentity Thing
  [:name]
  (validates-pressence-of :name "name is required."))

(defonce db (karras/mongo-db (karras/connect) :document-testing))

(use-fixtures :each (fn [t]
                      (karras/with-mongo-request db
                        (karras/drop-collection (collection-for Person))
                        (t))))

 (deftest test-presence-of
   (is (= [":first-name can't be blank."] (validate (Person.))))
   (is (thrown-with-msg? RuntimeException #":first-name can't be blank."
         (create Person {:last-name "Smith"})))
   (is (empty? (validate (make Person {:first-name "John"}))))
   (is (= ["name is required."] (validate (Thing.)))))
