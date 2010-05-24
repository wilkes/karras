(ns karras.test-validations
  (:require karras)
  (:use karras.sugar
        karras.document
        karras.validations
        clojure.test))

(defentity Person :first-name :last-name)

(defonce db (karras/mongo-db (karras/connect) :document-testing))

(use-fixtures :each (fn [t]
                      (karras/with-mongo-request db
                        (karras/drop-collection (collection-for Person))
                        (make-validatable Person)
                        (clear-validations)
                        (t))))

 (deftest test-presence-of
   (validates-pressence-of Person :first-name)
   (is (= [":first-name can't be blank."] (validate (Person.))))
   (is (thrown-with-msg? RuntimeException #":first-name can't be blank." (create Person {:last-name "Smith"}))))
