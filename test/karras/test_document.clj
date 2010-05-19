(ns karras.test-document
  (:use [karras.sugar :only [date]]
   karras.document
   clojure.test))

(defaggregate Street
  :name
  :number)

(defaggregate Address
  :street {:type Street}
  :city
  :state
  :postal-code)

(defaggregate Phone
  :country-code {:type Integer :default 1}
  :number)

(defentity Person 
  :first-name
  :middle-initial
  :last-name
  :birthday {:type java.util.Date}
  :blood-alcohol-level {:type Float :default 0.0}
  :address {:type Address}
  :phones {:type java.util.List :of Phone})

(deftest test-parse-fields
  (let [parsed? (fn [fields expected-parsed-fields]
                  (is (= (parse-fields fields) expected-parsed-fields)))]
    (testing "empty fields"
          (parsed? nil {}))
    (testing "no type specified"
      (parsed? [:no-type] {:no-type {:type String}}))
    (testing "type specified"
      (parsed? [:with-type {:type Integer}] {:with-type {:type Integer}}))
    (testing "mixed types and no types"
          (parsed? [:no-type
                    :with-type {:type Integer}]
                   {:no-type {:type String}
                    :with-type {:type Integer}})
          (parsed? [:with-type {:type Integer}
                    :no-type]
                   {:with-type {:type Integer}
                    :no-type {:type String}})))
  (are [fields] (thrown? IllegalArgumentException (parse-fields fields))
       ['not-a-keyword]
       [:keyword 'not-a-map-or-keyword]))

(deftest test-callback-protocol
  (are [callback e] (= e (callback e))
       before-create (Person.)
       before-destroy (Person.)
       before-save (Person.)
       before-update (Person.)
       before-validate (Person.)
       after-create (Person.)
       after-destroy (Person.)
       after-save (Person.)
       after-update (Person.)
       after-validate (Person.)))

(deftest test-docspec
  (is (not (nil? (docspec Address))))
  (is (not (nil? (docspec Phone))))
  (is (not (nil? (docspec Person)))))

(deftest test-docspec-in
  (is (= (docspec Address) (docspec-of Person :address)))
  (is (= (docspec java.util.List) (docspec-of Person :phones)))
  (is (= (docspec Phone) (docspec-of-item Person :phones)))
  (is (= (docspec Street) (docspec-of Person :address :street))))

(deftest test-docspec-in
  (is (= (docspec Address) (docspec-of Person :address)))
  (is (= (docspec java.util.List) (docspec-of Person :phones)))
  (is (= (docspec Phone) (docspec-of-item Person :phones)))
  (is (= (docspec Street) (docspec-of Person :address :street))))

(deftest test-collection-name
  (testing "default name"
    (is (= (:collection-name (docspec Person)) "Person")))
  (testing "override name"
    (alter-entity-type! Person :collection-name "people")
    (is (= (:collection-name (docspec Person)) "people"))))

(deftest test-create
  (testing "create"
    (let [phone (make Phone {:number "555-555-1212"})]
      (is (= Phone (class phone))))
    (let [address (make Address {:city "Nashville" :street {:name "Main St." :number "123"}})]
      (is (= Address (class address)))
      (is (= Street (class (:street address)))))
    (let [person (make Person {:first-name "John"
                               :last-name "Smith"
                               :birthday (date 1976 7 4)
                               :phones [{:number "123" :country-code 2}]
                               :address {:city "Nashville"
                                         :street {:number "123" :name "Main St."}}})]
      (is (= Address (class (-> person :address))))
      (is (= Street (class (-> person :address :street))))
      (is (= Phone (class (-> person :phones first)))))))


;(run-tests)
