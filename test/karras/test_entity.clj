(ns karras.test-entity
  (:require [karras.core :as karras])
  (:use karras.sugar
        karras.entity
        [karras.collection :only [drop-collection collection]]
        clojure.test
        midje.semi-sweet))

(def not-nil? (comp not nil?))
(defonce db (karras/mongo-db :karras-testing))

(defaggregate Street
  [:name
   :number])

(defaggregate Address
  [:street {:type Street}
   :city
   :state
   :postal-code])

(defaggregate Phone
  [:country-code {:default 1}
   :number])

(defmethod convert ::my-date
  [field-spec d]
  (if (instance? java.util.Date d)
    (.format (java.text.SimpleDateFormat. "yyyy-MM-dd") d)
    d))

(defentity Person
  [:first-name
   :middle-initial
   :last-name
   :birthday {:type ::my-date}
   :blood-alcohol-level {:default 0.0}
   :address {:type Address}
   :phones {:type :list :of Phone}]
  (index (desc :last-name) (desc :first-name))
  (index (asc :birthday)))

(defentity Company
  [:name
   :employees {:type :references :of Person}
   :ceo {:type :reference :of Person}
   :date-founded {:type ::my-date}]
  (deffetch older-companies [date-str]
    (lte :date-founded date-str))
  (deffetch modern-companies []
    (gt :date-founded "1980"))
  (deffetch-one company-by-name [name]
    (eq :name name)))

(defentity Simple
  [:value]
  (entity-spec-assoc :collection-name "simpletons"))

(defmacro mongo [& forms]
  `(karras/with-mongo-request db
    ~@forms))

(use-fixtures :each (fn [t]
                      (mongo
                        (drop-collection (collection-for Person))
                        (drop-collection (collection-for Company))
                        (drop-collection (collection-for Simple))
                        (t))))

(deftest test-parse-fields
  (let [parsed? (fn [fields expected-parsed-fields]
                  (expect (parse-fields fields) =>  expected-parsed-fields))]
    (testing "empty fields"
          (parsed? nil {}))
    (testing "no type specified"
      (parsed? [:no-type] {:no-type {}}))
    (testing "type specified"
      (parsed? [:with-type {:type Integer}] {:with-type {:type Integer}}))
    (testing "mixed types and no types"
          (parsed? [:no-type
                    :with-type {:type Integer}]
                   {:no-type {}
                    :with-type {:type Integer}})
          (parsed? [:with-type {:type Integer}
                    :no-type]
                   {:with-type {:type Integer}
                    :no-type {}})))
  (are [fields] (thrown? IllegalArgumentException (parse-fields fields))
       ['not-a-keyword]
       [:keyword 'not-a-map-or-keyword]))

(deftest test-entity-spec
  (doseq [e [Address Phone Person]]
    (expect e => not-nil?)))

(deftest test-entity-spec-in
  (expect (entity-spec-of Person :address) => (entity-spec Address))
  (expect (entity-spec-of Person :phones) => (entity-spec java.util.List))
  (expect (entity-spec-of-item Person :phones) => (entity-spec Phone))
  (expect (entity-spec-of Person :address :street) => (entity-spec Street)))

(deftest test-collection-name
  (testing "default name"
    (expect (:collection-name (entity-spec Person)) => "people"))
  (testing "override name"
    (expect (:collection-name (entity-spec Simple)) => "simpletons")))

(deftest test-make
  (testing "flat"
    (expect (class (make Phone {:number "555-555-1212"})) => Phone))
  (testing "nested"
    (let [address (make Address {:city "Nashville"
                                 :street {:number "123"
                                          :name "Main St."}})]
      (expect (class address) => Address)
      (expect (class (:street address)) => Street)))
  (testing "complex nested with defaults"
    (let [person (make Person
                       {:first-name "John"
                        :last-name "Smith"
                        :birthday (date 1976 7 4)
                        :phones [{:number "123"}]
                        :address {:city "Nashville"
                                  :street {:number "123"
                                           :name "Main St."}}})]
      (expect (-> person :address class) => Address)
      (expect (-> person :address :street class) => Street)
      (expect (-> person :phones first class)=> Phone)
      (expect (-> person :blood-alcohol-level) =>  0.0)
      (expect (-> person :phones first :country-code) => 1)))
  (testing "preserves the metadata of original hash")
   (let [person (make Person #^{:meta "data"} {:first-name "Jimmy"})]
     (expect (meta person) => {:meta "data"})))

(deftest test-crud
  (let [person (create Person
                       {:first-name "John"
                        :last-name "Smith"
                        :birthday (date 1976 7 4)
                        :phones [{:number "123" :country-code 2}]
                        :address {:city "Nashville"
                                  :street {:number "123"
                                           :name "Main St."}}})]
    (testing "create"
      (expect (class person) => Person)
      (expect (:birthday person) => "1976-07-04")
      (expect (:_id person) => not-nil?)
      (expect (count-instances Person) => 1))
    (testing "fetch-one"
      (expect (fetch-one Person (where (eq :_id (:_id person))))
              => person))
    (testing "fetch-all"
      (expect (first (fetch-all Person))
              => person))
    (testing "fetch"
      (expect (first (fetch Person (where (eq :last-name "Smith"))))
              => person))
    (testing "deletion"
      (dotimes [x 5]
        (create Person {:first-name "John" :last-name (str "Smith" (inc x))}))
      (expect (first (distinct-values Person :first-name)) => "John")
      (expect (count-instances Person) => 6)
      (testing "delete"
        (delete person)
        (expect (count-instances Person) => 5))
      (testing "delete-all with where clause"
        (delete-all Person (where (eq :last-name "Smith1")))
        (expect (count-instances Person) => 4))
      (testing "delete-all"
        (delete-all Person)
        (expect (count-instances Person) => 0)))))

(deftest test-collection-for
  (testing "entity type"
    (expect (collection-for Person) => :people
            (fake (collection "people") => :people)))
  (testing "entity instance"
    (expect (collection-for (make Person {:last-name "Smith"})) => :people
            (fake (collection "people") => :people))))

(deftest test-ensure-indexes
  (expect (list-indexes Person) => empty?)
  (ensure-indexes)
  ;; 2 + _id index
  (expect (count (list-indexes Person)) => 3))

(deftest test-references
  (testing "saving"
    (let [john (create Person {:first-name "John" :last-name "Smith"})
          jane (create Person {:first-name "Jane" :last-name "Doe"})
          company (-> (create Company {:name "Acme"})
                      (set-reference :ceo john)
                      (add-reference :employees jane)
                      save)]
      (expect (:ceo company) => (:_id john))
      (expect (first (:employees company)) => (:_id jane))))
  (testing "reading"
    (let [company (fetch-one Company (where (eq :name "Acme")))
          john (get-reference company :ceo)
          [jane] (get-reference company :employees)]
      (expect (:last-name john) => "Smith")
      (expect (:last-name jane) => "Doe" )))
  (testing "updating"
    (let [bill (create Person {:first-name "Bill" :last-name "Jones"})
          company (-> (fetch-one Company (where (eq :name "Acme")))
                      (add-reference :employees bill))
          [jane bill] (get-reference company :employees)]
      (expect (:first-name jane) => "Jane")
      (expect (:first-name bill) => "Bill"))))

(deftest test-deffetch
  (is (= {:older-companies older-companies
          :modern-companies modern-companies}
         (entity-spec-get Company :fetchs)))
  (let [jpmorgan (create Company {:name "JPMorgan Chase & Co." :date-founded "1799"})
        dell (create Company {:name "Dell" :date-founded (date 1984 11 4)})
        exxon (create Company {:name "Exxon" :date-founded "1911"})]
    (expect (older-companies "1800") => [jpmorgan])
    (expect (older-companies "1913") => (in-any-order [jpmorgan exxon]) )
    (expect (older-companies "1913" :sort [(asc :name)]) => [exxon jpmorgan])
    (expect (older-companies "1999" :sort [(asc :date-founded) (asc :name)])
            => [jpmorgan exxon dell])
    (expect (modern-companies) => [dell])))

(deftest test-deffetch-one
  (is (= {:company-by-name company-by-name}
         (entity-spec-get Company :fetch-ones)))
  (let [dell (create Company {:name "Dell" :date-founded (date 1984 11 4)})
        exxon (create Company {:name "Exxon" :date-founded "1911"})]
    (is (= dell (company-by-name "Dell")))))


(deftest test-find-and-*
  (let [foo (create Simple {:value "Foo"})
        expected (merge foo {:age 21})]
    (testing "find-and-modify"
      (expect (find-and-modify Simple (where (eq :value "Foo"))
                               (modify (set-fields {:age 21}))
                               :return-new true)
              => expected))
    (testing "find-and-remove"
      (expect (find-and-remove Simple (where (eq :value "Foo")))
              => expected))))

(deftest test-map-reduce
  (dotimes [n 5]
    (create Simple {:value n}))
  (expect (map-reduce-fetch-all Simple
                                "function() {emit('sum', this.value)}"
                                "function(k,vals) {
                                    var sum=0;
                                    for(var i in vals) sum += vals[i];
                                    return sum;
                                 }")
                  => [{:_id "sum" :value (apply + (range 5))}]))
