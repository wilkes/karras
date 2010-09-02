(ns karras.test-entity
  (:require [karras.core :as karras])
  (:use karras.entity :reload-all)
  (:use karras.sugar
        [karras.collection :only [collection]]
        clojure.test
        midje.sweet
        karras.entity.testing
        clojure.pprint
        [com.reasonr.scriptjure :only [js]]))

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

(defentity Resposibility 
  [:name])

(defentity Person
  [:first-name
   :middle-initial
   :last-name
   :birthday {:type ::my-date}
   :counter {:default 0}
   :address {:type Address}
   :phones {:type :list :of Phone}
   :responsibity {:type :reference :of Resposibility}]
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

(use-fixtures :each (entity-fixture db))

(deftest test-parse-fields
  (let [parsed? (fn [fields expected-parsed-fields]
                  (fact (parse-fields fields) =>  expected-parsed-fields))]
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
    (fact (entity-spec e) => not-nil?)))

(deftest test-entity-spec-in
  (fact (entity-spec-of Person :address) => (entity-spec Address)
        (entity-spec-of Person :phones) => (entity-spec java.util.List)
        (entity-spec-of-item Person :phones) => (entity-spec Phone)
        (entity-spec-of Person :address :street) => (entity-spec Street)))

(deftest test-collection-name
  (fact "default name"
        (:collection-name (entity-spec Person)) => "people"
        "override name"
        (:collection-name (entity-spec Simple)) => "simpletons"))

(deftest test-make
  (facts "flat"
         (class (make Phone {})) => Phone)
  (let [address (make Address {:city "Nashville"
                               :street {:number "123"
                                        :name "Main St."}})]
    (facts (class address) => Address
           (class (:street address)) => Street))
  (testing "complex nested with defaults"
    (let [person (make Person
                       {:first-name "John"
                        :last-name "Smith"
                        :birthday (date 1976 7 4)
                        :phones [{:number "123"}]
                        :address {:city "Nashville"
                                  :street {:number "123"
                                           :name "Main St."}}})]
      (facts (-> person :address class) => Address
             (-> person :address :street class) => Street
             (-> person :phones first class)=> Phone
             (-> person :counter) =>  0.0
             (-> person :phones first :country-code) => 1
             (-> person save :_id) => not-nil?)))
  (testing "preserves the metadata of original hash")
   (let [person (make Person #^{:meta "data"} {:first-name "Jimmy"})]
     (facts (meta person) => {:meta "data"})))

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
      (facts (class person) => Person
             (:birthday person) => "1976-07-04"
             (:_id person) => not-nil?
             (count-instances Person) => 1))
    (testing "fetch-one"
      (fact (fetch-one Person (where (eq :_id (:_id person))))
              => person))
    (testing "fetch-all"
      (fact (fetch-all Person) => [person]))
    (testing "fetch"
      (fact (fetch Person (where (eq :last-name "Smith")))
              => [person])
      (fact (fetch Person (where (eq :last-name "Nobody")))
              => [])
      (fact (fetch-one Person (where (eq :last-name "Nobody")))
              => nil))
    (testing "save"
      (save (assoc person :was-saved true))
      (fact (:was-saved (fetch-by-id person)) => true))
    (testing "update"
      (update Person (where (eq :last-name "Smith"))
                      (modify (set-fields {:birthday (date 1977 7 4)})))
      (fact (:birthday (fetch-one Person (where (eq :last-name "Smith"))))
              => "1977-07-04"))
    (testing "deletion"
      (dotimes [x 5]
        (create Person {:first-name "John" :last-name (str "Smith" (inc x))}))
      (fact (distinct-values Person :first-name) => #{"John"})
      (fact (count-instances Person) => 6)
      (testing "delete"
        (delete person)
        (fact (count-instances Person) => 5))
      (testing "delete-all with where clause"
        (delete-all Person (where (eq :last-name "Smith1")))
        (fact (count-instances Person) => 4))
      (testing "delete-all"
        (delete-all Person)
        (fact (count-instances Person) => 0)))))

(deftest test-fetch-by-id
  (let [person (create Person {:first-name "John" :last-name "Smith"})]
    (fact (fetch-by-id person) => person)
    (delete person)
    (fact (fetch-by-id person) => nil)))

(deftest test-collection-for
  (testing "entity type"
    (fact (collection-for Person) => :people
            (provided (collection "people") => :people)))
  (testing "entity instance"
    (fact (collection-for (make Person {:last-name "Smith"})) => :people
          (provided (collection "people") => :people)))

;; Breaks in the repl after the first run -- not sure why
  (deftest test-ensure-indexes
    (fact (list-indexes Person) => empty?)
    (ensure-indexes)
    (fact (count (list-indexes Person)) => 3))) ;; 2 + _id index

(deftest test-references
  (testing "make reference"
    (let [simple (create Simple {})]
      (fact (make-reference simple) => (make-reference Simple (:_id simple)))))
  (testing "saving"
    (let [john (create-with Person
                            {:first-name "John" :last-name "Smith"}
                            (relate :responsibity {:name "in charge"}))
          jane (create Person {:first-name "Jane" :last-name "Doe"})
          company (create-with Company
                               {:name "Acme"}
                               (relate :ceo john)
                               (relate :employees jane))]
      (fact (-> company :ceo :_id) => (:_id john))
      (fact (-> company :employees first :_id) => (:_id jane))))
  (testing "reading"
    (let [company (fetch-one Company (where (eq :name "Acme")))
          john (get-reference company :ceo)
          [jane] (get-reference company :employees)]
      (fact (class (:ceo company)) => Person)
      (fact (class (first (:employees company))) => Person)
      (fact (:last-name john) => "Smith")
      (fact (:last-name jane) => "Doe" )
      (testing "grab"
        (fact (grab company :name) => (:name company))
        (fact (grab company :ceo) => john)
        (fact (grab company :employees) => [jane]))
      (testing "grab-in"
        (fact (grab-in company [:ceo :first-name]) => "John")
        (fact (grab-in company [:ceo :responsibity :name]) => "in charge"))))
  (testing "updating"
    (let [company (-> (fetch-one Company (where (eq :name "Acme")))
                      (relate :employees {:first-name "Bill" :last-name "Jones"}))
          [jane bill] (grab company :employees)]
      (fact (:first-name jane) => "Jane")
      (fact (:first-name bill) => "Bill")))
  (testing "reverse look up company from person"
    (let [company (fetch-one Company (where (eq :name "Acme")))
          john (fetch-one Person (where (eq :first-name "John")))
          jane (fetch-one Person (where (eq :first-name "Jane")))]
      (fact (fetch-refers-to john Company :ceo) => [company])
      (fact (fetch-refers-to jane Company :employees) => [company]))))

(deftest test-grab-caching
  (let [company (create-with Company
                             {:name "Acme"}
                             (relate :ceo
                                     {:first-name "John" :last-name "Smith"})
                             (relate :employees
                                     {:first-name "Jane" :last-name "Doe"}))
        john (fetch-one Person (where (eq :first-name "John")))
        jane (fetch-one Person (where (eq :first-name "Jane")))]
    (testing "single reference"
      (fact (grab company :ceo) => :fake-result
            (provided (get-reference company :ceo) => :fake-result))
      (fact (-> (get company :ceo) meta :cache deref) => :fake-result)
      (fact (-> (get company :ceo) :_ref) => "people")
      (testing "cache hit"
        (fact (grab company :ceo) => :fake-result))
      (testing "cache refresh"
        (fact (grab company :ceo :refresh) => john)))
    (testing "list of references"
      (fact (grab company :employees) => :fake-result
              (provided (get-reference company :employees) => :fake-result))
      (fact (-> (get company :employees) meta :cache deref) => :fake-result)
      (fact (-> (get company :employees) first :_ref) => "people")
      (testing "cache hit"
        (fact (grab company :employees) => :fake-result))
      (testing "cache refresh"
        (fact (grab company :employees :refresh) => [jane])))))

(deftest test-deffetch
  (is (= {:older-companies older-companies
          :modern-companies modern-companies}
         (entity-spec-get Company :fetchs)))
  (let [jpmorgan (create Company {:name "JPMorgan Chase & Co." :date-founded "1799"})
        dell (create Company {:name "Dell" :date-founded (date 1984 11 4)})
        exxon (create Company {:name "Exxon" :date-founded "1911"})]
    (fact (older-companies "1800") => [jpmorgan])
    (fact (older-companies "1913") => (in-any-order [jpmorgan exxon]) )
    (fact (older-companies "1913" :sort [(asc :name)]) => [exxon jpmorgan])
    (fact (older-companies "1999" :sort [(asc :date-founded) (asc :name)])
            => [jpmorgan exxon dell])
    (fact (modern-companies) => [dell])))

(deftest test-deffetch-one
  (fact (entity-spec-get Company :fetch-ones)
          => {:company-by-name company-by-name})
  (let [dell (create Company {:name "Dell" :date-founded (date 1984 11 4)})
        exxon (create Company {:name "Exxon" :date-founded "1911"})]
    (fact (company-by-name "Dell") => dell)))


(deftest test-find-and-*
  (let [foo (create Simple {:value "Foo"})
        expected (merge foo {:age 21})]
    (testing "find-and-modify"
      (fact (find-and-modify Simple (where (eq :value "Foo"))
                               (modify (set-fields {:age 21}))
                               :return-new true)
              => expected))
    (testing "find-and-remove"
      (fact (find-and-remove Simple (where (eq :value "Foo")))
              => expected))))

(deftest test-map-reduce
  (dotimes [n 5]
    (create Simple {:value n}))
  (fact (map-reduce-fetch-all Simple
                                "function() {emit('sum', this.value)}"
                                "function(k,vals) {
                                    var sum=0;
                                    for(var i in vals) sum += vals[i];
                                    return sum;
                                 }")
          => [{:_id "sum" :value (apply + (range 5))}]))

(deftest test-group
  (dotimes [n 4]
    (create Simple {:value n :name (if (odd? n) "odd" "even")}))
  (let [odds-and-evens (group Simple [:name])]
    (fact (count odds-and-evens) => 2)
    (let [[g1 g2] odds-and-evens]
      (facts (count (:values g1)) => 2
             (count (:values g2)) => 2)))
  (let [[odd-sum] (group Simple
                         [:name]
                         (where (eq :name "odd"))
                         {:sum 0 :count 0}
                         (js (fn [obj prev]
                               (set! prev.sum (+ prev.sum obj.value))
                               (set! prev.count (+ prev.count 1))))
                         (js (fn [result]
                               (set! result.avg (/ result.sum result.count)))))]
    (fact (:sum odd-sum) => 4)
    (fact (:count odd-sum) => 2)))

(deftest test-reference-list-maintain-order
  (let [employees (doall (map #(create Person {:last-name %})
                              ["e1" "e2" "e3"]))
        company (save (reduce (fn [c e] (relate c :employees e))
                              (make Company {:name "Big Swifty"})
                              employees))]
    (fact (grab company :employees) => employees)
    (let [shuffled (shuffle (grab company :employees))
          modified-company (save (set-references company :employees shuffled))]
    (fact (grab modified-company :employees) => shuffled))))
