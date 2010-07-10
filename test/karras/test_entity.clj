(ns karras.test-entity
  (:require [karras.core :as karras])
  (:use karras.sugar
        karras.entity
        [karras.collection :only [drop-collection collection]]
        clojure.test))

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

(defn add-callback [s]
  (fn [e]
    (assoc e :called (conj (or (vec (:called e)) []) s))))

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
  (index (asc :birthday))
  (extend EntityCallbacks
    {:before-create (add-callback "before-create")
     :before-update (add-callback "before-update")
     :before-save (add-callback "before-save")
     :after-save (add-callback "after-save")
     :after-update (add-callback "after-update")
     :after-create (add-callback "after-create")
     :before-delete (add-callback "before-delete")
     :after-delete (add-callback "after-delete")}))

(defentity Company
  [:name
   :employees {:type :references :of Person}
   :ceo {:type :reference :of Person}
   :date-founded {:type ::my-date}]
  (defscope older-companies [date-str]
    (lte :date-founded date-str))
  (defscope modern-companies []
    (gt :date-founded "1980")))

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
                  (is (= (parse-fields fields) expected-parsed-fields)))]
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
  (is (not (nil? (entity-spec Address))))
  (is (not (nil? (entity-spec Phone))))
  (is (not (nil? (entity-spec Person)))))

(deftest test-entity-spec-in
  (is (= (entity-spec Address) (entity-spec-of Person :address)))
  (is (= (entity-spec java.util.List) (entity-spec-of Person :phones)))
  (is (= (entity-spec Phone) (entity-spec-of-item Person :phones)))
  (is (= (entity-spec Street) (entity-spec-of Person :address :street))))

(deftest test-entity-spec-in
  (is (= (entity-spec Address) (entity-spec-of Person :address)))
  (is (= (entity-spec java.util.List) (entity-spec-of Person :phones)))
  (is (= (entity-spec Phone) (entity-spec-of-item Person :phones)))
  (is (= (entity-spec Street) (entity-spec-of Person :address :street))))

(deftest test-collection-name
  (testing "default name"
    (is (= (:collection-name (entity-spec Person)) "people")))
  (testing "override name"
    (is (= (:collection-name (entity-spec Simple)) "simpletons"))))

(deftest test-make
  (let [phone (make Phone {:number "555-555-1212"})]
    (is (= Phone (class phone))))
  (let [address (make Address {:city "Nashville"
                               :street {:number "123"
                                        :name "Main St."}})]
    (is (= Address (class address)))
    (is (= Street (class (:street address)))))
  (let [person (make Person
                     {:first-name "John"
                      :last-name "Smith"
                      :birthday (date 1976 7 4)
                      :phones [{:number "123"}]
                      :address {:city "Nashville"
                                :street {:number "123"
                                         :name "Main St."}}})]
    (is (= Address (class (-> person :address))))
    (is (= Street (class (-> person :address :street))))
    (is (= Phone (class (-> person :phones first)))))
  (let [person (make Person {:last-name "Smith"})
        phone (make Phone {})]
    (is (= #{:last-name :blood-alcohol-level} (set (keys person))))
    (is (= 0.0 (:blood-alcohol-level person)))
    (is (= #{:country-code} (set (keys phone))))
    (is (= 1 (:country-code phone))))
   (let [person (make Person #^{:meta "data"} {:first-name "Jimmy"})]
     (is (= {:meta "data"} (meta person)) "preserves the metadata of original hash")))



(deftest test-crud
  (let [person (dissoc (create Person
                               {:first-name "John"
                                :last-name "Smith"
                                :birthday (date 1976 7 4)
                                :phones [{:number "123" :country-code 2}]
                                :address {:city "Nashville"
                                          :street {:number "123"
                                                   :name "Main St."}}})
                       :called)]
    (is (= Person (class person)))
    (is (= "1976-07-04" (:birthday person)))
    (is (not (nil? (:_id person))))
    (is (= (collection :people) (collection-for Person)))
    (is (= (collection :people) (collection-for person)))
    (is (= person (dissoc (fetch-one Person
                                     (where (eq :_id (:_id person))))
                          :called)))
    (is (= person (dissoc (first (fetch-all Person))
                          :called)))
    (is (= person (dissoc (first (fetch Person (where (eq :last-name "Smith"))))
                          :called)))
    (is (= 1 (count-instances Person)))
    (dotimes [x 5]
      (create Person {:first-name "John" :last-name (str "Smith" (inc x))}))
    (is (= "John" (first (distinct-values Person :first-name))))
    (is (= 6 (count-instances Person)))
    (delete person)
    (is (= 5 (count-instances Person)))
    (delete-all Person (where (eq :last-name "Smith1")))
    (is (= 4 (count-instances Person)))
    (delete-all Person)
    (is (= 0 (count-instances Person)))))

(deftest test-callback-protocol
  (are [callback e] (= e (callback e))
       before-create (Simple.)
       before-delete (Simple.)
       before-save (Simple.)
       before-update (Simple.)
       after-create (Simple.)
       after-delete (Simple.)
       after-save (Simple.)
       after-update (Simple.)))

(deftest test-callback-impls
  (let [person (create Person {:first-name "John" :last-name "Smith"})]
    (is (= ["before-create" "before-save" "after-save" "after-create"]
             (:called person)))
    (is (= ["before-update" "before-save" "after-save" "after-update"]
             (:called (save (dissoc person :called)))))
    (is (= ["before-delete" "after-delete"]
             (:called (delete (dissoc person :called)))))))

(deftest test-ensure-indexes
  (is (empty? (list-indexes Person)))
  (ensure-indexes)
  ;; 2 + _id index
  (is (= 3 (count (list-indexes Person)))))

(deftest test-references
  (testing "saving"
    (let [john (create Person {:first-name "John" :last-name "Smith"})
          jane (create Person {:first-name "Jane" :last-name "Doe"})
          company (-> (create Company {:name "Acme"})
                      (set-reference :ceo john)
                      (add-reference :employees jane)
                      save)]
      (is (= (:_id john) (:ceo company)))
      (is (= (:_id jane) (first (:employees company))))))
  (testing "reading"
    (let [company (fetch-one Company (where (eq :name "Acme")))
          john (get-reference company :ceo)
          [jane] (get-reference company :employees)]
      (is (= "Smith" (:last-name john)))
      (is (= "Doe" (:last-name jane)))))
  (testing "updating"
    (let [bill (create Person {:first-name "Bill" :last-name "Jones"})
          company (-> (fetch-one Company (where (eq :name "Acme")))
                      (add-reference :employees bill))
          [jane bill] (get-reference company :employees)]
      (is (= "Jane" (:first-name jane)))
      (is (= "Bill" (:first-name bill))))))

(deftest test-defscope
  (is (= {:older-companies older-companies
          :modern-companies modern-companies}
         (entity-spec-get Company :scopes)))
  (let [jpmorgan (create Company {:name "JPMorgan Chase & Co." :date-founded "1799"})
        dell (create Company {:name "Dell" :date-founded (date 1984 11 4)})
        exxon (create Company {:name "Exxon" :date-founded "1911"})]
    (is (= [jpmorgan] (older-companies "1800")))
    (is (=  #{jpmorgan exxon} (set (older-companies "1913"))))
    (is (=  [exxon jpmorgan] (older-companies "1913" :sort [(asc :name)])))
    (is (=  [jpmorgan exxon dell]
              (older-companies "1999" :sort [(asc :date-founded)
                                             (asc :name)])))
    (is (=  [dell] (modern-companies)))))


(deftest test-find-and-*
  (let [foo (create Simple {:value "Foo"})
        expected (merge foo {:age 21})]
    (is (= expected
           (find-and-modify Simple (where (eq :value "Foo"))
                            (modify (set-fields {:age 21}))
                            :return-new true)))
    (is (= expected
           (find-and-remove Simple (where (eq :value "Foo")))))))

(deftest test-map-reduce
  (dotimes [n 5]
    (create Simple {:value n}))
  (is (= [{:_id "sum" :value (apply + (range 5))}] 
           (map-reduce-fetch-all Simple
                                 "function() {emit('sum', this.value)}"
                                 "function(k,vals) {
                                                 var sum=0;
                                                 for(var i in vals) sum += vals[i];
                                                 return sum;
                                              }"))))
