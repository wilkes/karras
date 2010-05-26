(ns karras.test-document
  (:require [karras.core :as karras])
  (:use karras.sugar
        karras.document
        [karras.collection :only [drop-collection collection]]
        clojure.test))

(defonce db (karras/mongo-db :document-testing))

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
   :ceo {:type :reference :of Person}])

(defentity Simple
  [:value]
  (docspec-assoc :collection-name "simpletons"))

(use-fixtures :each (fn [t]
                      (karras/with-mongo-request db
                        (drop-collection (collection-for Person))
                        (drop-collection (collection-for Company))
                        (t))))

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
    (is (= (:collection-name (docspec Person)) "people")))
  (testing "override name"
    (is (= (:collection-name (docspec Simple)) "simpletons"))))

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
    (is (= 1 (:country-code phone)))))



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
    (is (= karras.test-document.Person (class person)))
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
