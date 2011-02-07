(ns karras.test-entity
  (:require [karras.core :as karras]
            [karras.collection :as c])
  (:use karras.entity :reload-all)
  (:use karras.sugar
        [karras.collection :only [collection]]
        clojure.test
        midje.sweet
        karras.entity.testing
        clojure.pprint
        [com.reasonr.scriptjure :only [js]]))

(defmethod convert ::my-date
  [field-spec d]
  (if (instance? java.util.Date d)
    (.format (java.text.SimpleDateFormat. "yyyy-MM-dd") d)
    d))

(defembedded Street
  [:name
   :number])

(defembedded Address
  [:street {:type Street}
   :city
   :state
   :postal-code])

(defembedded Phone
  [:country-code {:default 1}
   :number])

(defentity Person
  [:name
   :birthday {:type ::my-date}
   :counter {:default 0}
   :address {:type Address}
   :phones {:type :list :of Phone}])


(deftest test-parse-fields
  (facts
   (parse-fields nil) => {}
   (parse-fields [:no-type]) => {:no-type {}}
   (parse-fields [:with-type {:type Integer}]) => {:with-type {:type Integer}}
   (parse-fields [:no-type
                  :with-type {:type Integer}]) => {:no-type {}
                                                   :with-type {:type Integer}}
                  (parse-fields [:with-type {:type Integer}
                                 :no-type]) => {:with-type {:type Integer}
                                                :no-type {}}))

(deftest test-entity-spec-in
  (facts
   (entity-spec-of Person :address) => (entity-spec Address)
   (entity-spec-of Person :phones) => (entity-spec java.util.List)
   (entity-spec-of-item Person :phones) => (entity-spec Phone)
   (entity-spec-of Person :address :street) => (entity-spec Street)))

(deftest test-default-collection-name-is-plural
  (fact (:collection-name (entity-spec Person)) => "people"))


(defentity Simple
  [:value]
  (entity-spec-assoc :collection-name "simpletons"))

(deftest test-collection-name-is-overridable
  (fact (:collection-name (entity-spec Simple)) => "simpletons"))

(deftest test-make
  (fact
   (class (make Phone {})) => Phone)
  (let [address (make Address {:city "Nashville"
                               :street {:number "123"
                                        :name "Main St."}})]
    (facts (class address) => Address
           (class (:street address)) => Street))
  (testing "complex nested with defaults"
    (let [person (make Person
                       {:name "John Smith"
                        :birthday (date 1976 7 4)
                        :phones [{:number "123"}]
                        :address {:city "Nashville"
                                  :street {:number "123"
                                           :name "Main St."}}})]
      (facts
       (-> person :address class) => Address
       (-> person :address :street class) => Street
       (-> person :phones first class)=> Phone
       (-> person :counter) =>  0.0
       (-> person :phones first :country-code) => 1)))
  (let [person (make Person #^{:meta "data"} {:first-name "Jimmy"})]
    (facts (meta person) => {:meta "data"})))

(deftest test-create
  (fact
   (create ...type... ...data...) => ...saved...
   (provided
    (make ...type... ...data...) => ...made...
    (save ...made...) => ...saved...)))

(deftest test-fetch
  (fact
   (fetch ...type... ...criteria...) => [...result...]
   (provided
    (c/fetch (collection-for ...type...) ...criteria...) => [...fetched...]
    (make ...type... ...fetched...) => ...result...))
  (fact
   (fetch ...type... ...criteria... ...opt1... ...opt2...) => [...result...]
   (provided
    (c/fetch (collection-for ...type...) ...criteria... ...opt1... ...opt2...) => [...fetched...]
    (make ...type... ...fetched...) => ...result...)))

(deftest test-fetch-all
  (fact
   (fetch-all ...type...) => ...result...
   (provided
    (fetch ...type... nil) => ...result...)))

(deftest test-fetch-one
  (fact
   (fetch-one ...type... ...criteria... ...opt1... ...opt2...) => ...result...
   (provided
    (fetch ...type... ...criteria... ...opt1... ...opt2...) => [...result...])))

(deftest test-fetch-by-id
  (fact
   (fetch-by-id ...type... ...id...) => ...made...
   (provided
    (c/fetch-by-id (collection-for ...type...) ...id...) => ...fetched-entity...
    (make ...type... ...fetched-entity...) => ...made...))
  (fact
   (fetch-by-id ...type... ...id...) => nil
   (provided
    (c/fetch-by-id (collection-for ...type...) ...id...) => nil)))
    ;;(not-called make))))

(deftest test-update
  (fact
   (update ...type... ...criteria... ...modifiers... ...opt1... ...opt2...)
   => ...results...
   (provided
    (c/update (collection-for ...type...) ...criteria... ...modifiers... ...opt1... ...opt2...) => ...results...)))

(deftest test-update-all
  (fact
   (update-all ...type... ...obj...) => ...result...
   (provided
    (update ...type... {} ...obj... :multi) => ...result...))
  (fact
   (update-all ...type... ...criteria... ...obj...) => ...result...
   (provided
    (update ...type... ...criteria... ...obj... :multi) => ...result...)))

(comment "fails with an stack overflow"
  (deftest test-save
    (fact
      (save ...entity...) => ...result...
      (provided
        (collection-for ...entity...) => ...collection...
        (c/save ...collection... ...entity...) => ...saved...
        (class ...entity...) => ...class...
        (ensure-type ...class... ...saved...) => ...result...))))

(deftest test-delete
  (let [entity1 {:_id 1}
        entity2 {:_id 2}]
    (fact
     (delete entity1) => ...result...
     (provided
      (delete-all entity1 {:_id 1}) => ...result...))
    (fact
     (delete entity1 entity2) => [...result1... ...result2...]
     (provided
      (delete-all entity1 {:_id 1}) => ...result1...
      (delete-all entity2 {:_id 2}) => ...result2...))))

(deftest test-delete-all
  (fact
   (delete-all ...type...) => ...result...
   (provided
    (c/delete (collection-for ...type...) {}) => ...result...))
  (fact
   (delete-all ...type... ...conditions...) => ...result...
   (provided
    (c/delete (collection-for ...type...) ...conditions...) => ...result...)))

(defentity Typed [])
(deftest test-collection-for
  (fact
   (collection-for Typed) => ...collection...
   (provided
    (entity-spec-get Typed :collection-name) => ...collection-name...
    (c/collection ...collection-name...) => ...collection...))
  (fact
   (collection-for (make Typed nil)) => ...collection...
   (provided
    (entity-spec-get Typed :collection-name) => ...collection-name...
    (c/collection ...collection-name...) => ...collection...)))

(deftest test-ensure-indexes
  (fact
   (ensure-indexes ...type...) => nil
   (provided
    (entity-spec-get ...type... :indexes) => [...idx...]
    (c/ensure-index (collection-for ...type...) ...idx...) => nil)))

(deftest test-make-reference
  (let [entity {:_id 1}]
    (fact
     (make-reference entity) => {:_db ...db-name...
                                 :_id 1
                                 :_ref ...collection-name...}
     (provided
      (entity-db-name entity) => ...db-name...
      (entity-collection-name entity) => ...collection-name...))))

(deftest test-relate
  (let [entity (make Person {})]
    (fact
     (relate entity ...key... ...value...) => ...result...
     (provided
      (field-spec-of Person ...key...) => {:type :reference :of ...type...}
      (ensure-saved ...type... [...value...]) => [...value...]
      (set-reference entity ...key... ...value...) => ...result...))
    (fact
     (relate entity ...key... ...value1... ...value2...) => ...result...
     (provided
      (field-spec-of Person ...key...) => {:type :references :of ...type...}
      (ensure-saved ...type... [...value1... ...value2...]) =>  [...value1...
                                                                 ...value2...]
      (add-reference entity ...key... ...value1... ...value2...) => ...result...))
    (fact
     (relate entity ...key... ...value...) => entity
     (provided
      (field-spec-of Person ...key...) => {:type :not-reference :of ...type...}
      (ensure-saved ...type... [...value...]) => [...value...]))))

(deftest test-create-with
  (fact
   (create-with ...type...
                ...data...
                (relate ...field... ...value...)) => ...saved...
   (provided
    (make ...type... ...data...) => ...entity...
    (relate ...entity... ...field... ...value...) => ...related...
    (save ...related...) => ...saved...)))

(deftest test-get-reference
  (let [entity {:key '...ref...}]
    (fact
     (get-reference entity :key) => ...ref-value...
     (provided
      (field-spec-of entity :key) => {:type :reference :of ...ref-type...}
      (by-id ...ref...) => ...condition...
      (fetch-one ...ref-type... ...condition...) => ...ref-value...)))
  (let [entity {:key ['...ref...]}]
    (fact
     (get-reference entity :key) => [...ref-value...]
     (provided
      (field-spec-of entity :key) => {:type :references :of ...ref-type...}
      (by-id ...ref...) => ...condition...
      (fetch-one ...ref-type... ...condition...) => ...ref-value...))))

(deftest test-grab
  (let [entity {:key (with-meta '...ref... {:cache (atom nil)})}]
    (fact
     (grab entity :key) => ...ref-value...
     (provided
      (field-spec-of entity :key) => {:type :reference :of ...ref-type...}
      (get-reference entity :key) => ...ref-value...)))
  (let [entity {:key (with-meta ['...ref...] {:cache (atom nil)})}]
    (fact
     (grab entity :key) => [...ref-value...]
     (provided
      (field-spec-of entity :key) => {:type :references :of ...ref-type...}
      (get-reference entity :key) => [...ref-value...]))))

(deftest test-grab-in
  (fact
   (grab-in ...entity... [:key1 :child]) => ...child-value...
   (provided
    (grab ...entity... :key1 nil) => ...val1...
    (grab ...val1... :child nil) => ...child-value...)))


(deftest test-fetch-refers-to
  (let [entity {:_id '...id...}]
    (fact
     (fetch-refers-to entity ...referrer-type... :referrer-key) => [...referrer...]
     (provided
      (field-spec-of ...referrer-type... :referrer-key) => {:type :reference}
      (fetch ...referrer-type... {"referrer-key._id" ...id...}) => [...referrer...])))
  (let [entity {:_id '...id...}]
    (fact
     (fetch-refers-to entity ...referrer-type... :referrer-key) => [...referrer...]
     (provided
      (field-spec-of ...referrer-type... :referrer-key) => {:type :not-a-reference}
      (fetch ...referrer-type...
             (element-match :referrer-key {:_id ...id...})) => [...referrer...]))))

(deftest test-grab-caching
  (let [entity {:key (with-meta '...ref... {:cache (atom '...cached...)})}]
    (fact
     (grab entity :key) => ...cached...
     (provided
      (field-spec-of entity :key) => {:type :reference :of ...ref-type...})))
  (let [entity {:key (with-meta '...ref... {:cache (atom ['...cached...])})}]
    (fact
     (grab entity :key) => [...cached...]
     (provided
      (field-spec-of entity :key) => {:type :references :of ...ref-type...})))
  (let [entity {:key (with-meta '...ref... {:cache (atom '...cached...)})}]
    (fact
     (grab entity :key :refresh) => ...ref-value...
     (provided
      (field-spec-of entity :key) => {:type :reference :of ...ref-type...}
      (get-reference entity :key) => ...ref-value...))))


(defentity DefFetch
  []
  (deffetch no-args-fetch []
    (where (eq :key :value)))
  (deffetch with-args-fetch [arg1]
    (where (eq :key arg1)))
  (deffetch-one one-no-args-fetch []
    (where (eq :key :value)))
  (deffetch-one one-with-args-fetch [arg1]
      (where (eq :key :value))))

(deftest test-deffetch
  (fact
   (no-args-fetch) => ...result...
   (provided
    (fetch DefFetch {:key :value}) => ...result...))
  (fact
   (with-args-fetch :value) => ...result...
   (provided
    (fetch DefFetch {:key :value}) => ...result...))
  (fact
   (one-no-args-fetch) => ...result...
   (provided
    (fetch-one DefFetch {:key :value}) => ...result...))
  (fact
   (one-with-args-fetch :value) => ...result...
   (provided
    (fetch-one DefFetch {:key :value}) => ...result...))
  (fact
   (no-args-fetch :and (in :bar [1,2,3])) => ...result...
   (provided
    (fetch DefFetch (where (eq :key :value)
                           (in :bar [1,2,3]))) => ...result...))
  (fact
   (no-args-fetch :and (in :bar [1,2,3]) :opt1 :v1) => ...result...
   (provided
    (fetch DefFetch
           (where (eq :key :value)
                  (in :bar [1,2,3]))
           :opt1 :v1) => ...result...)))

(deftest test-find-and-modify
  (fact
   (find-and-modify ...type... ...criteria... ...modifiers...) => ...result...
   (provided
    (c/find-and-modify (collection-for ...type...) ...criteria... ...modifiers...) => ...found...
    (make ...type... ...found...) => ...result...)))

(deftest test-find-and-remove
  (fact
   (find-and-remove ...type... ...criteria...) => ...result...
   (provided
    (c/find-and-remove (collection-for ...type...) ...criteria...) => ...found...
    (make ...type... ...found...) => ...result...)))

(deftest test-map-reduce
  (fact
   (map-reduce ...type... ...mapfn... ...reducefn...) => ...map-reduce-result...
   (provided
    (c/map-reduce (collection-for ...type...) ...mapfn... ...reducefn...) => ...map-reduce-result...)))

(deftest test-fetch-map-reduce-values
  (fact
   (fetch-map-reduce-values ...map-reduce-result...) => ...all...
   (provided
    (c/fetch-map-reduce-values ...map-reduce-result...) => ...all...)))

(deftest test-group
  (fact
   (group ...type... ...keys...) => ...grouped...
   (provided
    (c/group (collection-for ...type...) ...keys...) => ...grouped...))
  (fact
   (group ...type... ...keys... ...criteria... ...initial... ...reduce...) => ...grouped...
   (provided
    (c/group (collection-for ...type...) ...keys... ...criteria... ...initial... ...reduce... nil) => ...grouped...))
  (fact
   (group ...type... ...keys... ...criteria... ...initial... ...reduce... ...finalize...) => ...grouped...
   (provided
    (c/group (collection-for ...type...) ...keys... ...criteria... ...initial... ...reduce... ...finalize...) => ...grouped...)))

