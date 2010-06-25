(ns karras.test-collection
  (:use karras.core
        karras.collection
        karras.sugar
        clojure.test))

(defprotocol Setifiable
  "Convert sequential types to sets for simpler equality checks"
  (setify [x]))

(extend-protocol Setifiable
 java.util.Map
 (setify [m]
   (reduce (fn [result [k v]]
             (assoc result
               k (setify v)))
           {} m))

 java.util.List
 (setify [l]
   (set (map setify l)))

 Object
 (setify [o] o)

 nil
 (setify [n] nil))

(defonce indexing-tests-db (mongo-db :integration-tests))
(defonce people (collection indexing-tests-db :people))

(def sample-people [{:first-name "Bill"  :last-name "Smith"   :age 21 :count 1}
                    {:first-name "Sally" :last-name "Jones"   :age 18 :count 1}
                    {:first-name "Jim"   :last-name "Johnson" :age 16 :count 1}
                    {:first-name "Jane"  :last-name "Johnson" :age 16 :count 1}])

(doseq [p sample-people]
  (eval `(declare ~(symbol (:first-name p)))))

(defn person-by-name [n]
  (fetch-one people {:first-name n}))

(use-fixtures :each
              (fn [t]
                (drop-collection people)
                (apply insert people sample-people)
                (binding [Bill (person-by-name "Bill")
                          Sally (person-by-name "Sally")
                          Jim (person-by-name "Jim")
                          Jane (person-by-name "Jane")]
                  (write-concern-strict (collection-db people))
                  (in-request (collection-db people) (t)))))

(deftest fetching-tests
  (testing "count-docs"
    (is (= 4 (count-docs people))))
  (testing "fetchgin"
    (is (= 4 (count (fetch-all people))))
    (is (= 2 (count (fetch people (where (gte :age 18))))))
    (is (= Bill (fetch-one people (where (eq :first-name "Bill"))))))
  (testing "distinct-values"
    (is (= #{21 18 16} (distinct-values people :age)))))

(deftest grouping-tests
  (testing "group by a key"
    (is (= #{{:age 21.0 :values #{Bill}}
             {:age 18.0 :values #{Sally}}
             {:age 16.0 :values #{Jim Jane}}}
           (setify (group people [:age])))))
  (testing "group by multiple keys"
    (is (= #{{:age 21.0 :last-name "Smith" :values #{Bill}}
             {:age 18.0 :last-name "Jones" :values #{Sally}}
             {:age 16.0 :last-name "Johnson" :values #{Jim Jane}}}
           (setify (group people [:age :last-name]))))))
  (testing "group and count"
    (is (= #{{:last-name "Johnson" :count 2.0 }
             {:last-name "Smith" :count 1.0 }
             {:last-name "Jones" :count 1.0 }}
           (setify
            (group people
                   [:last-name]
                   nil
                   {:count 0}
                   "function (o,out) { out.count++ }")))))
  (testing "group and finalize"
    (is (= #{{:last-name "Johnson" :age_sum 32.0 :count 2.0 :avg_age 16.0}
             {:last-name "Smith" :age_sum 21.0 :count 1.0 :avg_age 21.0}
             {:last-name "Jones" :age_sum 18.0 :count 1.0 :avg_age 18.0}}
           (setify
            (group people
                   [:last-name]
                   nil
                   {:age_sum 0 :count 0}
                   "function (o,out) { out.count++; out.age_sum += o.age; }"
                   "function (out) {out.avg_age = out.age_sum / out.count}")))))

(deftest deleting-tests
  (testing "delete by document"
    (delete people Jim Jane)
    (is (= #{Bill Sally} (setify (fetch-all people)))))
  (testing "delete by where clause"
    (delete people (where (gte :age 17)))
    (is (empty? (fetch-all people)))))

(deftest saving-tests
  (save people (merge Jim {:weight 180}))
  (is (= 4 (count-docs people)))
  (is (= 180 (:weight (fetch-one people {:first-name "Jim"})))))

(deftest updating-tests
  (update people {:first-name "Jim"} (merge Jim {:weight 180}))
  (is (= 4 (count-docs people)))
  (is (= 180 (:weight (fetch-one people {:first-name "Jim"})))))

(deftest update-all-tests
  (is (= 4 (count-docs people)))
  (update-all people {:last-name "Johnson"} (modify (set-fields {:age 17})))
  (is (= 4 (count-docs people)))
  (let [johnsons (fetch people {:last-name "Johnson"})]
    (is (= 2 (count johnsons)))
    (doseq [j johnsons]
      (is (= 17 (:age j))))))

(deftest indexing-tests
  (is (= 1 (count (list-indexes people)))) ;; _id is always indexed
    
  (ensure-index people (asc :age))
  (is (= [{:key {:_id 1}, :ns "integration-tests.people", :name "_id_"}
          {:key {:age 1}, :ns "integration-tests.people", :name "age_1"}]
         (list-indexes people)))
    
  (drop-index people (asc :age))
  (is (= 1 (count (list-indexes people))))
    
  (ensure-unique-index people "unique-first-name" (asc :first-name))
  (is (= [{:key {:_id 1}, :ns "integration-tests.people", :name "_id_"}
          {:key {:first-name 1}, :unique true, :ns "integration-tests.people",
           :name "unique-first-name"}]
           (list-indexes people))))

