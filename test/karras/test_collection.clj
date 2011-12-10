(ns karras.test-collection
  (:use karras.core
        karras.collection :reload-all
        karras.sugar
        clojure.test
        midje.sweet))

(defonce indexing-tests-db (mongo-db :integration-tests))
(defonce people (collection indexing-tests-db :people))

(def sample-people [{:first-name "Bill"  :last-name "Smith"   :age 21}
                    {:first-name "Sally" :last-name "Jones"   :age 18}
                    {:first-name "Jim"   :last-name "Johnson" :age 16}
                    {:first-name "Jane"  :last-name "Johnson" :age 16}])

(doseq [p sample-people]
  (eval `(declare ~(symbol (:first-name p)))))

(defn person-by-name [n]
  (fetch-one people {:first-name n}))

(def ^:dynamic Bill  nil)
(def ^:dynamic Sally nil)
(def ^:dynamic Jim   nil)
(def ^:dynamic Jane  nil)

(background
 (around :facts
         (do
           (drop-collection people)
           (apply insert people sample-people)
           (binding [Bill (person-by-name "Bill")
                     Sally (person-by-name "Sally")
                     Jim (person-by-name "Jim")
                     Jane (person-by-name "Jane")]
             (write-concern-strict (collection-db people))
             (in-request (collection-db people) ?form)))))


(facts "count-docs"
  (fact (count-docs people) => 4))

(facts "fetching"
  (facts
    (count (fetch-all people)) => 4
    (count (fetch people (where (gte :age 18)))) => 2
    (fetch-one people (where (eq :first-name "Bill"))) => Bill
    (fetch-by-id people (-> Bill :_id)) => Bill
    (fetch-by-id people (-> Bill :_id str)) => Bill))

(facts "distinct-values"
  (fact (distinct-values people :age) => #{21 18 16}))

(fact
  (build-fields-subset [:foo :bar] nil) => {:foo 1 :bar 1}
  (build-fields-subset nil [:foo :bar]) => {:foo 0 :bar 0}
  (build-fields-subset [:foo :bar] [:ignored :fields]) => {:foo 1 :bar 1}
  (build-fields-subset [:foo (slice :bar [0 10])] nil)
  => {:foo 1
      :bar {:$slice [0 10]}}
  (build-fields-subset [:foo (slice :bar [0 10]) :bar._id] nil)
  => {:foo 1
      :bar {:$slice [0 10]}
      :bar._id 1})

(fact "group by a key"
  (group people [:age])
  => (in-any-order [{:age 21 :values [Bill]}
                    {:age 18 :values [Sally]}
                    {:age 16 :values [Jim Jane]}]))

(fact "group by multiple keys"
  (group people [:age :last-name])
  => (in-any-order [{:age 21 :last-name "Smith"   :values [Bill]}
                    {:age 18 :last-name "Jones"   :values [Sally]}
                    {:age 16 :last-name "Johnson" :values [Jim Jane]}]))

(fact "group and count"
  (group people
         [:last-name]
         nil
         {:count 0}
         "function (o,out) { out.count++ }")
  => (in-any-order [{:last-name "Johnson" :count 2.0 }
                    {:last-name "Smith" :count 1.0 }
                    {:last-name "Jones" :count 1.0 }]))

(fact "group and finalize"
  (group people
         [:last-name]
         nil
         {:age_sum 0 :count 0}
         "function (o,out) { out.count++; out.age_sum += o.age; }"
         "function (out) {out.avg_age = out.age_sum / out.count}")
  => (in-any-order [{:last-name "Johnson"
                     :age_sum 32.0 :count 2.0 :avg_age 16.0}
                    {:last-name "Smith"
                     :age_sum 21.0 :count 1.0 :avg_age 21.0}
                    {:last-name "Jones"
                     :age_sum 18.0 :count 1.0 :avg_age 18.0}]))

(facts "delete by document"
  (fetch-all people) => (in-any-order [Bill Sally])
  (against-background
    (before :checks (delete people Jim Jane))))

(facts "delete by where clause"
  (fetch-all people) => (in-any-order [Jim Jane])
  (against-background
    (before :checks (delete people (where (gte :age 17))))))


(fact (count-docs people) => 4
  (:weight (fetch-one people {:first-name "Jim"})) => 180
  (against-background
    (before :checks  (save people (merge Jim {:weight 180})))))


(fact "meta-data is preserved"
  (meta saved-doc) => (meta doc-with-meta)
  (against-background
    (around :facts
            (let [doc-with-meta (with-meta Jim {:some :data})
                  saved-doc (save people doc-with-meta)]
              ?form))))

(fact "updating-tests"
  (count-docs people) => 4
  (:weight (fetch-one people {:first-name "Jim"})) => 180
  (against-background
    (before :facts
            (update people {:first-name "Jim"}
                    (merge Jim {:weight 180})))))

(fact "upsert-update-test"
  (count-docs people) => 4
  (:weight (fetch-one people {:first-name "Jim"})) => 180
  (against-background
    (before :facts
            (upsert people {:first-name "Jim"}
                    (merge Jim {:weight 180})))))

(fact "upsert-insert-test"
  (count-docs people) => 5
  (against-background
    (before :facts
            (upsert people {:first-name "Arnold"}
                    (merge (dissoc Jim :_id) {:first-name "Arnold"})))))

(facts "update-all-tests"
  (doseq [j (fetch people {:last-name "Johnson"})]
    (:age j) => 17)
  (against-background
    (before :facts
            (update-all people {:last-name "Johnson"}
                        (modify (set-fields {:age 17}))))))
(facts "find-and-modify"
       (fact  "return unmodified document"
              (find-and-modify people
                               (where (eq :age 18))
                               (modify (set-fields {:voter true}))
                               :return-new false)
              => Sally)

       (fact "return modified document"
             (find-and-modify people
                              (where (eq :age 18))
                              (modify (set-fields {:voter false})))
             => (merge Sally {:voter false}))

       (fact "can upsert"
             (find-and-modify people
                              (where (eq :first-name "Chewbacca"))
                              (modify (set-fields {:voter false}))
                              :upsert true)
             => (contains {:first-name "Chewbacca"
                           :voter false
                           :_id (comp not nil?)}))

       (fact "but doesn't by default"
             (find-and-modify people
                              (where (eq :first-name "Han"))
                              (modify (set-fields {:voter false})))
             => (throws com.mongodb.CommandResult$CommandFailure))

       (fact "supports the fields argument"
             (find-and-modify people
                              (where (eq :age 18))
                              (modify (set-fields {:voter false}))
                              :fields [:first-name :voter])
             => (just {:_id (comp not nil?)
                       :first-name "Sally"
                       :voter false}))

       (fact "sorting"
             (find-and-modify people
                              (where (eq :age 16))
                              (modify (set-fields {:driver true}))
                              :sort [(asc :last-name) (asc :first-name)]
                              :return-new false)
             => Jane)
       (fact "update 'position of the matched array item in the query'"
             (find-and-modify people
                              (where (eq :age 16)
                                     (eq :update-me 3))
                              (modify (incr (matched :update-me))))
             => (contains {:update-me [1 2 4]})
             (against-background
              (before :checks
                      (find-and-modify people
                                       (where (eq :age 16))
                                       (modify (set-fields {:update-me [1 2 3]})))))))

(let [do-update #(find-and-modify people
                                  (where (eq :age 16))
                                  (modify (add-to-set :sample-set 1)))]
  (fact "add-to-set"
    (do-update) => (contains {:sample-set [1]})
    (do-update) => (contains {:sample-set [1]})))

(fact "return removed  document"
  (find-and-remove people (where (eq :age 18))) => Sally)

(fact "map-reduce:simple counting" (:ok results) => 1.0
  (:counts results) => {:output 1, :emit 4, :input 4}
  (:timing results) => not-nil?
  (:timeMillis results) => not-nil?
  (:result results) => not-nil?
  (fetch-map-reduce-values results) => [{:value 4.0}]
  (fetch-map-reduce-values results (where (eq :values 3))) => empty?
  (against-background
    (around :facts
            (let [not-nil? (comp not nil?)
                  results (map-reduce people
                                      "function() {emit(this.last_name, 1)}"
                                      "function(k,vals) {
                                        var sum=0;
                                        for(var i in vals) sum += vals[i];
                                        return sum;
                                     }"
                                      :out "_mr_counting_test")]
              ?form))))

(fact
  (collection-db-name people) => "integration-tests")

(fact
  (collection-name people) => "people")
