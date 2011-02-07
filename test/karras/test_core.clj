(ns karras.test-core
  (:use karras.core :reload)
  (:use clojure.test
        midje.sweet)
  (:import [com.mongodb BasicDBObject]))

(deftest test-to-dbo
  (testing "primitives"
    (fact (to-dbo nil) => nil
          (to-dbo 1) => 1
          (to-dbo \a) => \a
          (to-dbo "a") => "a"))
  (testing "maps"
    (fact (to-dbo {:a 1 :b 2}) => (doto (BasicDBObject.)
                                      (.put "a" 1)
                                      (.put "b" 2))))
  (testing "list"
    (fact (to-dbo [1 2]) => [1 2]))
  (testing "lazy seq"
    (fact (to-dbo (lazy-seq [1 2])) => [1 2])))

(deftest test-to-clj
  (testing "primitives"
    (fact (to-clj nil) => nil
          (to-clj 1) => 1
          (to-clj \a) => \a
          (to-clj "a") => "a"))
  (testing "maps"
    (fact (to-clj (doto (BasicDBObject.)
                      (.put "a" 1)
                      (.put "b" 2)))
            => {:a 1 :b 2}))
  (testing "list"
    (fact (to-clj [1 2]) => [1 2]))
  (testing "lazy seqs"
    (fact (to-clj (lazy-seq [1 2])) => [1 2])))


(deftest test-db-bulder
  (fact (str (build-dbo :a 1 :b 2)) => "{ \"a\" : 1 , \"b\" : 2}"))
