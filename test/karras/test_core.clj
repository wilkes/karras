(ns karras.test-core
  (:use karras.core :reload)
  (:use clojure.test
        midje.semi-sweet)
  (:import [com.mongodb BasicDBObject]))

(deftest test-to-dbo
  (testing "primitives"
    (expect (to-dbo nil) => nil)
    (expect (to-dbo 1) => 1)
    (expect (to-dbo \a) => \a)
    (expect (to-dbo "a") => "a"))
  (testing "maps"
    (expect (to-dbo {:a 1 :b 2}) => (doto (BasicDBObject.)
                                      (.put "a" 1)
                                      (.put "b" 2))))
  (testing "list"
    (expect (to-dbo [1 2]) => [1 2])))

(deftest test-to-clj
  (testing "primitives"
    (expect (to-clj nil) => nil)
    (expect (to-clj 1) => 1)
    (expect (to-clj \a) => \a)
    (expect (to-clj "a") => "a"))
  (testing "maps"
    (expect (to-clj (doto (BasicDBObject.)
                      (.put "a" 1)
                      (.put "b" 2)))
            => {:a 1 :b 2}))
  (testing "list"
    (expect (to-clj [1 2]) => [1 2])))


(deftest test-db-bulder
  (expect (str (build-dbo :a 1 :b 2)) => "{ \"a\" : 1 , \"b\" : 2}"))
