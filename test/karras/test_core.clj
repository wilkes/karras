(ns karras.test-core
  (:use karras.core :reload)
  (:use midje.sweet)
  (:import [com.mongodb BasicDBObject]))


(fact "primitives"
  (to-dbo nil) => nil
  (to-dbo 1) => 1
  (to-dbo \a) => \a
  (to-dbo "a") => "a")

(fact "maps"
  (to-dbo {:a 1 :b 2}) => (doto (BasicDBObject.)
                            (.put "a" 1)
                            (.put "b" 2)))
(fact "list"
  (to-dbo [1 2]) => [1 2])

(fact "lazy seq"
  (to-dbo (lazy-seq [1 2])) => [1 2])


(fact "primitives" (to-clj nil) => nil
  (to-clj 1) => 1
  (to-clj \a) => \a
  (to-clj "a") => "a")

(fact "maps"
  (to-clj (doto (BasicDBObject.)
            (.put "a" 1)
            (.put "b" 2)))
  => {:a 1 :b 2})

(fact "list"
  (to-clj [1 2]) => [1 2])

(fact "lazy seqs"
  (to-clj (lazy-seq [1 2])) => [1 2])


(fact
  (str (build-dbo :a 1 :b 2)) => "{ \"a\" : 1 , \"b\" : 2}")
