(ns test-karras
  (:use karras
        clojure.test)
  (:import [com.mongodb BasicDBObject]))

(deftest test-to-dbo
  (testing "primitives"
    (is (= (BasicDBObject.) (to-dbo nil)))
    (is (= 1 (to-dbo 1)))
    (is (= \a (to-dbo \a)))
    (is (= "a" (to-dbo "a"))))
  (testing "maps"
    (is (= (doto (BasicDBObject.)
             (.put "a" 1)
             (.put "b" 2))
           (to-dbo {:a 1 :b 2}))))
  (testing "list"
    (is (= [1 2] (to-dbo [1 2])))))

(deftest test-to-clj
  (testing "primitives"
    (is (= nil (to-clj nil)))
    (is (= 1 (to-clj 1)))
    (is (= \a (to-clj \a)))
    (is (= "a" (to-clj "a"))))
  (testing "maps"
    (is (= {:a 1 :b 2}
           (to-clj (doto (BasicDBObject.)
                     (.put "a" 1)
                     (.put "b" 2))))))
  (testing "list"
    (is (= [1 2] (to-clj [1 2])))))

;(run-tests)
