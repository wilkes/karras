(ns karras.test-sugar
  (:use karras.sugar
        clojure.test
        midje.semi-sweet))

(deftest test-slice
  (expect (slice :comments 5) => {:comments {:$slice 5}})
  (expect (slice :comments [20 10]) => {:comments {:$slice [20 10]}}))

(deftest test-or
  (expect (|| (eq :a 1)
              (eq :b 2))
          => {:$or [{:a 1} {:b 2}]}))

