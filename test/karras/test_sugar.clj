(ns karras.test-sugar
  (:use karras.sugar
        clojure.test
        midje.sweet))

(deftest test-slice
  (fact (slice :comments 5) => {:comments {:$slice 5}})
  (fact (slice :comments [20 10]) => {:comments {:$slice [20 10]}}))

(deftest test-or
  (fact (|| (eq :a 1)
            (eq :b 2))
        => {:$or [{:a 1} {:b 2}]}))

