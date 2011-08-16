(ns karras.test-sugar
  (:use karras.sugar
        midje.sweet))

(fact
  (slice :comments 5) => {:comments {:$slice 5}}
  (slice :comments [20 10]))

(fact (|| (eq :a 1)
          (eq :b 2))
  => {:$or [{:a 1} {:b 2}]})

