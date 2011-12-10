(ns karras.def)

;; Stolen from clojure.contrib.def until I figure out how to support clojure 1.2 and clojure 1.3
(defmacro defvar
  "Defines a var with an optional intializer and doc string"
  ([name]
     (list `def name))
  ([name init]
     (list `def name init))
  ([name init doc]
     (list `def (with-meta name (assoc (meta name) :doc doc)) init)))
