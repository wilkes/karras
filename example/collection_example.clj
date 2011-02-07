(ns collection-example
  (:use karras.core
        karras.collection
        karras.sugar
        clojure.pprint))

(def my-collection (collection (connect) :my-database :my-collection))

;; clear the slate
(drop-collection my-collection)

;; load some data
(insert my-collection
        {:name "doc11" :author "author1" :counter 0 :size 10}
        {:name "doc12" :author "author1" :counter 0 :size 20}
        {:name "doc21" :author "author2" :counter 0 :size 30}
        {:name "doc22" :author "author2" :counter 0 :size 40}
        {:name "doc31" :author "author3" :counter 0 :size 50}
        {:name "doc32" :author "author3" :counter 0 :size 60})

;; add an index on the :size field
(ensure-index my-collection (asc :size))

;; add a compound index on namd and author
(ensure-index my-collection (compound-index (asc :name) (asc :author)))


(pprint (count-docs my-collection))
;; => 6

(pprint (count-docs my-collection (where (eq :author "author3"))))
;; => 2

;; print in reverse order by author then name
(pprint (fetch-all my-collection :sort [(desc :author) (desc :name)]))
;; => ({:_ns "my-collection",
;;  :size 60,
;;  :counter 0,
;;  :author "author3",
;;  :name "doc32",
;;  :_id #<ObjectId 4b5388873a1050bdd17b6dbe>}
;; ...

;; increment the counters for all the documents
(update-all my-collection (modify (incr :counter)))
(pprint (count-docs my-collection (where (gte :counter 1))))
;; => 6

;; count all the documents using group
(pprint (group my-collection
                 []
                 nil
                 {:csum 0}
                 "function(doc,result) { result.csum += doc.counter; }"))
;; => ({:csum 6.0})



;; add a new tags field as list, push "big" to list and increment the counter
(update-all my-collection (where (gte :size 30))
            (modify (push :tags "big")
                    (incr :counter)))

;; group by author and sum the counters
(pprint (group my-collection
               [:author]
               (where (exist? :tags))
               {:csum 0}
               "function(doc,result) { result.csum += doc.counter; }"))
;; => ({:csum 4.0, :author "author2"} {:csum 4.0, :author "author3"})
