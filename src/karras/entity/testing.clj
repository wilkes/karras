(ns karras.entity.testing
  (:use [karras.core :only [with-mongo-request]]
        [karras.entity :only [entity-specs collection-for]]
        [karras.collection :only [drop-collection collection]]))

(defn entity-fixture
  "creates a test fixture that wraps tests in a with-mongo-request and drops
   all the colllections for the known entities."
  [db]
  (fn [t]
    (with-mongo-request db
      (doseq [t (keys @entity-specs)]
       (drop-collection (collection-for t)))
     (t))))
