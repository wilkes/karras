(ns karras.document
  (:require [karras.collection :as c])
  (:use karras.sugar
        [clojure.contrib.def :only [defnk defalias]]
        [clojure.contrib.str-utils2 :only [lower-case]]
        inflections))

(defonce docspecs (atom {}))

(defn docspec
  "Lookup the DocSpec for a given type"
  [type]
  (get @docspecs type))

(defn docspec-value
  "Lookup the DocSpec value for given key of the given type."
  [type key]
  (get (docspec type) key))

(defrecord DocSpec
  [record-class
   is-entity?
   fields
   collection-name])

(defn parse-fields [ks]
  (loop [attrs ks
         results {}]
    (if (empty? attrs)
      results
      (let [[x y & zs] attrs]
        (cond
         (and (keyword? x) (map? y)) (recur zs (assoc results x y))
         (and (keyword? x) (nil? y)) (recur zs  (assoc results x {:type String}))
         (keyword? x)                (recur (cons y zs) (assoc results x {:type String}))
         :else (throw (IllegalArgumentException. (str x " is not a keyword or a map"))))))))

(defprotocol EntityCallbacks
  "Lifecycle callbacks for Entities.
   All of these are required to take in an entity and return an entity.

  (before-create [e])
  (before-delete [e])
  (before-save [e])
  (before-update [e])
  (after-create [e])
  (after-delete [e])
  (after-save [e])
  (after-update [e])"
  (before-create [e])
  (before-delete [e])
  (before-save [e])
  (before-update [e])
  (after-create [e])
  (after-delete [e])
  (after-save [e])
  (after-update [e]))

(defonce default-callbacks
  (reduce #(assoc %1 %2 identity) {} (-> EntityCallbacks :method-map keys)))

(defmulti convert
  "Multimethod used to convert a document.
   Takes in a field-spec and a val and returns the conver
   Dispatches off of :type key of first arg.
   :default returns the value unmodified"
  :type)

(defmethod convert :list
  [field-spec vals]
  (map #(convert (assoc field-spec :type (:of field-spec)) %) vals))

(defmethod convert :default
  [_ val]
  val)

(defn make
  "Converts a hashmap to the supplied type."
  [type hmap]
  (let [fields (-> type docspec :fields)
        has-key? (fn [e k]
                   (try (some #{k} (keys e))
                        (catch Exception _ nil)))]
    (reduce (fn [entity [k field-spec]]
              (cond
               (has-key? entity k)   (assoc entity k (convert field-spec (k hmap)))
               (:default field-spec) (assoc entity k (:default field-spec))
               :otherwise            entity))
            (merge (.newInstance type) hmap)
            fields)))

(defn- default-collection-name [classname]
  (pluralize (lower-case (last (.split (str classname) "\\.")))))

(defn- make-mongo-type [classname is-entity? fields type-fns]
  `(do
     (defrecord ~classname [])
     (swap! docspecs assoc
            ~classname
            (DocSpec. ~classname
                      ~is-entity?
                      ~(parse-fields fields)
                      ~(if is-entity?
                         (default-collection-name classname)
                         nil)))
     (extend ~classname EntityCallbacks default-callbacks)
     (defmethod convert ~classname [field-spec# val#]
                (make (:type field-spec#) val#))
     ~@(map (fn [f] `(-> ~classname ~f)) type-fns)))

(defmacro defentity
  "Defines an entity that corresponds to a collection."
  [classname [& fields] & type-fns]
  (make-mongo-type classname true fields type-fns))

(defmacro defaggregate
  "Defines an aggregate type that can be embedded into an entity."
  [classname [& fields] & type-fns]
  (make-mongo-type classname false fields type-fns))

(defn field-spec-of
  "Given a type and one or more keys,
   returns the field spec for the last key."
  [type & keys]
  (let [field-type (or (reduce (fn [t k]
                                 (-> t docspec :fields k :type))
                               type
                               (butlast keys))
                       type)
        last-key (last keys)]
    (-> field-type docspec :fields last-key)))

(defn docspec-of
  "Given a type and one or more keys,
   lookup of the docspec for the :type of the last field supplied."
  [type & keys]
  (docspec (:type (apply field-spec-of type keys))))

(defn docspec-of-item
  "Given a type and one or more keys,
   lookup of the docspec for the :of of the last field supplied."
  [type & keys]
  (docspec (:of (apply field-spec-of type keys))))

(defn docspec-assoc
  "Associate the suppled keys and values with the docspec for the given type."
  [type & kvs]
  (swap! docspecs assoc type (apply assoc (docspec type) kvs)))

(defn collection-for
  "Returns the DBCollection for the supplied entity instance or type."
  [entity-or-type]
  (let [type (if (instance? Class entity-or-type)
               entity-or-type
               (class entity-or-type))]
    (c/collection (-> type docspec :collection-name))))

(defn ensure-type
  "Force an entity to be of a given type if is not already."
  [type entity]
  (if (= type (class entity))
    entity
    (make type entity)))

(defn save
  "Inserts or updates one or more entities."
  ([entity]
     (let [update?  (-> entity :_id nil? not)
           before-cb (if update? before-update before-create)
           after-cb (if update? after-update after-create)]
       (->> entity
            before-cb
            before-save
            (c/save (collection-for entity))
            (ensure-type (class entity))
            after-save
            after-cb)))
  ([entity & entities]
     (doall (map save (cons entity entities)))))

(defn create
  "Makes a new instance of type and saves it."
  [type hmap]
  (save (make type hmap)))

(defalias update save)

(defn delete
  "Deletes one or more entities."
  ([entity]
     (let [do-delete #(do (c/delete (collection-for entity) (where (eq :_id (:_id entity))))
                          %)]
       (->> entity
            before-delete
            do-delete
            after-delete)))
  ([entity & entities]
     (doall (map delete (cons entity entities)))))

(defn delete-all
  "Deletes all documents given an optional where clause."
  ([type]
     (delete-all type {}))
  ([type where]
     (c/delete (collection-for type) where)))

(defnk fetch
  "Fetch a seq of documents for the given type matching the supplied parameters."
  [type query
   :limit nil :skip nil :include nil :exclude nil :sort nil :count false]
  (map #(make type %) (c/fetch (collection-for type)
                                    query
                                    :include include
                                    :exclude exclude
                                    :limit   limit  
                                    :skip    skip   
                                    :sort    sort   
                                    :count   count)))

(defnk fetch-all
  "Fetch all of the documents for the given type."
  [type
   :limit nil :skip nil :include nil :exclude nil :sort nil :count false]
  (map #(make type %) (c/fetch-all (collection-for type)
                                        :include include
                                        :exclude exclude
                                        :limit   limit  
                                        :skip    skip   
                                        :sort    sort   
                                        :count   count)))

(defnk fetch-one
  "Fetch the first document for the given type matching the supplied query and options."
  [type query
   :limit nil :skip nil :include nil :exclude nil :sort nil :count false]
  (make type (c/fetch-one (collection-for type)
                               query
                               :include include
                               :exclude exclude
                               :limit   limit  
                               :skip    skip   
                               :sort    sort   
                               :count   count)))

(defn count-instances
  "Return the number of documents optionally matching a given where clause."
  ([type]
     (count-instances type nil))
  ([type query]
     (c/fetch (collection-for type) query :count true)))

(defn distinct-values
  "Return the distinct values of a given type for a given key."
  [type kw]
  (c/distinct-values (collection-for type) kw))

(defn index
  "Associate an index with a give type."
  [type & keys]
  (let [indexes (get (docspec type) :indexes [])]
    (docspec-assoc type :indexes (conj indexes (apply compound-index keys)))))

(defn ensure-indexes
  "Ensure the indexes are built, optionally for a given type. Defaults to all types."
  ([]
     (doseq [type (keys @docspecs)]
       (ensure-indexes type)))
  ([type]
     (doseq [idx (docspec-value type :indexes)]
       (c/ensure-index (collection-for type) idx))))

(defn list-indexes
  ""
  [type]
  (c/list-indexes (collection-for type)))

(defn add-reference
  "Add one more :_id's to a sequence of the given key"
  [entity k & vs]
  (if-not (remove nil? (map :_id vs))
    (throw (IllegalArgumentException. "All references must have an :_id.")))
  (assoc entity k (concat (or (k entity) []) (map :_id vs))))

(defn set-reference
  "Set an :_id to the key of the given entity"
  [entity k v]
  (if-not (:_id v)
    (throw (IllegalArgumentException. "Reference must have an :_id.")))
  (assoc entity k (:_id v)))

(defn get-reference
  "Fetch the entity or entities referrenced by the given key."
  [entity k]
  (let [field-spec (field-spec-of (class entity) k)
        target-type (:of field-spec)
        list? (= (:type field-spec) :references)]
    (if list?
      (fetch target-type (where (in :_id (k entity))))
      (fetch-one target-type (where (eq :_id (k entity)))))))
