(ns
    #^{:author "Wilkes Joiner"
       :doc "
A library for defining entities and aggregates.  Entities correspond to a mongo entity.
Aggregates correspond to a an embedded entity.

Example:

  (defaggregate Address [:street :city :state :zip])

  (defaggregate Phone [:area-code :number]

  (defentity Person
    [:first-name 
     :last-name
     :address {:type Address}
     :phones {:type :list :of Phone}])
"}
  karras.entity
  (:require [karras.collection :as c])
  (:use karras.sugar
        [clojure.contrib.def :only [defnk defalias defvar]]
        [clojure.contrib.str-utils2 :only [lower-case]]
        inflections))

(defrecord EntitySpec
  [record-class
   is-entity?
   fields
   collection-name])

(defonce entity-specs (atom {}))

(defn entity-spec
  "Lookup the EntitySpec for a given type"
  [type]
  (get @entity-specs type))

(defn entity-spec-get
  "Lookup the EntitySpec value for given key of the given type."
  [type key & [default]]
  (get (entity-spec type) key default))

(defn entity-spec-get-in
  "Lookup the EntitySpec value for given key of the given type."
  [type path]
  (get-in (entity-spec type) path))

(defn parse-fields [ks]
  (loop [attrs ks
         results {}]
    (if (empty? attrs)
      results
      (let [[x y & zs] attrs]
        (cond
         (and (keyword? x) (map? y)) (recur zs (assoc results x y))
         (and (keyword? x) (nil? y)) (recur zs  (assoc results x {}))
         (keyword? x)                (recur (cons y zs) (assoc results x {}))
         :else (throw (IllegalArgumentException. (str x " is not a keyword or a map"))))))))

(defn- assoc-meta-cache [v]
  (with-meta v (merge {:cache (atom nil)} (meta v))))

(declare make)
(defmulti convert
  "Multimethod used to convert a entity.
   Takes in a field-spec and a val and returns the conver
   Dispatches off of :type key of first arg.
   :default returns the value unmodified"
  :type)

(defmethod convert :list
  [field-spec vals]
  (map #(convert (assoc field-spec :type (:of field-spec)) %) vals))

(defmethod convert :reference
  [field-spec val]
  (assoc-meta-cache (make (:of field-spec) val :no-defaults)))

(defmethod convert :references
  [field-spec vals]
  (assoc-meta-cache (map #(make (:of field-spec) % :no-defaults) vals)))

(defmethod convert :default
  [_ val]
  val)

(defn make
  "Converts a hashmap to the supplied type."
  [#^Class type hmap & [no-defaults]]
  (let [fields (-> type entity-spec :fields)
        has-key? (fn [e k]
                   (try (some #{k} (keys e))
                        (catch Exception _ nil)))
        make-default? #(and (nil? no-defaults) (:default %))]
    (reduce (fn [entity [k field-spec]]
              (cond
               (has-key? entity k)   (assoc entity k (convert field-spec (k hmap)))
               (make-default? field-spec) (assoc entity k (:default field-spec))
               :otherwise            entity))
            (with-meta (merge (.newInstance type) hmap) (meta hmap))
            fields)))

(defn- default-collection-name [classname]
  (pluralize (lower-case (last (.split (str classname) "\\.")))))

(defn- make-mongo-type [classname is-entity? fields type-fns]
  `(do
     (defrecord ~classname [])
     (swap! entity-specs assoc
            ~classname
            (EntitySpec. ~classname
                      ~is-entity?
                      ~(parse-fields fields)
                      ~(if is-entity?
                         (default-collection-name classname)
                         nil)))
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
                                 (-> t entity-spec :fields k :type))
                               type
                               (butlast keys))
                       type)
        last-key (last keys)]
    (-> field-type entity-spec :fields last-key)))

(defn entity-spec-of
  "Given a type and one or more keys,
   lookup of the entity-spec for the :type of the last field supplied."
  [type & keys]
  (entity-spec (:type (apply field-spec-of type keys))))

(defn entity-spec-of-item
  "Given a type and one or more keys,
   lookup of the entity-spec for the :of of the last field supplied."
  [type & keys]
  (entity-spec (:of (apply field-spec-of type keys))))

(defn entity-spec-assoc
  "Associate the suppled keys and values with the entity-spec for the given type."
  [type & kvs]
  (swap! entity-specs assoc type (apply assoc (entity-spec type) kvs)))

(defn swap-entity-spec-in!
  "Combines swap! and assoc-in to modify the attribute of a entity-spec."
  [type attribute-path f & args]
  (swap! entity-specs
         (fn [specs]
           (let [path (cons type attribute-path)
                 current-value (get-in specs path)]
             (assoc-in specs path (apply f current-value args))))))

(defn collection-for
  "Returns the DBCollection for the supplied entity instance or type."
  [entity-or-type]
  (let [type (if (instance? Class entity-or-type)
               entity-or-type
               (class entity-or-type))]
    (c/collection (-> type entity-spec :collection-name))))

(defn ensure-type
  "Force an entity to be of a given type if is not already."
  [type entity]
  (if (= type (class entity))
    entity
    (make type entity)))

(defn save
  "Inserts or updates one or more entities."
  ([entity]
     (->> entity
          (c/save (collection-for entity))
          (ensure-type (class entity))))
  ([entity & entities]
     (doall (map save (cons entity entities)))))

(defn create
  "Makes a new instance of type and saves it."
  [type hmap]
  (save (make type hmap)))

(defn update
  "Updates one or more documents in a collection that match the criteria with the document 
   provided.
     :upsert, performs an insert if the document doesn't have :_id
     :multi, update all documents that match the criteria
   If working with an instance of an entity, use the save function."
  [type criteria modifiers & options]
  (apply c/update (collection-for type) criteria modifiers options))

(defn update-all
  "Shortcut for (update type criteria obj :multi)"
  ([type obj]
     (update-all type {} obj))
  ([type criteria obj]
     (update type criteria obj :multi)))

(defn delete-all
  "Deletes all entitys given an optional where clause."
  ([type]
     (delete-all type {}))
  ([type where]
     (c/delete (collection-for type) where)))

(defn delete
  "Deletes one or more entities."
  ([entity]
     (delete-all entity (where (eq :_id (:_id entity)))))
  ([entity & entities]
     (doall (map delete (cons entity entities)))))

(defn fetch
  "Fetch a seq of entities for the given type matching the supplied parameters."
  [type criteria & options]
  (map #(make type %) (apply c/fetch (collection-for type) criteria options)))

(defn fetch-all
  "Fetch all of the entities for the given type."
  [type & options]
  (apply fetch type nil options))

(defn fetch-one
  "Fetch the first entity for the given type matching the supplied criteria and options."
  [type criteria & options]
  (first (apply fetch type criteria options)))

(defn fetch-by-id
  "Fetch an enity by :_id"
  ([entity]
     (fetch-by-id (class entity) (:_id entity)))
  ([type id]
      (c/fetch-by-id (collection-for type) id)))

(defn count-instances
  "Return the number of entities optionally matching a given where clause."
  ([type]
     (count-instances type nil))
  ([type criteria]
     (c/count-docs (collection-for type) criteria)))

(defn distinct-values
  "Return the distinct values of a given type for a given key."
  [type kw]
  (c/distinct-values (collection-for type) kw))

(defn group
  "Fetch a seq of grouped items.
     Example:
       SQL: select a,b,sum(c) csum from coll where active=1 group by a,b
       Karras: (group MyType
                      [:a :b] 
                      {:active 1}
                      {:csum 0}
                      \"function(obj,prev) { prev.csum += obj.c; }\")"
  ([type keys]
     (c/group (collection-for type) keys))
  ([type keys cond initial reduce]
     (group type keys cond initial reduce nil))
  ([type keys cond initial reduce finalize]
     (c/group (collection-for type) keys cond initial reduce finalize)))

(defn find-and-modify
  "See http://www.mongodb.org/display/DOCS/findandmodify+Command"
  [type criteria modifier & options]
  (make type (apply c/find-and-modify (collection-for type) criteria modifier options)))

(defn find-and-remove
  "See http://www.mongodb.org/display/DOCS/findandmodify+Command"
  [type criteria & options]
  (make type (apply c/find-and-remove (collection-for type) criteria options)))

(defn map-reduce
  "See http://www.mongodb.org/display/DOCS/MapReduce"
  [type mapfn reducefn & options]
  (apply c/map-reduce (collection-for type) mapfn reducefn options))

(defn fetch-map-reduce-values
  "Takes the result of map-reduce and fetches the values. Takes the same options as fetch."
  [map-reduce-result & fetch-options]
  (apply c/fetch-map-reduce-values map-reduce-result fetch-options))

(defvar map-reduce-fetch-all (comp fetch-map-reduce-values map-reduce)
  "Composes map-reduce and fetch-map-reduce-values and returns all the results.
   If you need to filter the results use:
     (fetch-map-reduce-values (map-reduce ...) ...your fetch options...")

(defn index
  "Associate an index with a give type."
  [type & keys]
  (swap-entity-spec-in! type [:indexes] conj (apply compound-index keys)))

(defn ensure-indexes
  "Ensure the indexes are built, optionally for a given type. Defaults to all types."
  ([]
     (doseq [type (keys @entity-specs)]
       (ensure-indexes type)))
  ([type]
     (doseq [idx (entity-spec-get type :indexes)]
       (c/ensure-index (collection-for type) idx))))

(defn list-indexes
  ""
  [type]
  (c/list-indexes (collection-for type)))

(defn- make-reference [entity]
  (let [coll (collection-for entity)]
    {:_db (.getName (c/collection-db coll)) :_id (:_id entity) :_ref (.getName coll)}))

(defn add-reference
  "Add one more :_id's to a sequence of the given key"
  [entity k & vs]
  (if-not (remove nil? (map :_id vs))
    (throw (IllegalArgumentException. "All references must have an :_id.")))
  (assoc entity k (assoc-meta-cache (concat (or (k entity) []) (map make-reference vs)))))

(defn set-references
  [entity k vs]
  (apply add-reference (assoc entity k []) k vs))

(defn set-reference
  "Set an :_id to the key of the given entity"
  [entity k v]
  (if-not (:_id v)
    (throw (IllegalArgumentException. "Reference must have an :_id.")))
  (assoc entity k (assoc-meta-cache (make-reference v))))

(defn get-reference
  "Fetch the entity or entities referrenced by the given key."
  [entity k]
  (let [field-spec (field-spec-of (class entity) k)
        target-type (:of field-spec)
        list? (= (:type field-spec) :references)]
    (if list?
      (map #(fetch-one target-type (where (eq :_id (:_id %)))) (k entity))
      (fetch-one target-type (where (eq :_id (:_id (k entity))))))))

(defn grab
  "Analogous to clojure.core/get except that it will follow references.
   References are cached in a :cache atom of the references metadata.
   Takes an optional refresh flag to force it to fetch the reference."
  [parent k & refresh]
  (let [field-spec (field-spec-of (class parent) k)
        val (get parent k)]
    (if (some #{(:type field-spec)} [:reference :references])
      (if-let [cached (and (not refresh) (-> val meta :cache deref))]
        cached
        (let [result (get-reference parent k)]
          (swap! (:cache (meta val)) (fn [_] result))
          result))
      val)))

(defn grab-in
  "Analogous to clojure.core/get-in except that it will follow references.
   References are cached in a :cache atom of the references metadata.
   Takes an optional refresh flag to force it to fetch any references
   along the path."
  [parent ks & refresh]
  (reduce #(grab %1 %2 refresh) parent ks))

(defn relate
  "Relates an entity to another entity as a reference. Returns the parent with
   the reference associated.  No-op if the target field is not a :reference or 
   :references. If the the key is a list it will add the reference, otherwise it
   will set the reference. References are automatically saved if they haven't 
   been already. References are stored on the entity as:
     {:_db \"db-name\" :_id ObjectId :_ref \"collection-name\")}"
  [parent k & vs]
  (let [field-spec (field-spec-of (class parent) k)
        related-type (:of field-spec)
        saved-entities (map #(if (:_id %)
                               %
                               (create related-type %))
                            vs)]
    (if (= :reference (:type field-spec))
      (set-reference parent k (first saved-entities))
      (if (= :references (:type field-spec))
        (apply add-reference parent k saved-entities)
        parent))))

(defmacro create-with [type hmap & relations]
  `(-> (make ~type ~hmap)
       ~@relations
       save))

(defn fetch-refers-to
  "Given an entity, a type and a field, fetch all the entities of the given type
   that refer to the given entity. Takes the same options as fetch. "
  [entity referrer-type referrer-field & options]
  (let [field-spec (field-spec-of referrer-type referrer-field)
        id (:_id entity)
        criteria (if (= :reference (:type field-spec))
                   (where
                    (eq (str (name referrer-field) "._id") id))
                   (where
                    (element-match referrer-field (eq :_id id))))]
    (apply fetch referrer-type criteria options)))

(defn make-fetch
  [fetch-fn spec-key type fn-name args criteria]
  `(do
     (defn ~fn-name
       [~@args & options#]
       (let [and-clauses# (:and (apply hash-map options#))]
         (apply ~fetch-fn  ~type
                (where ~@criteria and-clauses#)
                options#)))
     (swap-entity-spec-in! ~type [~spec-key] assoc ~(keyword fn-name) ~fn-name)))

(defmacro deffetch
  "Defines a fetch function for the given type. 
   The function created takes all of the options of fetch plus an :and option to append to the where clause.

   Usage:
     (defentity Person [:first-name :last-name :age]
        (deffetch adults [] (gte :age 18))
        (deffetch peope-in-age-range [min max] (within :age min max)))

   Outside of defentity:
     (deffetch Person teenagers [] (within :age 13 19))

   Give me all the teenagers with last names A-J sorted by last name:
     (teenagers :and (within :last-name \"A\" \"J\") :sort [(asc :last-name)])

   Give me the youngest 10 people between the ages of 21 and 100 sorted by age and last name:      
     (peope-in-age-range 21 100 :limit 10 :sort [(asc :age) (asc :last-name)])"
  [type fn-name [& args] & criteria]
  (make-fetch 'fetch :fetchs type fn-name args criteria))

(defmacro deffetch-one
  "Defines a fetch-one function for the given type. 
   The function created takes all of the options of fetch-one plus an :and option to append to the where clause.

   Usage:
     (defentity Person [:first-name :last-name :age]
        (deffetch-one person-by-fullname
          [first-name last-name]
          (eq :first-name first-name)
          (eq :last-name last-name)))

   Fetch a person by name:
     (person-by-name \"John\" \"Smith\")"
  [type fn-name [& args] & criteria]
  (make-fetch 'fetch-one :fetch-ones type fn-name args criteria))
