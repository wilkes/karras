(ns karras.document
  (:require karras)
  (:use karras.sugar
        [clojure.contrib.def :only [defnk defalias]]))

(defonce docspecs (atom {}))

(defn docspec [type]
  (get @docspecs type))

(defn docspec-value [type key]
     (get (docspec type) key))

(defrecord DocumentType [record-class
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

(defmulti convert :type)

(defmethod convert java.util.List
  [field-spec vals]
  (map #(convert (assoc field-spec :type (:of field-spec)) %) vals))

(defmethod convert :default
  [_ val]
  val)

(defn make [type hmap]
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

(defn- make-mongo-type [classname is-entity? fields type-fns]
  `(do
     (defrecord ~classname [])
     (swap! docspecs assoc
            ~classname
            (DocumentType. ~classname
                           ~is-entity?
                           ~(parse-fields fields)
                           ~(if is-entity? (last (.split (str classname) "\\.")) nil)))
     (extend ~classname EntityCallbacks default-callbacks)
     (defmethod convert ~classname [field-spec# val#]
                (make (:type field-spec#) val#))
     ~@(map (fn [f] `(-> ~classname ~f)) type-fns)))

(defmacro defaggregate [classname fields & type-fns]
  (make-mongo-type classname false fields type-fns))

(defmacro defentity [classname fields & type-fns]
  (make-mongo-type classname true fields type-fns))

(defn field-spec-of [type & keys]
  (let [field-type (or (reduce (fn [t k]
                                 (-> t docspec :fields k :type))
                               type
                               (butlast keys))
                       type)
        last-key (last keys)]
    (-> field-type docspec :fields last-key)))

(defn docspec-of [type & keys]
  (docspec (:type (apply field-spec-of type keys))))

(defn docspec-of-item [type & keys]
  (docspec (:of (apply field-spec-of type keys))))

(defn docspec-assoc [type & kvs]
  (swap! docspecs assoc type (apply assoc (docspec type) kvs)))

(defn collection-for [entity-or-type]
  (let [type (if (instance? Class entity-or-type) entity-or-type (class entity-or-type))]
    (karras/collection (-> type docspec :collection-name))))

(defn ensure-type [type entity]
  (if (= type (class entity))
    entity
    (make type entity)))

(defn save
  ([entity]
     (let [update?  (-> entity :_id nil? not)
           before-cb (if update? before-update before-create)
           after-cb (if update? after-update after-create)]
       (->> entity
            before-cb
            before-save
            (karras/save (collection-for entity))
            (ensure-type (class entity))
            after-save
            after-cb)))
  ([entity & entities]
     (doall (map save (cons entity entities)))))

(defn create [type hmap]
  (save (make type hmap)))

(defalias update save)

(defn delete
  ([entity]
     (let [do-delete #(do (karras/delete (collection-for entity) (where (eq :_id (:_id entity))))
                          %)]
       (->> entity
            before-delete
            do-delete
            after-delete)))
  ([entity & entities]
     (doall (map delete (cons entity entities)))))

(defn delete-all
  ([type]
     (delete-all type {}))
  ([type query]
     (karras/delete (collection-for type) query)))

(defnk fetch [type query
              :limit nil :skip nil :include nil :exclude nil :sort nil :count false]
  (map #(make type %) (karras/fetch (collection-for type)
                                    query
                                    :include include
                                    :exclude exclude
                                    :limit   limit  
                                    :skip    skip   
                                    :sort    sort   
                                    :count   count)))

(defnk fetch-all [type
                  :limit nil :skip nil :include nil :exclude nil :sort nil :count false]
  (map #(make type %) (karras/fetch-all (collection-for type)
                                        :include include
                                        :exclude exclude
                                        :limit   limit  
                                        :skip    skip   
                                        :sort    sort   
                                        :count   count)))

(defnk fetch-one [type query
                  :limit nil :skip nil :include nil :exclude nil :sort nil :count false]
  (make type (karras/fetch-one (collection-for type)
                               query
                               :include include
                               :exclude exclude
                               :limit   limit  
                               :skip    skip   
                               :sort    sort   
                               :count   count)))

(defn count-instances
  ([type]
     (count-instances type nil))
  ([type query]
     (karras/fetch (collection-for type) query :count true)))

(defn distinct-values [type kw]
  (karras/distinct-values (collection-for type) kw))

(defn index [type & keys]
  (let [indexes (get (docspec type) :indexes [])]
    (docspec-assoc type :indexes (conj indexes (apply compound-index keys)))))

(defn ensure-indexes
  ([]
     (doseq [type (keys@docspecs)]
       (ensure-indexes type)))
  ([type]
     (doseq [idx (docspec-value type :indexes)]
       (karras/ensure-index (collection-for type) idx))))

(defn list-indexes [type]
  (karras/list-indexes (collection-for type)))

