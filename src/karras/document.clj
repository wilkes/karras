(ns karras.document
  (:require karras)
  (:use karras.sugar
        [clojure.contrib.def :only [defnk defalias]]))

(def docspecs (atom {}))

(defn docspec [type]
  (get @docspecs type))

(defrecord DocumentType [record-class
                      is-entity?
                      fields
                      collection-name
                      validators])

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
  (before-validate [e])
  (after-create [e])
  (after-delete [e])
  (after-save [e])
  (after-update [e])
  (after-validate [e]))

(def default-callbacks
     (reduce (fn [defaults k] (assoc defaults k identity))
             {}
             (-> EntityCallbacks :method-map keys)))

(defmulti convert :type)

(defmethod convert java.util.List
  [field-spec vals]
  (map #(convert (assoc field-spec :type (:of field-spec)) %) vals))

(defmethod convert :default
  [_ val]
  val)

(defn make [type hmap]
  (let [fields (-> type docspec :fields)]
    (reduce (fn [entity [k field-spec]]
              (assoc entity k (convert field-spec (k hmap))))
            (merge (.newInstance type) hmap)
            fields)))

(defn- make-mongo-type [classname is-entity? fields]
  `(do
     (defrecord ~classname [])
     (swap! docspecs assoc
            ~classname
            (DocumentType. ~classname
                           ~is-entity?
                           ~(parse-fields fields)
                           ~(if is-entity? (last (.split (str classname) "\\.")) nil)
                           nil))
     (extend ~classname
             EntityCallbacks
             default-callbacks)
     (defmethod convert ~classname [field-spec# val#]
       (make (:type field-spec#) val#))))

(defmacro defaggregate [classname & fields]
  (make-mongo-type classname false fields))

(defmacro defentity [classname & fields]
  (make-mongo-type classname true fields))

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

(defn alter-entity-type! [type & kvs]
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
