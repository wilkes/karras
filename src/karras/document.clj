(ns karras.document)

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
  (before-destroy [e])
  (before-save [e])
  (before-update [e])
  (before-validate [e])
  (after-create [e])
  (after-destroy [e])
  (after-save [e])
  (after-update [e])
  (after-validate [e]))

(def default-callbacks
     {:before-create   identity 
      :before-destroy  identity 
      :before-save     identity 
      :before-update   identity 
      :before-validate identity 
      :after-create    identity 
      :after-destroy   identity 
      :after-save      identity 
      :after-update    identity 
      :after-validate  identity})

(defmulti convert :type)

(defmethod convert java.util.List [field-spec vals]
  (map #(convert (assoc field-spec :type (:of field-spec)) %) vals))

(defmethod convert :default [field-spec val]
  val)

(defn make [type hmap]
  (let [fields (-> type docspec :fields)]
    (reduce (fn [entity [k field-spec]]
              (assoc entity k (convert field-spec (k hmap))))
            (.newInstance type)
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


