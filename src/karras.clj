(ns karras
  (:use [clojure.contrib.def :only [defnk]])
  (:import [com.mongodb Mongo DB DBCollection BasicDBObject
            DBObject DBCursor DBAddress MongoOptions
            ObjectId]))

(defn has-option? [options k]
  (boolean (some #{k} options)))

(defn db-object [m]
  (let [to-s #(if (keyword? %) (name %) (str %))
        to-v #(if (map? %) (db-object %) %)
        dbo  (BasicDBObject.)]
    (doseq [[k v] m]
      (.put dbo (to-s k) (to-v v)))
    dbo))

(defn clojure-object [dbo]
  (apply merge (map (fn [#^java.util.Map$Entry e]
                       (let [k (.getKey e)
                             v (.getValue e)]
                         {(keyword k)
                          (if (instance? DBObject v)
                            (clojure-object v)
                            v)}))
                    (seq dbo))))

(defn connect [#^String host & [port]]
  (Mongo. host (int (or port 27017))))

(defn mongo-db [#^Mongo connection #^String name]
  (.getDB connection name))

(defn collection
  ([#^DB db collection-name]
     (.getCollection db collection-name))
  ([#^Mongo mongo db-name collection-name]
     (collection (.getDB mongo db-name) collection-name)))

(defn drop-collection [#^DBCollection collection]
  (.drop collection))

(defn insert [#^DBCollection collection & objs]
  (if (= 1 (count objs))
    (clojure-object
     (.insert collection #^DBObject (db-object (first objs))))
    (map clojure-object
         (.insert collection #^java.util.List (map db-object objs)))))

(defn update [#^DBCollection collection query obj & options]
  (let [o? #(has-option? options %)]
    (clojure-object (.update collection
                             (db-object query)
                             (db-object obj)
                             (o? :upsert)
                             (o? :multi)))))

(defn upsert [collection query obj]
  (update collection query obj :upsert))

(defn update-all [collection query obj]
  (update collection query obj :multi))

(defn include-keys [& keys]
  (apply merge (map (fn [k] {k 1}) (remove nil? keys))))

(defn exclude-keys [& keys]
  (apply merge (map (fn [k] {k 0}) (remove nil? keys))))

(defn search* [#^DBCollection collection query keys limit skip order-by count?]
  (let [cursor (if query
                 (if keys
                   (.find collection #^DBObject (db-object query) #^DBObject (db-object keys))
                   (.find collection #^DBObject (db-object query)))
                 (.find collection ))
        cursor (if limit (.limit cursor limit) cursor)
        cursor (if skip (.skip cursor skip) cursor)
        cursor (if order-by (.sort cursor (db-object (apply merge order-by))) cursor)
        cursor (if count? (.count cursor) cursor)]
    (map clojure-object cursor)))

(defnk fetch [collection query :limit nil :skip nil :include nil :exclude nil :order-by nil :count false]
  (search* collection
           query
           (merge (apply include-keys include) (apply exclude-keys exclude))
           limit
           skip
           order-by
           count))

(defnk fetch-all [collection :limit nil :skip nil :order-by nil :count false]
  (search* collection
           nil
           nil
           limit
           skip
           order-by
           count))

(defn fetch-by-id [#^DBCollection collection #^String s]
  (.findOne collection (ObjectId. s)))

(defn distinct-values [#^DBCollection collection s]
  (seq (.distinct collection (name s))))

(defn delete [#^DBCollection collection obj]
  (.remove collection (db-object obj)))

(defn asc [k]  {k 1})
(defn desc [k] {k -1})

(defn query [& clauses]
  (apply merge clauses))

(defn lt         [field y]   {field {:$lt  y}})
(defn gt         [field y]   {field {:$gt  y}})
(defn lte        [field y]   {field {:$lte y}})
(defn gte        [field y]   {field {:$gte y}})
(defn eq         [field y]   {field y})
(defn within     [field y z] {field {:$gt y :$lt z}})
(defn ne         [field y]   {field {:$ne y}})
(defn in         [field & y] {field {:$in y}})
(defn not-in     [field & y] {field {:$nin y}})
(defn eq-mod     [field m v] {field {:$mod [m v]}})
(defn all        [field & y] {field {:$all y}})
(defn size       [field y]   {field {:$size y}})
(defn exist?     [field]     {field {:$exists true}})
(defn not-exist? [field]     {field {:$exists false}})
(defn where      [field]     {:where field})

(defn modify [& clauses]
  (apply merge clauses))

(defn incr       [field & [amount]] {:$inc     {field (or amount 1)}})
(defn set-fields [& pairs]          {:$set     (apply hash-map pairs)})
(defn unset      [field]            {:$unset   {field 1}})
(defn push       [field value]      {:$push    {field value}})
(defn push-all   [field & values]   {:$push    {field values}})
(defn pop-last   [field]            {:$pop     {field 1}})
(defn pop-first  [field]            {:$pop     {field -1}})
(defn pull       [field value]      {:$pull    {field value}})
(defn pull-all   [field & values]   {:$pullAll {field values}})

(defn ensure-index
  [#^DBCollection collection fields & options]
  (let [o? #(has-option? options %)]
    (.ensureIndex collection #^DBObject
                  (db-object fields)
                  (boolean (o? :force))
                  (boolean (o? :unique)))))

(defn drop-index [#^DBCollection collection o]
  (.dropIndex collection #^DBObject (db-object o)))

(defn drop-index-named [#^DBCollection collection #^String s]
  (.dropIndex collection s))

