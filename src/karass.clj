(ns karras
  (:use [clojure.contrib.def :only [defnk]])
  (:import [com.mongodb Mongo DB DBCollection BasicDBObject
            DBObject DBCursor DBAddress MongoOptions
            ObjectId]))

(defn db-object [#^clojure.lang.IPersistentMap m]
  (let [to-s #(if (keyword? %) (name %) (str %))
        to-v #(if (map? %) (db-object %) %)
        r (BasicDBObject.)]
    (doseq [[k v] m]
      (.put r #^String (to-s k) #^Object (to-v v)))
    r))

(defn clojure-object [#^DBObject dbo]
  (apply merge (map (fn [#^java.util.Map$Entry e]
                       (let [k (.getKey e)
                             v (.getValue e)]
                         {(keyword k)
                          (if (instance? DBObject v)
                            (clojure-object v)
                            v)}))
                    (seq dbo))))

(defn connect [host & [port]]
  (Mongo. #^String host (or port 27017)))

(defn mongo-db [connection name]
  (.getDB connection name))

(defn collection
  ([db collection-name]
     (.getCollection db collection-name))
  ([mongo db-name collection-name]
     (collection (.getDB mongo db-name) collection-name)))

(defn insert [#^DBCollection collection & objs]
  (if (= 1 (count objs))
    (clojure-object
     (.insert collection #^DBObject (db-object (first objs))))
    (map clojure-object
         (.insert collection #^java.util.List (map db-object objs)))))

(defn update [#^DBCollection collection query obj & [upsert? multi?]]
  (clojure-object (.update collection
                           #^DBObject (db-object query)
                           #^DBObject (db-object obj)
                           (boolean upsert?)
                           (boolean multi?))))

(defn upsert [#^DBCollection collection query obj]
  (update collection query obj true))

(defn update-all [#^DBCollection collection query obj]
  (update collection query obj false true))

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

(defnk fetch [#^DBCollection collection query :limit nil :skip nil :include nil :exclude nil :order-by nil :count false]
  (search* collection
           query
           (merge (apply include-keys include) (apply exclude-keys exclude))
           limit
           skip
           order-by
           count))

(defnk fetch-all [#^DBCollection collection :limit nil :skip nil :order-by nil :count false]
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
  (seq (.distinct collection #^String (name s))))

(defn delete [#^DBCollection collection obj]
  (.remove collection (db-object obj)))

(defn asc [k]  {k 1})
(defn desc [k] {k -1})

(defn query [& clauses]
  (apply merge clauses))

(defn lt         [x y]   {x {:$lt  y}})
(defn gt         [x y]   {x {:$gt  y}})
(defn lte        [x y]   {x {:$lte y}})
(defn gte        [x y]   {x {:$gte y}})
(defn eq         [x y]   {x y})
(defn within     [x y z] {x {:$gt y :$lt z}})
(defn ne         [x y]   {x {:$ne y}})
(defn in         [x & y] {x {:$in y}})
(defn not-in     [x & y] {x {:$nin y}})
(defn eq-mod     [x m v] {x {:$mod [m v]}})
(defn all        [x & y] {x {:$all y}})
(defn size       [x y]   {x {:$size y}})
(defn exist?     [x]     {x {:$exist true}})
(defn not-exist? [x]     {x {:$exist false}})
(defn where      [x]     {:where x})