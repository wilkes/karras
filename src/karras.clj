;; The MIT License
;;  
;; Copyright (c) 2010 Wilkes Joiner
;;  
;; Permission is hereby granted, free of charge, to any person obtaining a copy
;; of this software and associated documentation files (the "Software"), to deal
;; in the Software without restriction, including without limitation the rights
;; to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
;; copies of the Software, and to permit persons to whom the Software is
;; furnished to do so, subject to the following conditions:
;;  
;; The above copyright notice and this permission notice shall be included in
;; all copies or substantial portions of the Software.
;;  
;; THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
;; IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
;; FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
;; AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
;; LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
;; OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
;; THE SOFTWARE.

(ns karras
  (:use [clojure.contrib.def :only [defnk]])
  (:import [com.mongodb Mongo DB DBCollection BasicDBObject
            DBObject DBCursor DBAddress MongoOptions
            ObjectId]))

(defn- keyword-str [kw]
  (if (keyword? kw)
    (name kw)
    (str kw)))

(defn- has-option? [options k]
  (boolean (some #{k} options)))

(defmulti to-dbo class)

(defmethod to-dbo java.util.Map
  [m]
  (let [dbo  (BasicDBObject.)]
    (doseq [[k v] m]
      (.put dbo (keyword-str k) (to-dbo v)))
    dbo))

(defmethod to-dbo java.util.List
  [v]
  (map to-dbo v))

(defmethod to-dbo :default
  [v]
  v)

(defmulti to-clj class)

(defmethod to-clj java.util.Map
  [v]
  (apply merge
         (map (fn [#^java.util.Map$Entry e]
                {(keyword (.getKey e))
                 (to-clj (.getValue e))})
              (seq v))))

(defmethod to-clj java.util.List
  [v] 
  (map to-clj v))

(defmethod to-clj :default
  [v]
  v)

(defn connect
  "Returns a single server connection. Defaults to host 127.0.0.1 port 27017"
  ([] (connect "127.0.0.1"))
  ([host] (connect host 27017))
  ([#^String host port]
     (Mongo. host (int port))))

(defn mongo-db
  [#^Mongo connection db-name]
  (.getDB connection (keyword-str db-name)))

(defn drop-db [#^DBObject db]
  (.dropDatabase db))

(defn list-collections [#^Mongo db]
  (map keyword (.getCollectionNames db)))

(defn collection
  "Returns a DBCollection."
  ([#^DB db collection-name]
     (.getCollection db (keyword-str collection-name)))
  ([#^Mongo mongo db-name collection-name]
     (collection (.getDB mongo (keyword-str db-name))
                 collection-name)))

(defn drop-collection [#^DBCollection collection]
  (.drop collection))

(defn insert
  "Inserts one or more documents into a collection"
  [#^DBCollection collection & objs]
  (if (= 1 (count objs))
    (to-clj
     (.insert collection #^DBObject (to-dbo (first objs))))
    (map to-clj
         (.insert collection #^java.util.List (map to-dbo objs)))))

(defn save
  "Saves a document to a colection, does an upsert behind the scenes"
  [#^DBCollection collection & objs]
  (doseq [o objs]
    (.save collection (to-dbo o))))

(defn update
  "Updates one or more documents in a collection that match the query with the document 
   provided.
     :upsert, performs an insert if the document doesn't have :_id
     :multi, update all documents that match the query"
  [#^DBCollection collection query obj & options]
  (let [o? #(has-option? options %)]
    (to-clj (.update collection
                     (to-dbo query)
                     (to-dbo obj)
                     (o? :upsert)
                     (o? :multi)))))

(defn upsert
  "Shortcut for (update collection query obj :upsert)"
  [collection query obj]
  (update collection query obj :upsert))

(defn update-all
  "Shortcut for (update collection query obj :multi)"
  ([collection obj]
     (update-all collection nil obj))
  ([collection query obj]
      (update collection query obj :multi)))

(defnk fetch
  "Fetch a seq of documents that match a given query.
   Accepts the following keywords:
       :limit, maximum number of documents to return
       :skip, where in the result set the seq will begin, i.e. paging
       :include, which keys to include in the result set, can not be combined with :exclude
       :exclude, which keys to exclude from the result set, can not be combined with :include
       :sort, which keys to order by
       :count, if true return the count of the result set, defaults to false"
  [collection query
   :limit nil :skip nil :include nil :exclude nil :sort nil :count false]
  (let [cursor (if query
                 (if (or include exclude)
                   (let [keys (merge (zipmap (remove nil? include)
                                             (repeat 1))
                                     (zipmap (remove nil? exclude)
                                             (repeat 0)))]
                     (.find collection
                            #^DBObject (to-dbo query)
                            #^DBObject (to-dbo keys)))
                   (.find collection
                          #^DBObject (to-dbo query)))
                 (.find collection ))
        cursor (if limit
                 (.limit cursor limit)
                 cursor)
        cursor (if skip
                 (.skip cursor skip)
                 cursor)
        cursor (if sort
                 (.sort cursor (to-dbo
                                (apply merge sort)))
                   cursor)]
    (if count?
      (.count cursor)
      (map to-clj cursor))))

(defnk fetch-all
  "Fetch all the documents of a collection. Same options as fetch."
  [collection :limit nil :skip nil :include nil :exclude nil :sort nil :count false]
  (fetch collection
         nil
         include
         exclude
         limit
         skip
         sort
         count))

(defnk fetch-one
  "Fetch one document of a collection. Supports same options as fetch except :limit and :count"
  [collection query :skip nil :include nil :exclude nil :sort nil]
  (first (fetch collection
                query
                include
                exclude
                1
                skip
                sort
                false)))

(defn count-docs
  "Returns the count of documents, optionally, matching a query"
  ([collection]
     (count-docs collection nil))
  ([collection query]
     (fetch collection query :count true)))

(defn fetch-by-id
  "Fetch a document by :_id"
  [#^DBCollection collection #^String s]
  (to-clj (.findOne collection (ObjectId. s))))

(defn distinct-values
  "Fetch a seq of the distinct values of a given collection for a key."
  [#^DBCollection collection kw]
  (seq (.distinct collection (name kw))))

(defn group
  "Fetch a seq of grouped items.
     Example:
       SQL: select a,b,sum(c) csum from coll where active=1 group by a,b
       Karras: (group coll
                      [:a :b] 
                      {:active 1}
                      {:csum 0}
                      \"function(obj,prev) { prev.csum += obj.c; }\")
"
  [#^DBCollection collection keys cond initial reduce]
  (map to-clj (.group collection
                              (to-dbo (zipmap (map name keys)
                                                 (repeat true)))
                              (to-dbo cond)
                              (to-dbo initial)
                              #^String reduce)))

(defn delete
  "Remove a document from a collection."
  [#^DBCollection collection obj]
  (.remove collection (to-dbo obj)))

(defn ensure-index
  "Ensure an index exist on a collection
   Options:
     :force, force index creation, even if one exists
     :unique, require unique values for a field"
  [#^DBCollection collection fields & options]
  (let [o? #(has-option? options %)]
    (.ensureIndex collection #^DBObject
                  (to-dbo fields)
                  (boolean (o? :force))
                  (boolean (o? :unique)))))

(defn drop-index
  [#^DBCollection collection o]
  (.dropIndex collection #^DBObject (to-dbo o)))

(defn drop-index-named
  [#^DBCollection collection kw]
  (.dropIndex collection (name kw)))

(defn list-indexes [#^DBCollection collection]
  (map to-clj (.getIndexInfo collection)))

(defn eval-code [db code-str]
  (to-clj (.eval db code-str (into-array nil))))