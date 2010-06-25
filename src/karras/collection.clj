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

(ns karras.collection
  (:use [clojure.contrib.def :only [defnk]]
        [karras.core :only [*mongo-db* to-dbo to-clj]])
  (:import [com.mongodb Mongo DB DBCollection DBObject]
           [org.bson.types ObjectId]))

(defn- keyword-str [kw]
  (if (keyword? kw)
    (name kw)
    (str kw)))

(defn- has-option? [options k]
  (boolean (some #{k} options)))

(defn collection-db
  "Get the DB that this collection is associated with."
  [#^DBCollection collection]
  (.getDB collection))

(defn collection
  "Returns a DBCollection."
  ([collection-name]
     (collection *mongo-db* collection-name))
  ([#^DB db collection-name]
     (.getCollection db (keyword-str collection-name)))
  ([#^Mongo mongo db-name collection-name]
     (collection (.getDB mongo (keyword-str db-name))
                 collection-name)))

(defn drop-collection
  ""
  [#^DBCollection collection]
  (.drop collection))

(defn save
  "Saves a document to a colection, does an upsert behind the scenes.
   Returns the object with :_id if it was inserted.."
  [#^DBCollection collection obj]
  (let [dbo (to-dbo obj)]
    (.save collection dbo)
    (to-clj dbo)))

(defn insert
  "Inserts one or more documents into a collection. 
   Returns the inserted object with :_id"
  [#^DBCollection collection & objs]
  (let [inserted (doall (map #(save collection %) objs))]
    (if (= 1 (count objs))
      (first inserted)
      inserted)))

(defn update
  "Updates one or more documents in a collection that match the query with the document 
   provided.
     :upsert, performs an insert if the document doesn't have :_id
     :multi, update all documents that match the query"
  [#^DBCollection collection query obj & options]
  (let [o? #(has-option? options %)]
    (.update collection
             (to-dbo query)
             (to-dbo obj)
             (o? :upsert)
             (o? :multi))))

(defn upsert
  "Shortcut for (update collection query obj :upsert)"
  ([collection obj]
     (upsert collection obj obj))
  ([collection query obj]
     (update collection query obj :upsert)))

(defn update-all
  "Shortcut for (update collection query obj :multi)"
  ([collection obj]
     (update-all collection {} obj))
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
  [#^DBCollection collection query
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
                                (apply merge (reverse sort))))
                   cursor)]
    (if count
      (.count cursor)
      (map to-clj cursor))))

(defn fetch-all
  "Fetch all the documents of a collection. Same options as fetch."
  [collection & options]
  (apply fetch collection nil options))

(defn fetch-one
  "Fetch one document of a collection. Supports same options as fetch except :limit and :count"
  [collection query & options]
  (first (apply fetch collection query options)))

(defn count-docs
  "Returns the count of documents, optionally, matching a query"
  ([#^DBCollection collection]
     (count-docs collection {}))
  ([#^DBCollection collection query]
     (fetch collection query :count true)))

(defn fetch-by-id
  "Fetch a document by :_id"
  [#^DBCollection collection #^String s]
  (to-clj (.findOne collection (ObjectId. s))))

(defn distinct-values
  "Fetch a seq of the distinct values of a given collection for a key."
  [#^DBCollection collection kw]
  (set (.distinct collection (name kw))))

(defn group
  "Fetch a seq of grouped items.
     Example:
       SQL: select a,b,sum(c) csum from coll where active=1 group by a,b
       Karras: (group coll
                      [:a :b] 
                      {:active 1}
                      {:csum 0}
                      \"function(obj,prev) { prev.csum += obj.c; }\")"
  ([#^DBCollection collection keys]
     (group collection keys nil {:values []}
            "function(obj,prev) {prev.values.push(obj)}"))
  ([#^DBCollection collection keys cond initial reduce]
     (group collection keys cond initial reduce nil))
  ([#^DBCollection collection keys cond initial reduce finalize]
     (let [cmd {:ns (.getName collection)
                :key (zipmap (map name keys) (repeat true))
                :cond cond
                :initial initial
                :$reduce reduce}
           cmd (merge cmd (when finalize {:finalize finalize}))
           response (.command (collection-db collection)
                              (to-dbo {:group cmd}))]
       (.throwOnError response)
       (to-clj (.get response "retval")))))


(defn delete
  "Removes documents matching the supplied queries from a collection."
  [#^DBCollection collection & queries]
  (doseq [q queries]
    (.remove collection (to-dbo q))))

(defn ensure-index
  "Ensure an index exist on a collection."
  [#^DBCollection collection fields]
  (.ensureIndex collection (to-dbo fields)))

(defn ensure-unique-index
  "Ensure a unique index exist on a collection."
  [#^DBCollection collection #^String name fields]
  (.ensureIndex collection
                #^DBObject (to-dbo fields)
                name
                true))

(defn ensure-named-index
  "Ensure an index exist on a collection with the given name."
  [#^DBCollection collection #^String name fields]
  (.ensureIndex collection
                #^DBObject (to-dbo fields)
                name))

(defn drop-index
  ""
  [#^DBCollection collection o]
  (.dropIndex collection #^DBObject (to-dbo o)))

(defn drop-index-named
  ""
  [#^DBCollection collection kw]
  (.dropIndex collection (name kw)))

(defn list-indexes
  ""
  [#^DBCollection collection]
  (map to-clj (.getIndexInfo collection)))
