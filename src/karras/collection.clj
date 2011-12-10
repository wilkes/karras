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
  (:use [karras.core :only [*mongo-db* to-dbo to-clj build-dbo]]
        [karras.def :only [defvar]])
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

(defn collection-db-name
  "Get the name of the DB that this collection is associated with."
  [#^DBCollection collection]
  (.getName (collection-db collection)))

(defn collection-name
  "Get the name of the collection."
  [collection]
  (.getName collection))

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
  [#^DBCollection coll]
  (.drop coll))

(defn save
  "Saves a document to a colection, does an upsert behind the scenes.
   Returns the object with :_id if it was inserted.."
  [#^DBCollection coll obj]
  (let [dbo (to-dbo obj)]
    (.save coll dbo)
    (-> dbo to-clj (with-meta (meta obj)))))

(defn insert
  "Inserts one or more documents into a collection.
   Returns the inserted object with :_id"
  [#^DBCollection coll & objs]
  (let [inserted (doall (map #(save coll %) objs))]
    (if (= 1 (count objs))
      (first inserted)
      inserted)))

(defn update
  "Updates one or more documents in a collection that match the criteria with the document
   provided.
     :upsert, performs an insert if the document doesn't have :_id
     :multi, update all documents that match the criteria"
  [#^DBCollection coll criteria obj & options]
  (let [o? #(has-option? options %)]
    (.update coll
             (to-dbo criteria)
             (to-dbo obj)
             (o? :upsert)
             (o? :multi))))

(defn upsert
  "Shortcut for (update collection criteria obj :upsert)"
  ([coll obj]
     (upsert coll obj obj))
  ([coll criteria obj]
     (update coll criteria obj :upsert)))

(defn update-all
  "Shortcut for (update collection criteria obj :multi)"
  ([coll obj]
     (update-all coll {} obj))
  ([coll criteria obj]
     (update coll criteria obj :multi)))

(defn build-fields-subset [include exclude]
  (let [make-map #(apply merge
                   (zipmap (remove map? %1) (repeat %2))
                   (filter map? %1))]
    (if include
      (make-map include 1)
      (make-map exclude 0))))

(defn fetch
  "Fetch a seq of documents that match a given criteria.
   Accepts the following keywords:
       :limit, maximum number of documents to return
       :skip, where in the result set the seq will begin, i.e. paging
       :include, which keys to include in the result set, can not be combined with :exclude
       :exclude, which keys to exclude from the result set, can not be combined with :include
       :sort, which keys to order by
       :count, if true return the count of the result set, defaults to false"
  [#^DBCollection coll criteria
   & {:keys [limit skip include exclude sort count]
      :or {count false}}]
  (let [cursor (if criteria
                 (if (or include exclude)
                   (let [keys (build-fields-subset include exclude)]
                     (.find coll
                            #^DBObject (to-dbo criteria)
                            #^DBObject (to-dbo keys)))
                   (.find coll
                          #^DBObject (to-dbo criteria)))
                 (.find coll ))
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
  [coll & options]
  (apply fetch coll nil options))

(defn fetch-one
  "Fetch one document of a collection. Supports same options as fetch except :limit and :count"
  [coll criteria & options]
  (first (apply fetch coll criteria options)))

(defn count-docs
  "Returns the count of documents, optionally, matching a criteria"
  ([#^DBCollection coll]
     (count-docs coll {}))
  ([#^DBCollection coll criteria]
     (fetch coll criteria :count true)))

(defn fetch-by-id
  "Fetch a document by :_id"
  [#^DBCollection coll id]
  (let [id (if (= ObjectId (class id)) id (ObjectId. id))]
    (to-clj (fetch-one coll {:_id id}))))

(defn distinct-values
  "Fetch a seq of the distinct values of a given collection for a key."
  [#^DBCollection coll kw]
  (set (.distinct coll (name kw))))

(defn group
  "Fetch a seq of grouped items.
     Example:
       SQL: select a,b,sum(c) csum from coll where active=1 group by a,b
       Karras: (group coll
                      [:a :b]
                      {:active 1}
                      {:csum 0}
                      \"function(obj,prev) { prev.csum += obj.c; }\")"
  ([#^DBCollection coll keys]
     (group coll keys nil {:values []}
            "function(obj,prev) {prev.values.push(obj)}"))
  ([#^DBCollection coll keys cond initial reduce]
     (group coll keys cond initial reduce nil))
  ([#^DBCollection coll keys cond initial reduce finalize]
     (let [cmd [:ns (.getName coll)
                :key (zipmap (map name keys) (repeat true))
                :cond cond
                :initial initial
                :$reduce reduce]
           cmd  (apply build-dbo (if finalize
                                   (conj cmd :finalize finalize)
                                   cmd))
           response (.command (collection-db coll)
                              (to-dbo {:group cmd}))]
       (.throwOnError response)
       (to-clj (.get response "retval")))))

(defn- find-and-modify*   [#^DBCollection coll criteria modifier remove sort return-new fields upsert]
  (let [cmd [:findandmodify (.getName coll)
             :query criteria
             :sort (apply build-dbo (flatten (apply concat sort)))
             :new return-new
             :fields (build-fields-subset fields nil)
             :upsert (boolean upsert)]
        cmd (apply build-dbo (concat cmd
                                     (if remove
                                       [:remove true]
                                       [:update modifier])
                                ))
        db (collection-db coll)
        response (.command db cmd)]
    (.throwOnError response)
    (to-clj (.get response "value"))))

(defn find-and-modify
  "See http://www.mongodb.org/display/DOCS/findandmodify+Command"
  [#^DBCollection coll criteria modifier
   & {:keys [sort return-new fields upsert]
      :or {sort [] return-new true fields [] upsert false}}]
  (find-and-modify* coll criteria modifier false sort return-new fields upsert))

(defn find-and-remove
  "See http://www.mongodb.org/display/DOCS/findandmodify+Command"
  [#^DBCollection coll criteria
      & {:keys [sort return-new fields]
         :or {sort [] return-new true fields []}}]
  (find-and-modify* coll criteria nil true sort return-new fields false))

(defn map-reduce
  "See http://www.mongodb.org/display/DOCS/MapReduce"
  [#^DBCollection coll mapfn reducefn
   & {:keys [query sort limit out keeptemp? finalize scope verbose?]
      :or {sort [] keeptemp? false verbose? true}}]
  (let [db (collection-db coll)
        cmd (apply build-dbo
                   (concat [:mapreduce (.getName coll)]
                           [:map mapfn]
                           [:reduce reducefn]
                           [:keeptemp keeptemp?]
                           [:verbose verbose?]
                           (if query [:query query])
                           (if sort [:sort (apply build-dbo (flatten (apply concat sort)))])
                           (if limit [:limit limit])
                           (if out [:out out])
                           (if finalize [:finalize finalize])
                           (if scope [:scope scope])))
        response (.command db (to-dbo cmd))
        clj-response (to-clj response)
        results-coll (collection db (:result clj-response))]
    (.throwOnError response)
    (assoc clj-response :fetch-fn (fn [& [criteria & options]]
                                    (apply fetch results-coll criteria options)))))

(defn fetch-map-reduce-values
  "Takes the result of map-reduce and fetches the values. Takes the same options as fetch."
  [map-reduce-result & fetch-options]
  (let [fetch-fn (:fetch-fn map-reduce-result)]
    (apply fetch-fn fetch-options)))

(defvar map-reduce-fetch-all (comp fetch-map-reduce-values map-reduce)
  "Composes map-reduce and fetch-map-reduce-values and returns all the results.
   If you need to filter the results use:
     (fetch-map-reduce-values (map-reduce ...) ...your fetch options...")

(defn delete
  "Removes documents matching the supplied queries from a collection."
  [#^DBCollection coll & queries]
  (doseq [q queries]
    (.remove coll (to-dbo q))))

(defn ensure-index
  "Ensure an index exist on a collection."
  ([#^DBCollection coll fields]
     (.ensureIndex coll (to-dbo fields)))
  ([#^DBCollection coll fields options]
     (.ensureIndex coll (to-dbo fields) (to-dbo options))))

(defn ensure-unique-index
  "Ensure a unique index exist on a collection."
  [#^DBCollection coll #^String name fields]
  (.ensureIndex coll
                #^DBObject (to-dbo fields)
                name
                true))

(defn ensure-named-index
  "Ensure an index exist on a collection with the given name."
  [#^DBCollection coll #^String name fields]
  (.ensureIndex coll
                #^DBObject (to-dbo fields)
                name))

(defn drop-index
  ""
  [#^DBCollection coll o]
  (.dropIndex coll #^DBObject (to-dbo o)))

(defn drop-index-named
  ""
  [#^DBCollection coll kw]
  (.dropIndex coll (name kw)))

(defn list-indexes
  ""
  [#^DBCollection coll]
  (map to-clj (.getIndexInfo coll)))
