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

(ns karras.core
  (:use [clojure.contrib.def :only [defnk defvar]]
        [clojure.contrib.ns-utils :only [immigrate]])
  (:import [com.mongodb Mongo DB BasicDBObject]
           [java.util Map Map$Entry List]))

(defn- keyword-str [kw]
  (if (keyword? kw)
    (name kw)
    (str kw)))

(defprotocol MongoMappable
  "(to-dbo [d])
   (to-clj [d])
   Implementations provided for Map, List, Object, and nil"
  (to-dbo [d])
  (to-clj [d])
  (to-description [d]))

(extend-protocol MongoMappable
 Map
 (to-dbo [m]
  (let [dbo  (BasicDBObject.)]
    (doseq [[k v] m]
      (.put dbo (keyword-str k) (to-dbo v)))
    dbo))
 (to-clj [v]
  (let [f (fn [result #^Map$Entry e]
            (conj result [(keyword (.getKey e))
                          (to-clj (.getValue e))]))]
    (reduce f {} v)))
 (to-description [v]
  (let [f (fn [result #^Map$Entry e]
            (conj result [(keyword (.getKey e))
                          (to-description (.getValue e))]))]
    (reduce f {} v)))


 List
 (to-dbo [v] (map to-dbo v))
 (to-clj [v] (map to-clj v))
 (to-description [v] (vec (set (map to-description v))))

 clojure.lang.Keyword
 (to-dbo [v] (str v))
 (to-clj [v] (str v))
 (to-description [v] java.lang.String)

 clojure.lang.Symbol
 (to-dbo [v] (str v))
 (to-clj [v] (str v))
 (to-description [v] java.lang.String)
 
 Object
 (to-dbo [v] v)
 (to-clj [v] v)
 (to-description [v] (class v))

 nil
 (to-dbo [v] v)
 (to-clj [v] v)
 (to-description [v] v))

(defvar *mongo-db* nil
  "Var to bind a com.mongo.DB. Use with with-mongo or with-mongo-request.")

(defmacro with-mongo
  "Macro to bind *mongo-db*"
  [mongo-db & body]
  `(binding [karras/*mongo-db* ~mongo-db]
     ~@body))

(defmacro with-mongo-request
  "Macro to bind *mongo-db* and wraps the body in a Mongo request.
   For more information see: http://www.mongodb.org/display/DOCS/Java+Driver+Concurrency"
  [mongo-db & body]
  `(binding [*mongo-db* ~mongo-db]
     (in-request *mongo-db*  ~@body)))

(defn connect
  "Returns a single server connection. Defaults to host 127.0.0.1:27017"
  ([] (connect "127.0.0.1"))
  ([host] (connect host 27017))
  ([#^String host port]
     (Mongo. host (int port))))

(defn mongo-db
  "Returns a com.mongo.DB object.
   Defaults to host 127.0.0.1:27017 if a connection is not provided"
  ([db-name]
     (mongo-db (connect) db-name))
  ([#^Mongo connection db-name]
      (.getDB connection (keyword-str db-name))))

(defmacro in-request
  "Macro to wrap the body in a Mongo request.
   For more information see: http://www.mongodb.org/display/DOCS/Java+Driver+Concurrency"
  [#^DB db & body]
  `(try
    (.requestStart ~db)
    ~@body
    (finally
     (.requestDone ~db))))

(defn write-concern-none
  "From the mongo driver javadocs: 
   Don't check for or report any errors on writes."
  [#^DB db]
  (.setWriteConcern db com.mongodb.DB$WriteConcern/NONE))

(defn write-concern-normal
  "From the mongo driver javadocs:
   Use the default level of error checking on writes."
  [#^DB db]
  (.setWriteConcern db com.mongodb.DB$WriteConcern/NORMAL))

(defn write-concern-strict
  "From the mongo driver javadocs:
   Send a getLastError command following all writes."
  [#^DB db]
  (.setWriteConcern db com.mongodb.DB$WriteConcern/STRICT))

(defn drop-db
  ""
  [#^DB db]
  (.dropDatabase db))

(defn list-collections
  ""
  [#^DB db]
  (map keyword (.getCollectionNames db)))

(defn eval-code
  [#^DB db code-str]
  (to-clj (.eval db code-str (into-array nil))))
