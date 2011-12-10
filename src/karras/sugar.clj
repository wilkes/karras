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

(ns karras.sugar
  (:use [karras.def :only [defvar]])
  (:import [java.util Calendar]))

(defn compound-index
  "Sugar to create compound index"
  [& asc-or-descs]
  (apply merge asc-or-descs))

(defn asc  "Sugar for order by and index." [k] {k 1})
(defn desc "Sugar for order by and index." [k] {k -1})

(defvar double-type           1 "")
(defvar string-type           2 "")
(defvar object-type           3 "")
(defvar array-type            4 "")
(defvar binary-type           5 "")
(defvar object-id-type        7 "")
(defvar boolean-type          8 "")
(defvar date-type             9 "")
(defvar null-type            10 "")
(defvar regex-type           11 "")
(defvar js-code-type         13 "")
(defvar symbol-type          14 "")
(defvar js-scoped-code-type  15 "")
(defvar int32-type           16 "")
(defvar timestamp-type       17 "")
(defvar int64-type           18 "")
(defvar min-key-type        255 "")
(defvar max-key-type        127 "")

(defn where
  "Sugar to create a where document. Example:
     (where (ne :j 3) (gt k 10))
   produces:
     {:j {:$ne 3} :k {:$gt 10}}"
  [& clauses]
  (apply merge clauses))

(defn through-date [d]
  (.getTime
   (doto (Calendar/getInstance)
     (.setTime d)
     (.add Calendar/DAY_OF_MONTH 1))))

(defn lt            "" [field y]           {field {:$lt  y}})
(defn gt            "" [field y]           {field {:$gt  y}})
(defn lte           "" [field y]           {field {:$lte y}})
(defn gte           "" [field y]           {field {:$gte y}})
(defn eq            "" [field y]           {field y})
(defn within        "" [field y z]         {field {:$gte y :$lte z}})
(defn within-dates  "" [field y z]         {field {:$gte y :$lt (through-date z)}})
(defn ne            "" [field y]           {field {:$ne y}})
(defn in            "" [field ys]          {field {:$in ys}})
(defn not-in        "" [field ys]          {field {:$nin ys}})
(defn eq-mod        "" [field m v]         {field {:$mod [m v]}})
(defn all           "" [field y]           {field {:$all y}})
(defn size          "" [field y]           {field {:$size y}})
(defn exist?        "" [field]             {field {:$exists true}})
(defn not-exist?    "" [field]             {field {:$exists false}})
(defn js-query      "" [js-string]         {:where js-string})
(defn element-match "" [field match-query] {field {:$elemMatch match-query}})
(defn is-type       "" [field type-number] {field {:$type type-number}})
(defn negate        "" [where-expr]        {:$not where-expr})
(defn is-nil?       "" [field]             (is-type field null-type))
;; mongo 1.6
(defn slice         "" [field val]         {field {:$slice val}})
(defn ||          "or" [& clauses]         {:$or clauses})
(defn add-to-set    "" [field value & values]
  (if values
    {:$addToSet {field {:$each (cons value values)}}}
    {:$addToSet {field value}}))

(def atomic {:$atomic true})

(defn matched
  "The $ operator (by itself) means 'position of the matched array item in the
   query'. Use this to find an array member and then manipulate it."
  [array-name & [field]]
  (str (name array-name) ".$" (when field (str "." (name field)))))

(defn sort-by-keys [keys maps]
  (sort (fn [x y]
          (first (remove #(= 0 %)
                         (map #(compare (% x) (% y)) keys))))
        maps))
(defn modify
  "Sugar to create update documents
     (modify (incr :j) (push :k 3))
   produces:
     {:$inc :j :$push {:k 3}}"
  [& clauses]
  (apply merge clauses))

(defn incr        "" [field & [amount]] {:$inc {field (or amount 1)}})
(defn set-fields  "" [field-map]        {:$set field-map})
(defn unset       "" [field]            {:$unset {field 1}})
(defn push        "" [field value]      {:$push {field value}})
(defn pop-last    "" [field]            {:$pop {field 1}})
(defn pop-first   "" [field]            {:$pop {field -1}})
(defn pull        "" [field value]      {:$pull {field value}})
(defn pull-all    "" [field values]   {:$pullAll {field values}})

(defn date
  "A convenience constructor for making a java.util.Date.  Takes zero or more
   args. Zero args return the current time. One or more args returns a date
   values provided and all other values zeroed out.
   Args are year, month, date, hour, minute, second, and  milliseconds"
  [& [year month date hour minute second milli]]
  (let [c  (Calendar/getInstance)
        z? #(int (or % 0))]
    (when year
      (.set c Calendar/YEAR        (z? year))
      (.set c Calendar/MONTH       (z? (- (or month 1) 1)))
      (.set c Calendar/DATE        (z? date))
      (.set c Calendar/HOUR_OF_DAY (z? hour))
      (.set c Calendar/MINUTE      (z? minute))
      (.set c Calendar/SECOND      (z? second))
      (.set c Calendar/MILLISECOND (z? milli)))
    (.getTime c)))
