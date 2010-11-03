(ns karras.utils
  (:use [karras.core :only [with-mongo-request mongo-db]]
        [karras.entity :only [save ensure-type]]
        [clojure-csv.core :only [parse-csv]]
        [clojure.java.io :only [reader]]))

(defn csv-line-to-clj
  "headers => vector of n headers
   file-line => single line from csv with n collumns"
  [headers file-line]
  (zipmap headers (first (parse-csv file-line))))

(defn clj-to-mongo [t e]
  (save (ensure-type t e)))

(defn csv-to-mongo [db-name collection-type csv-file headers]
  (with-mongo-request (mongo-db db-name)
    (with-open [f (reader csv-file)]
      (doseq [file-line (line-seq f)]
        (clj-to-mongo collection-type
                      (csv-line-to-clj headers file-line))))))
