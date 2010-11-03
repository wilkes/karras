(defproject karras "0.6.0-SNAPSHOT"
  :description "A clojure entity framework for MongoDB"
  :dependencies [[org.clojure/clojure "1.2.0"]
                 [org.clojure/clojure-contrib "1.2.0"]
                 [org.mongodb/mongo-java-driver "2.1"]
                 [inflections "0.4"]]  
  :dev-dependencies [[swank-clojure "1.2.1"]
                     [lein-clojars "0.5.0"]
                     [autodoc "0.7.1"]
                     [scriptjure "0.1.9"]
                     [midje "0.6.1"]
                     [clojure-csv/clojure-csv "1.2.0"]]
  :namespaces [karras.core karras.collection karras.sugar karras.entity]
  :autodoc {:web-src-dir "http://github.com/wilkes/karras/blob/"
            :web-home "http://wilkes.github.com/karras"})
