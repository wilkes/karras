(defproject karras "0.4.0-SNAPSHOT"
  :description "A clojure entity framework for MongoDB"
  :dependencies [[clojure "1.2.0-master-SNAPSHOT"]
                 [clojure-contrib "1.2.0-SNAPSHOT"]
                 [org.mongodb/mongo-java-driver "2.0rc3"]
                 [inflections "0.3"]]  
  :dev-dependencies [[swank-clojure "1.2.1"]
                     [lein-clojars "0.5.0"]
                     [autodoc "0.7.1"]]
  :namespaces [karras.core karras.collection karras.sugar karras.document])
