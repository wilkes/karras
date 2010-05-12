(defproject karras "0.3.0-SNAPSHOT"
  :description "A clojure entity framework for MongoDB"
  :dependencies [[org.clojure/clojure "1.2.0-master-SNAPSHOT"]          
                 [org.clojure/clojure-contrib "1.2.0-master-SNAPSHOT"]
                 [org.mongodb/mongo-java-driver "1.4"]]
  :dev-dependencies [[swank-clojure/swank-clojure "1.2.0-SNAPSHOT"]
                     [leiningen/lein-swank "1.2.0-SNAPSHOT"]
                     [lein-clojars "0.5.0-SNAPSHOT"]
                     [autodoc "0.7.0"]]
  :autodoc {:web-home "http://wilkes.github.com/karras/"})
