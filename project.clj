(defproject karras "0.9.0"
  :description "A clojure entity framework for MongoDB"
  :dependencies [[org.clojure/clojure "[1.2.1],[1.3.0]"]
                 [org.mongodb/mongo-java-driver "2.7.2"]
                 [inflections "0.6.3"]]
  :dev-dependencies [[midje "1.3.0-RC4"]]
  :autodoc {:web-src-dir "http://github.com/wilkes/karras/blob/"
            :web-home "http://wilkes.github.com/karras"})
