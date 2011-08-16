(defproject karras "0.7.0"
  :description "A clojure entity framework for MongoDB"
  :dependencies [[org.clojure/clojure "1.2.0"]
                 [org.clojure/clojure-contrib "1.2.0"]
                 [org.mongodb/mongo-java-driver "2.1"]
                 [inflections "0.4"]]
  :dev-dependencies [[scriptjure "0.1.9"
                      :exclusions [org.clojure/clojure
                                   org.clojure/clojure-contrib]]
                     [midje "1.2.0"]]
  :aot [karras.core karras.collection karras.sugar karras.entity]
  :autodoc {:web-src-dir "http://github.com/wilkes/karras/blob/"
            :web-home "http://wilkes.github.com/karras"})
