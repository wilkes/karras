(ns floyd.resource
  (:use     [compojure    :only [redirect-to html link-to capitalize]]
            [karras.sugar])
  (:require [karras       :as mongo])
  (:import [com.mongodb ObjectId]))

(declare *db*)
(defn set-db [db]
  (def *db* db))

(defn resource-collection-name [request]
  (-> request :params :collection))

(defn resource-collection [request]
  (mongo/collection *db* (resource-collection-name request)))

(defn resource-id [request]
  (-> request :params :id))

(defn fetch-item [request]
  (mongo/fetch-by-id (resource-collection request)
                     (resource-id request)))

(defn fetch-and-merge-item [request]
  (merge request {:resource-item (merge (fetch-item request) (:form-params request))}))

(defonce *resource-map* (atom {}))

(defn add-resource [kw resource-name resource-proto validator-fn]
  (derive kw ::collection)
  (swap! *resource-map* conj {resource-name {:kw kw
                                             :proto resource-proto
                                             :validator validator-fn}}))

(defn humanize [k]
  (apply str
         (interpose " "
                    (map capitalize 
                         (apply concat (map #(.split % "-")
                                            (.split (name k) "_")))))))

(defn get-resource-descriptor [request]
  (@*resource-map* (resource-collection-name request)))

(derive ::index  ::action)
(derive ::show   ::action)
(derive ::new    ::action)
(derive ::create ::action)
(derive ::edit   ::action)
(derive ::update ::action)
(derive ::delete ::action)

(derive ::html ::content-type)
(derive ::json ::content-type)

(def content-type-map {"text/html" ::html
                       "text/json" ::json
                       "application/x-www-form-urlencoded" ::html})

(defn errors-for [errors field]
  (when (contains? errors field)
    doall (map (fn [error] [:div {:class "warning"} error]) (errors field))))

(defn handle-dispatcher [request & _]
  (let [action (request :resource-action)
        content-type (or (request :content-type) "text/html")]
    [action ((get-resource-descriptor request) :kw) (content-type-map content-type)]))

(defmulti handle-resource handle-dispatcher)
(defmulti display-resource handle-dispatcher)
(defmulti display-layout handle-dispatcher)
(defmulti resource-form handle-dispatcher)

(defmethod handle-resource [::index ::collection ::content-type]
  [request & _]
  (display-resource
   (merge request
          {:resource-items
           (mongo/fetch-all (resource-collection request))})))

(defmethod handle-resource [::show ::collection ::content-type]
  [request & _]
  (display-resource (fetch-and-merge-item request)))

(defmethod handle-resource [::new ::collection ::content-type]
  [request & _]
  (display-resource request))

(defmethod handle-resource [::create ::collection ::content-type]
  [request & _]
  (let [resource (resource-collection-name request)
        validate ((get-resource-descriptor request) :validator)
        entity (request :form-params)
        validations (validate entity)]
    (if (seq validations)
      (display-resource (merge request {:validation-errors validations}))
      (do
        (mongo/insert (resource-collection request) entity)
        (display-resource request)))))

(defmethod handle-resource [::edit ::collection ::content-type]
  [request & _]
  (display-resource (fetch-and-merge-item request)))

(defmethod handle-resource [::update ::collection ::content-type]
  [request & _]
  (let [resource (resource-collection-name request)
        collection (resource-collection request)
        id         (resource-id request)
        validate ((get-resource-descriptor request) :validator)
        entity   (request :form-params)
        validations (validate entity)]
    (if (seq validations)
      (display-resource (merge request {:validation-errors validations}))
      (do
        (mongo/update collection
                  (query (eq :_id (ObjectId. id)))
                  (modify (set-fields (-> request :form-params))))
        (display-resource request)))))

(defmethod handle-resource [::delete ::collection ::content-type]
  [request & _]
  (let [resource (resource-collection-name request)
        collection (resource-collection request)
        id         (resource-id request)]
    (mongo/delete collection (query (eq :_id (ObjectId. id))))
    (display-resource request)))


(defmethod display-layout [::action ::collection ::content-type]
  [request title & body]
  (html [:html
         [:head [:title title]]
         [:body body]]))


(defn remove-special-params [m]
  (let [remove-param? (fn [[k v]]
                        (or (= \_ (first (name k)))
                            (= k :id)
                            (= k :collection)))]
    (apply hash-map (apply concat (remove remove-param? m)))))

(defmethod resource-form [::action ::collection ::content-type]
  [request]
  (let [collection-name (resource-collection-name request)
        item (or (:resource-item request) {})
        errors (or (:validation-errors request) {})]
    [:form {:method "post"
            :action (str "/" collection-name (if (:_id item) (str "/" (:_id item))))}
     (doall (map (fn [[k v]]
                   [:div
                    (when (contains? errors k)
                      doall (map (fn [error] [:div {:class "warning"} error]) (errors k)))
                    [:div (humanize k)
                     [:input {:name (name k) :type "text"  :value v}]]])
                 (remove-special-params item)))
     [:div [:input {:type "submit" :value "Save Changes"}]]
     (if (:_id item) [:input {:name "_method" :type "hidden" :value "PUT"}])]))

(defmethod display-resource [::action ::collection ::content-type]
  [request & _] "Not Implemented.")

(defmethod display-resource [::index ::collection ::content-type]
  [request & _]
  (let [collection-name (resource-collection-name request)
        items-html (map (fn [item]
                          [:div (link-to (str "/" collection-name
                                              "/" (:_id item))
                                         (str (remove-special-params item)))])
                        (:resource-items request))]
    (display-layout request "Index"
                    (link-to (str "/" collection-name "/new") "Add")
                    items-html)))

(defmethod display-resource [::show ::collection ::content-type]
  [request & _]
  (let [item (:resource-item request)
        collection-name (resource-collection-name request)
        display-attribute (fn [[kw val]] [:div (str (humanize kw) ": " val)])]
    (display-layout request "Show"
                    [:div
                     (doall (map display-attribute (remove-special-params item)))
                     (link-to (str "/" collection-name "/" (:_id item) "/edit") "Edit ")
                     [:form {:method "post" :action (str "/" collection-name "/"(:_id item))}
                      [:input {:type "submit" :value "Delete"}]
                      [:input {:type "hidden" :value "DELETE" :name "_method"}]]])))

(defmethod display-resource [::new ::collection ::content-type]
  [request & _]
  (display-layout request "New"
                  (resource-form 
                   (merge request
                          {:resource-item
                           (merge ((get-resource-descriptor request) :proto) (:form-params request))}))))

(defmethod display-resource [::create ::collection ::content-type]
  [request & _]
  (if (nil? (:validation-errors request))
    (redirect-to (str "/" (resource-collection-name request)))
    (display-resource (merge request {:resource-action ::new}))))

(defmethod display-resource [::edit ::collection ::content-type]
  [request & _]
  (display-layout request "Edit" (resource-form request)))

(defmethod display-resource [::update ::collection ::content-type]
  [request & _]
  (let [resource (resource-collection-name request)
        id         (resource-id request)]
    (if (nil? (:validation-errors request))
      (redirect-to (str "/" resource "/" id))
      (display-resource
       (fetch-and-merge-item (merge request {:resource-action ::edit}))))))

(defmethod display-resource [::delete ::collection ::content-type]
  [request & _]
  (let [resource (resource-collection-name request)]
    (redirect-to (str "/" resource))))
