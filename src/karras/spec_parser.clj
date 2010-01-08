(ns karras.spec-parser)

(defn attr-name [[name option-map]] name)
(defn attr-options [[name option-map]] option-map)

(defn filter-spec [f spec]
  (filter f (partition 2 spec)))

(defn contains-options? [attr & keywords]
  (let [options (attr-options attr)]
    (reduce #(and %1 (contains? options %2))
            keywords)))

(defn required-attrs [spec]
  (filter-spec #(= :required (:type (attr-options %)))
               spec))

(defn enum-attrs [spec]
  (filter-spec #(contains? (attr-options %) :enums)
               spec))

(defn pair-attrs [spec]
  (filter-spec #(not (contains-options? % :enums :type))
               spec))

(defn attrs-with-defaults [spec]
  (filter-spec #(contains-options? % :default)
              spec))

(defn pair-field [spec token]
  (some #(if (= token (attr-name %)) %) (pair-attrs spec)))

(defn enum-field [spec token]
  (some #(if (contains? (:enums (attr-options %)) token) %)
        (enum-attrs spec)))

(defn enum-name [spec val]
  (attr-name (enum-field spec val)))

(defn parse-pair [spec parsed key [value & tail]]
  [(assoc parsed key value) tail])

(defn parse-enum [spec parsed token tail]
  (let [key (enum-name spec token)]
    [(assoc parsed key token) tail]))

(defn parse-ignore [spec parsed token tail]
  [parsed tail])

(defn choose-parser [spec token]
  (cond (pair-field spec token) parse-pair
        (enum-field spec token) parse-enum
        :default parse-ignore))

(defn parse-options [spec item init-results init-unparsed]
  (loop [results init-results
         [token & tail] init-unparsed]
    (if token
      (let [parser (choose-parser spec token)
            [parsed unparsed] (parser spec results token tail)]
        (recur parsed unparsed))
      results)))

(defn apply-default [parsed-item attr]
  (let [default-fn (:default (attr-options attr))]
    {(attr-name attr)
     (default-fn parsed-item)}))

(defn set-defaults [spec parsed-item]
  (let [attrs (attrs-with-defaults spec)
        need-defaults (filter #(nil? ((attr-name %) parsed-item)) attrs)
        defaulted (map #(apply-default parsed-item %) need-defaults)]
    (apply merge parsed-item defaulted)))

(defn parse-item [spec item]
  (let [required-keys (map first (required-attrs spec))
        required-values (take (count required-keys) item)
        init-results (zipmap required-keys required-values)
        init-unparsed (drop (count required-keys) item)
        parsed-item (parse-options spec item init-results init-unparsed)]
    (set-defaults spec parsed-item)))

(defn parse-items [spec items]
  (map #(parse-item spec %) items))

(def demographic-spec
     [:name {:type :required}
      :age  {:type :required}
      :gender {:enums #{:male :female}}])

(def dick ["dick" 1 :male  ])
(def jane ["jane" 1 :female])

(assert (= (parse-item demographic-spec dick)
           {:gender :male, :age 1, :name "dick"}))

(assert (= (parse-items demographic-spec [dick jane])
           [{:gender :male, :age 1, :name "dick"}
            {:gender :female, :age 1, :name "jane"}]))


