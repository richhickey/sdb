;   Copyright (c) Rich Hickey. All rights reserved.
;   The use and distribution terms for this software are covered by the
;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;   which can be found in the file epl-v10.html at the root of this distribution.
;   By using this software in any fashion, you are agreeing to be bound by
;   the terms of this license.
;   You must not remove this notice, or any other, from this software.

(ns org.clojure.sdb
 "A Clojure library for working with Amazon SimpleDB
  
  http://aws.amazon.com/simpledb/

  Built on top of the Java API:

  http://developer.amazonwebservices.com/connect/entry.jspa?externalID=1132&categoryID=189
  http://s3.amazonaws.com/awscode/amazon-simpledb/2007-11-07/java/library/doc/index.html"
 (use clojure.contrib.pprint)
 (import
   (com.amazonaws.sdb AmazonSimpleDB AmazonSimpleDBClient AmazonSimpleDBConfig)
   (com.amazonaws.sdb.model
     Attribute BatchPutAttributesRequest CreateDomainRequest 
     DeleteAttributesRequest DeleteDomainRequest DomainMetadataRequest  
     GetAttributesRequest Item ListDomainsRequest  
     PutAttributesRequest ReplaceableAttribute ReplaceableItem
     ResponseMetadata SelectRequest)
   (com.amazonaws.sdb.util AmazonSimpleDBUtil)))

(defn uuid
  "Given no arg, generates a random UUID, else takes a string
  representing a specific UUID"
  ([] (java.util.UUID/randomUUID))
  ([s] (java.util.UUID/fromString s)))

(defn create-client
  "Creates a client for talking to a specific AWS SimpleDB
  account. The same client can be reused for multiple requests (from
  the same thread?)."
  ([aws-key aws-secret-key config]
     (AmazonSimpleDBClient. aws-key aws-secret-key config))
  ([aws-key aws-secret-key]
     (create-client aws-key aws-secret-key
                    (.withSignatureVersion (AmazonSimpleDBConfig.) "1"))))

(defn create-domain
  "Creates a domain in the account. This is an administrative operation"
  [client name]
  (.createDomain client (CreateDomainRequest. name)))

(defn domains 
  "Returns a sequence of domain names"
  [client]
  (vec
    (.. client
      (listDomains
        (ListDomainsRequest.))
      getListDomainsResult
      getDomainName)))

(defn domain-metadata 
  "Returns a map of domain metadata"
  [client domain]
  (select-keys
    (-> client
      (.domainMetadata (DomainMetadataRequest. domain))
      .getDomainMetadataResult
      bean)
    [:timestamp :attributeValuesSizeBytes  :attributeNameCount  :itemCount
     :attributeValueCount  :attributeNamesSizeBytes :itemNamesSizeBytes]))


(defn- encode-integer [offset n]
  (let [noff (+ n offset)]
    (assert (pos? noff))
    (let [s (str noff)]
      (if (> (count (str offset)) (count s))
        (str "0" s)
        s))))

(defn- decode-integer [offset nstr]
  (- (read-string (if (= \0 (nth nstr 0)) (subs nstr 1) nstr))
     offset))

(declare decode-sdb-str)

(defn from-sdb-str 
  "Reproduces the representation of the item from a string created by to-sdb-str"
  [s]
  (let [si (.indexOf s ":")
        tag (subs s 0 si)
        str (subs s (inc si))]
    (decode-sdb-str tag str)))

(defn- encode-sdb-str [prefix s]
  (str prefix ":" s))

(defmulti #^{:doc "Produces the representation of the item as a string for sdb"}
  to-sdb-str type)
(defmethod to-sdb-str String [s] (encode-sdb-str "s" s))
(defmethod to-sdb-str clojure.lang.Keyword [k] (encode-sdb-str "k" (name k)))
(defmethod to-sdb-str Integer [i] (encode-sdb-str "i" (encode-integer 10000000000 i)))
(defmethod to-sdb-str Long [n] (encode-sdb-str "l" (encode-integer 10000000000000000000 n)))
(defmethod to-sdb-str java.util.UUID [u] (encode-sdb-str "U" u))
(defmethod to-sdb-str java.util.Date [d] (encode-sdb-str "D" (AmazonSimpleDBUtil/encodeDate d)))
(defmethod to-sdb-str Boolean [z] (encode-sdb-str "z" z))

(defmulti decode-sdb-str (fn [tag s] tag))
(defmethod decode-sdb-str "s" [_ s] s)
(defmethod decode-sdb-str "k" [_ k] (keyword k))
(defmethod decode-sdb-str "i" [_ i] (decode-integer 10000000000 i))
(defmethod decode-sdb-str "l" [_ n] (decode-integer 10000000000000000000 n))
(defmethod decode-sdb-str "U" [_ u] (java.util.UUID/fromString u))
(defmethod decode-sdb-str "D" [_ d] (AmazonSimpleDBUtil/decodeDate d))
(defmethod decode-sdb-str "z" [_ z] (condp = z, "true" true, "false" false))

(defn- item-attrs [item]
  (reduce (fn [kvs [k v :as kv]]
              (cond
               (= k :sdb/id) kvs
               (set? v) (reduce (fn [kvs v] (conj kvs [k v])) kvs v)
               :else (conj kvs kv)))
          [] item))

(defn item-triples
  "Given an item-map, returns a set of triples representing the attrs of an item"
  [item]
  (let [s (:sdb/id item)]
    (reduce (fn [ts [k v]]
                (conj ts {:s s :p k :o v}))
            #{} (item-attrs item))))

(defn- replaceable-attrs [item add-to?]
  (map (fn [[k v]] (ReplaceableAttribute. (to-sdb-str k) (to-sdb-str v) (not (add-to? k))))
                  (item-attrs item)))

(defn put-attrs
  "Puts attrs for one item into the domain. By default, attrs replace
  all values present at the same attrs/keys. You can pass an add-to?
  function (usually a set), and when it returns true for a key, values
  will be added to the set of values at that key, if any."
  ([client domain item] (put-attrs client domain item #{}))
  ([client domain item add-to?]
    (let [item-name (to-sdb-str (:sdb/id item))
          attrs (replaceable-attrs item add-to?)]
      (.putAttributes client (PutAttributesRequest. domain item-name attrs)))))

(defn batch-put-attrs
  "Puts the attrs for multiple items into a domain, with the same semantics as put-attrs"
  ([client domain items] (batch-put-attrs client domain items #{}))
  ([client domain items add-to?]
    (.batchPutAttributes client
      (BatchPutAttributesRequest. domain
        (map
          #(ReplaceableItem. (to-sdb-str (:sdb/id %)) (replaceable-attrs % add-to?))
          items)))))

(defn setify
  "If v is a set, returns it, else returns a set containing v"
  [v]
  (if (set? v) v (hash-set v)))

(defn- build-item [item-id attrs]
  (reduce (fn [m #^Attribute a]
            (let [k (from-sdb-str (.getName a))
                  v (from-sdb-str (.getValue a))
                  ov (m k)]
              (assoc m k (if ov (conj (setify ov) v) v))))
          {:sdb/id item-id} attrs))

(defn get-attrs
  "Gets the attributes for an item, as a valid item map. If no attrs are supplied,
  gets all attrs for the item."
  [client domain item-id & attrs]
  (let [r (.getAttributes client
                          (GetAttributesRequest. domain (to-sdb-str item-id) (map to-sdb-str attrs)))
        attrs (.. r getGetAttributesResult getAttribute)]
    (build-item item-id attrs)))

;todo remove a subset of a set of vals
(defn delete-attrs
  "Deletes the attrs from the item. If no attrs are supplied, deletes
  all attrs and the item. attrs can be a set, in which case all values
  at those keys will be deleted, or a map, in which case only the
  values supplied will be deleted."
  ([client domain item-id] (delete-attrs client domain item-id #{}))
  ([client domain item-id attrs]
    (.deleteAttributes client
      (DeleteAttributesRequest. domain (to-sdb-str item-id)
        (cond
          (set? attrs) (map #(Attribute. (to-sdb-str %) nil) attrs)
          (map? attrs) (map (fn [[k v]] (Attribute. (to-sdb-str k) (to-sdb-str v))) attrs)
          :else (throw (Exception. "attrs must be set or map")))))))

(defn- attr-str [attr]
  (if (sequential? attr)
    (let [[op a] attr]
      (assert (= op 'every))
      (format "every(%s)" (attr-str a)))
    (str \` (to-sdb-str attr) \`)))

(defn- op-str [op]
  (.replace (str op) "-" " "))

(defn- val-str [v]
  (str \" (.replace (to-sdb-str v) "\"" "\"\"") \"))

(defn- simplify-sym [x]
  (if (and (symbol? x) (namespace x))
    (symbol (name x))
    x))

(defn- expr-str
  [e]
  (condp #(%1 %2) (simplify-sym (first e))
    '#{not}
      (format "(not %s)" (expr-str (second e)))
    '#{and or intersection}
      :>> #(format "(%s %s %s)" (expr-str (nth e 1)) % (expr-str (nth e 2)))
    '#{= != < <= > >= like not-like}
      :>> #(format "(%s %s %s)" (attr-str (nth e 1)) (op-str %) (val-str (nth e 2)))
    '#{null not-null}
      :>> #(format "(%s %s)" (op-str %) (attr-str (nth e 1)))
    '#{between}
      :>> #(format "(%s %s %s and %s)" (attr-str (nth e 1)) % (val-str (nth e 2)) (val-str (nth e 3)))
    '#{in} (cl-format nil "~a in(~{~a~^, ~})"
             (attr-str (nth e 1)) (map val-str (nth e 2)))
    ))

(defn- where-str
  [q] (expr-str q))

(defn select-str
  "Produces a string representing the query map in the SDB Select language.
  query calls this for you, just public for diagnostic purposes."
  [m]
  (str "select "
    (condp = (simplify-sym (:select m))
      '* "*"
      'ids "itemName()"
      'count "count(*)"
      (cl-format nil "~{~a~^, ~}" (map attr-str (:select m))))
    " from " (:from m)
    (when-let [w (:where m)]
      (str " where " (where-str w)))
    (when-let [s (:order-by m)]
      (str " order by " (attr-str (first s)) " " (or (second s) 'asc)))
    (when-let [n (:limit m)]
      (str " limit " n))))

(defn query
  "Issue a query. q is a map with mandatory keys:

  :select */ids/count/[sequence-of-attrs]
  :from domain-name

  and optional keys:

  :where sexpr-based query expr supporting

    (not expr)
    (and/or/intersection expr expr)
    (=/!=/</<=/>/>=/like/not-like attr val)
    (null/not-null attr)
    (between attr val1 val2)
    (in attr #(val-set})

  :order-by [attr] or [attr asc/desc]
  :limit n

  When :select is
      count - returns a number
      ids - returns a sequence of ids
      * or [sequence-of-attrs] - returns a sequence of item maps, containing all or specified attrs.

  See:

      http://docs.amazonwebservices.com/AmazonSimpleDB/2007-11-07/DeveloperGuide/

  for further details of select semantics. Note query maps to the SDB Select, not Query, API
  next-token, if supplied, must be the value obtained from the :next-token attr of the metadata
  of a previous call to the same query, e.g. (:next-token (meta last-result))"
  ([client q] (query client q nil))
  ([client q next-token]
    (let [response (.select client (SelectRequest. (select-str q) next-token))
          response-meta (.getResponseMetadata response)
          result (.getSelectResult response)
          items (.getItem result)
          m {:box-usage (read-string (.getBoxUsage response-meta))
             :request-id (.getRequestId response-meta)
             :next-token (.getNextToken result)}]
      (condp = (simplify-sym (:select q))
        'count (-> items first .getAttribute (.get 0) .getValue Integer/valueOf)
        'ids (with-meta (map #(from-sdb-str (.getName %)) items) m)
        (with-meta (map (fn [item]
                          (build-item (from-sdb-str (.getName item)) (.getAttribute item)))
                     items)
          m)))))

(defn query-all
  "Issue a query repeatedly to get all results"
  [client q]
  (loop [ret [] next-token nil]
    (let [r1 (query client q next-token)
          nt (:next-token ^r1)]
      (if nt
        (recur (into ret r1) nt)
        (with-meta (into ret r1) ^r1)))))


(comment
;some sample usage
(use 'org.clojure.sdb)

;get logging to calm down, else noisy at REPL 
(org.apache.log4j.PropertyConfigurator/configure 
 (doto (java.util.Properties.) 
   (.setProperty "log4j.rootLogger" "WARN")
   (.setProperty "log4j.logger.com.amazonaws.sdb" "WARN") 
   (.setProperty "log4j.logger.httpclient" "WARN")))

;the 'spreadsheet' sample data in the canonic item-map representation
;an item-map must have an :sdb/id key. Multi-value attrs are represented as sets. 
;Note that attrs with single values always come back as non-sets!
(def db
     #{{:sdb/id (uuid "773fb848-70a2-4586-b6b3-4aaa2dd55e00"),
        :category #{"Clothing" "Motorcycle Parts"},
        :subcat "Clothing",
        :Name "Leather Pants",
        :color "Black",
        :size #{"Small" "Medium" "Large"}}
       {:sdb/id (uuid "e76589f6-34e5-4a14-8af6-b70bf0242d7d"),
        :category "Clothes",
        :subcat "Pants",
        :Name "Sweatpants",
        :color #{"Yellow" "Pink" "Blue"},
        :size "Large"}
       {:sdb/id (uuid "7658a719-837a-4f55-828b-2472cc823bb1"),
        :category "Clothes",
        :subcat "Sweater",
        :Name "Cathair Sweater",
        :color "Siamese",
        :size #{"Small" "Medium" "Large"}}
       {:sdb/id (uuid "fa18dc85-63d3-47b0-865b-7946057d7f42"),
        :category "Car Parts",
        :subcat "Emissions",
        :Name "02 Sensor",
        :make "Audi",
        :model "S4"}
       {:sdb/id (uuid "6bca3ac8-fc8d-44d2-b7c3-da959ad7dfc4"),
        :category "Clothes",
        :subcat "Pants",
        :Name "Designer Jeans",
        :color #{"Paisley Acid Wash"},
        :size #{"32x32" "30x32" "32x34"}}
       {:sdb/id (uuid "ebd3cb4a-b94d-4150-baaa-0821beaa776c"),
        :category "Car Parts",
        :subcat "Engine",
        :Name "Turbos",
        :make "Audi",
        :model "S4"}
       {:sdb/id (uuid "598d2273-ae72-4946-9fa4-1fe4a2874be7"),
        :category "Motorcycle Parts",
        :subcat "Bodywork",
        :Name "Fender Eliminator",
        :color "Blue",
        :make "Yamaha",
        :model "R1"}})


(def client (create-client "your-aws-key" "your-aws-secret-key"))

;one-time domain create
;(create-domain client "test")

(domains client)

(domain-metadata client "test")

;put them all in
(batch-put-attrs client "test" db)

;get one
(get-attrs client "test" (uuid "e76589f6-34e5-4a14-8af6-b70bf0242d7d"))

;get part of one
(get-attrs client "test" (uuid "e76589f6-34e5-4a14-8af6-b70bf0242d7d") :color :Name)

(get-attrs client "test" (uuid "6bca3ac8-fc8d-44d2-b7c3-da959ad7dfc4"))
(delete-attrs client "test" (uuid "6bca3ac8-fc8d-44d2-b7c3-da959ad7dfc4") {:size "32x32"})
(get-attrs client "test" (uuid "6bca3ac8-fc8d-44d2-b7c3-da959ad7dfc4"))

(get-attrs client "test" (uuid "e76589f6-34e5-4a14-8af6-b70bf0242d7d"))
;wipe it out
(delete-attrs client "test" (uuid "e76589f6-34e5-4a14-8af6-b70bf0242d7d"))
(get-attrs client "test" (uuid "e76589f6-34e5-4a14-8af6-b70bf0242d7d"))

;restore it
(put-attrs client "test"
  {:sdb/id (uuid "e76589f6-34e5-4a14-8af6-b70bf0242d7d"),
   :category "Clothes",
   :subcat "Pants",
   :Name "Sweatpants",
   :color #{"Yellow" "Pink" "Blue"},
   :size "Large"})
(get-attrs client "test" (uuid "e76589f6-34e5-4a14-8af6-b70bf0242d7d"))

;remove bits
(delete-attrs client "test" (uuid "e76589f6-34e5-4a14-8af6-b70bf0242d7d") #{:Name :size})
(get-attrs client "test" (uuid "e76589f6-34e5-4a14-8af6-b70bf0242d7d"))
(delete-attrs client "test" (uuid "e76589f6-34e5-4a14-8af6-b70bf0242d7d") {:color "Yellow"})
(get-attrs client "test" (uuid "e76589f6-34e5-4a14-8af6-b70bf0242d7d"))

;query variants
(query client
  '{;:select ids
    ;:select count
    :select *
    ;:select [:Name, :color]
    :from "test"
    :where (or
             (and (= :category "Clothes") (= :subcat "Pants"))
             (= :category "Car Parts"))})

;parameterized query is just syntax-quote!
(let [cat "Clothes"]
  (query client `{:select * :from "test" :where (or (= :category ~cat)
                                                  (= :category "Car Parts"))}))
)
