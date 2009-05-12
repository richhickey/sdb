;Stick your iTunes database in sdb
;note, the commands in this script are meant to be run interactively in the REPL

(use 'clojure.contrib.pprint)
(use 'org.clojure.sdb)


;get logging to calm down, else noisy at REPL 
(org.apache.log4j.PropertyConfigurator/configure 
 (doto (java.util.Properties.) 
   (.setProperty "log4j.rootLogger" "WARN")
   (.setProperty "log4j.logger.com.amazonaws.sdb" "WARN") 
   (.setProperty "log4j.logger.httpclient" "WARN")))

;this might take a minute, depending on the size of your library
(def itx (clojure.xml/parse "file:///Users/rich/Music/iTunes/iTunes Music Library.xml"))

(defn body
  "returns the presumably single content element of node"
  [node]
  ((:content node) 0))

(defn dict->map
  "dict node is {:tag :dict :content [k v k v ...]}
  where k is: {:tag :key :content [\"Key Name\"]}
  and v is {:tag :dict|:string|:integer|:date|:array|:true|:false|:data}"
  ([node] (dict->map node identity))
  ([key-fn node]
   (let [df (java.text.SimpleDateFormat. "yyyy-MM-dd'T'HH:mm:ss'Z'")]
     (condp = (:tag node)
       :string (body node)
       :data (body node)
       :integer (read-string (body node))
       :date (.parse df (body node))
       :true true
       :false false
       :dict (reduce (fn [m [k v]]
                       (assoc m (key-fn (body k)) (dict->map key-fn v)))
                     {} (partition 2 (:content node)))
       :array (vec (map #(dict->map key-fn %) (:content node)))))))
  
(defn strk [s]
  (-> s .toLowerCase (.replace " " "-") keyword))

;this might take a few secs
(def itmdb (dict->map strk (body itx)))
(keys itmdb)
(pprint (-> itmdb :tracks first val))

(def client (create-client "your-aws-key" "your-aws-secret-key"))

(defn tracks [itmdb]
  (-> itmdb :tracks vals))

(take 1 (tracks itmdb))

(defn add-track
  [client domain track]
  (let [item (merge track {:sdb/id (uuid) :my/db "iTunes" :my/type "Track"})]
    (prn (select-keys item '[:artist :name :sdb/id :persistent-id]))
    (put-attrs client domain item)))

;(create-domain client "test")
(domains client)
(map #(add-track client "test" %) (take 100 (tracks itmdb)))

(query client '{:select [:artist :name] :from "test" :where (and (= :my/db "iTunes") (= :genre "Rock"))})
