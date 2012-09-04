(ns util.test
  (:require [clj-json [core :as json]])  
  (:use util.storage)
  (:use util.jdbc)
  (:use util.dynamo)
  )

;; 9 seconds to create 1M items and insert them
;; 5 seconds to dump to TSV file (this is great)
;; 19 seconds to read 1M from file and insert them (dumb version)

(defn -main [& args]
  ;;note: 1.6 sec overhead for getting this far

  (let [store (dynamo-structstore "asset")]

    (if false
      (let [items (map (fn [i] (let [id (format "item-%05d" i)] {:id id :title (str "This is item " i)})) (range 0 200)) ;; 1.5 sec
            item-map (apply hash-map (flatten (map (fn [item] [(:id item) item]) items)))]

        (put-structs store item-map) ;; {mem: 4.1 sec, sqlite: 16.5 sec, h2:}
        ;;(doall (map (fn [item] (put-struct store (:id item) item)) items)) ;; 7.0 sec
        
        ;;(write-struct-file store "test2.tsv") ;; 10.5 total, 4.9 sec

        ;;total time to create database: {mem: 10.5, sqlite: 48.3, h2: 58.7 MINUTES}. Note: sqlite takes 79 seconds if the db exists and it replaces every item
        )

      (let [keys (map (fn [i] (format "item-%05d" i)) (range 0 200))]
        
        ;;(read-struct-file store "test2.tsv") ;; {mem: 16.4, sqlite: 0.1}
        ;;(println (get-struct store "item-658906"))
        (if false
          (loop [m (scan-structs store)]
            (let [s (:structs m)]
              (println "Scanned" (count s) " records")
              (doseq [v s]
                (println "\t" v))
              (if (:more m)
                (recur (scan-structs store :skip (:skip m))))))
          (let [s (get-structs store keys)]
            (println "Scanned" (count s) " records")
            (doseq [v s]
              (println "\t" v))))

        ))
    

    ))
