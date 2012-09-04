(ns util.jdbc
  (:use util.uuid)
  (:use util.storage)
  (:use [clojure.java.io :as io])
  (:require [clj-json [core :as json]])
  (:import java.sql.DriverManager java.sql.Connection java.sql.Statement java.sql.PreparedStatement java.sql.ResultSet))

;;
;; Each struct is assumed to be limited in size to 64k of serialized JSON
;;
 
(defn jdbc-structstore
  "Return an IStructStore instance connected to the JDBC system specified by the settings. Possible keys are :classname, :subprotocol, :subname, :user, and :password"
  [settings]
  (Class/forName (:classname settings))
  (let [url (str "jdbc:" (:subprotocol settings) ":" (:subname settings))
        db (DriverManager/getConnection url)
        keep (or (:keep settings) 1)
        stmt (.createStatement db)
        init (fn [sql-init] (when sql-init (doseq [conf sql-init] (.executeUpdate stmt conf))))]

    (try
      (let [rs (.executeQuery stmt "SELECT * FROM asset limit 1")]
        (.next rs))
      (catch Exception e
        (init (:init-sql settings))
        (.executeUpdate stmt "CREATE TABLE asset(key VARCHAR(128) PRIMARY KEY, data VARCHAR(65536))")
        (.executeUpdate stmt "CREATE INDEX master ON asset(key)")
        (println "database created")))

    (let [ps-insert (.prepareStatement db "INSERT INTO asset(key,data) VALUES(?,?)")
          ps-update (.prepareStatement db "UPDATE asset SET key=?, data=? WHERE key=?")
          ps-get (.prepareStatement db "SELECT data from asset WHERE key=?")
          ps-delete (.prepareStatement db "DELETE from asset WHERE key=?")

          vrange2 (fn [n]
                    (loop [i 0 v (transient [])]
                      (if (< i n)
                        (recur (inc i) (conj! v i))
                        (persistent! v))))

          key-data-results (fn [rs]
                             (let [v (transient [])]
                               (loop []
                                 (if (.next rs)
                                   (let [key (.getString rs 1) data (.getString rs 2) struct (json->struct data)]
                                     (conj! v [key struct])
                                     (recur))
                                   (rseq (persistent! v))))))

          data-result (fn [rs]
                        (if (.next rs)
                          (json->struct (.getString rs 1))))

          scan-results (fn [rs limit]
                         ;;        (let [results (apply array-map (flatten (map (fn [row] (let [k (:key row)] [k (json->struct (:data row))])) rows)))
                         (let [results (key-data-results rs)
                               [last-key last-struct] (last results)
                               base (if (and last-key (= (count results) limit)) {:more last-key} {})]
                           (assoc base :structs (seq results))))

          update-or-insert (fn [key struct]
                             (let [data (struct->json struct)]
                               (.setString ps-update 1 key)
                               (.setString ps-update 2 data)
                               (.setString ps-update 3 key)
                               (let [count (.executeUpdate ps-update)]
                                 (if (= 0 count)
                                   (do
                                     (.setString ps-insert 1 key)
                                     (.setString ps-insert 2 data)
                                     (.executeUpdate ps-insert))
                                   count))))
          ]

      (proxy [util.storage.IStructStore] []

        (^clojure.lang.IPersistentMap scan [^String prefix ^String skip ^Number limit]
          (let [lim (if limit (str " limit " limit) "")]
            (if prefix
              (let [stop (str (.substring prefix 0 (dec (count prefix))) (char (+ 1 (int (last prefix)))))]
                (if skip
                  (scan-results (.executeQuery stmt (str "SELECT key, data FROM asset WHERE key > '" skip "' AND '" key "' < '" stop "'" limit)))
                  (scan-results (.executeQuery stmt (str "SELECT key, data FROM asset WHERE key >= '" prefix "' AND '" key "' < '" stop "'" limit))))
                (if skip
                  (scan-results (.executeQuery stmt (str "SELECT key, data FROM asset WHERE key > '" stop "'" limit)))
                  (scan-results (.executeQuery stmt (str "SELECT key, data FROM asset" limit))))))))

        (^clojure.lang.IPersistentMap put [^String key ^clojure.lang.IPersistentMap struct]
          (update-or-insert key struct)
          struct)

        (^clojure.lang.ISeq multiget [^clojure.lang.ISeq keys]
          (doall (map (fn [key]
                        (.setString ps-get 1 key)
                        (data-result (.executeQuery ps-get)))
                      keys)))

        (^Boolean multiput [^clojure.lang.IPersistentMap bindings]
          (do ;;(sql/transaction
            (.setAutoCommit db false)
            (try
              (doall (map (fn [[key struct]]
                            (update-or-insert key struct))
                          bindings))
              (.commit db)
              (catch Exception e
                (.rollback db)
                (throw e)))
            true))

        (^clojure.lang.IPersistentMap get [^String key]
          (.setString ps-get 1 key)
          (data-result (.executeQuery ps-get)))

        (^Boolean clear []
          (.executeUpdate stmt "DELETE FROM asset")
          true)

        (^Boolean delete [^String key]
          (.setString ps-delete 1 key)
          (let [n (.executeUpdate ps-delete)]
            (not (= 0 n))))))))


;;a simple test of inserting 1M records takes h2 53 minutes [com.h2database/h2 "1.3.168"]. That's slow enough that probably I won't use it... but at least it works.
;;
;(defn h2-structstore
;  "Return an IStructStore instance connected to an H2 database with the given name. It is created if it doesn't exist."
;  [name]
;  (jdbc-structstore {:classname "org.h2.Driver" :subprotocol "h2" :subname name}))
;;

;;a simple test of inserting 1M records takes sqlite 49 seconds, using either [sqlitejdbc "0.5.6"] or [org.xerial/sqlite-jdbc "3.7.2"]
;; the xerial one is apache licensed, on google code in Mercurial: `hg clone https://code.google.com/p/xerial/` although I had to fiddle to build on OS X 10.8.1:
;;    export JAVA_HOME=`/usr/libexec/java_home`
;;    sudo ln -s /System/Library/Frameworks/JavaVM.framework/Versions/Current/Headers $JAVA_HOME/include
;; then the make proceeded fine. FYI.
;;
(defn sqlite-structstore
  "Return an IStructStore instance connected to a sqlite database with the given name. It is created if it doesn't exist"
  [name]
  (let [init-sql ["pragma page_size=32768" "pragma cache_size=122000"]]
    (jdbc-structstore {:classname "org.sqlite.JDBC" :subprotocol "sqlite" :subname (str name ".sqlite") :init-sql init-sql})))

