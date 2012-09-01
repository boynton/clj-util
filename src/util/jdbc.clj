(ns util.jdbc
  (:use util.uuid)
  (:use util.storage)
  (:use [clojure.java.io :as io])
  (:require [clj-json [core :as json]])
  (:require [clojure.java.jdbc :as sql]))

;;
;; Each struct is assumed to be limited in size to 64k of serialized JSON
;;

(defn jdbc-structstore
  "Return an IStructStore instance connected to the JDBC system specified by the settings. Possible keys are :classname, :subprotocol, :subname, :user, and :password"
  [settings]
  (let [url (str "jdbc:" (:subprotocol settings) ":" (:subname settings))
        keep (or (:keep settings) 1)]
    (sql/with-connection settings
      (try
        (and (sql/with-query-results rows ["SELECT * FROM asset limit 1"] (first rows) true))
        (catch Exception e
          (sql/create-table "asset" [:key "varchar(128)" "PRIMARY KEY"] [:data "VARCHAR(65536)"]))))

    (defn scan-results [rows limit]
      (let [results (apply array-map (flatten (map (fn [row] (let [k (:key row)] [k (json/parse-string (:data row))])) rows)))
            [last-key last-struct] (last results)
            base (if (and last-key (= (count results) limit)) {:more last-key} {})]
        (assoc base :structs results)))

    (proxy [util.storage.IStructStore] []

      (^clojure.lang.IPersistentMap scan [^String prefix ^String skip ^Number limit]
        (sql/with-connection settings
          (let [lim (if limit (str " limit " limit) "")]
            (if prefix
              (let [stop (str (.substring prefix 0 (dec (count prefix))) (char (+ 1 (int (last prefix)))))]
                (if skip
                  (sql/with-query-results rows [(str "SELECT key, data FROM asset where key > ? and key < ?" lim) skip stop]
                    (scan-results rows limit))
                  (sql/with-query-results rows [(str "SELECT key, data FROM asset where key >= ? and key < ?" lim) prefix stop]
                    (scan-results rows limit))))
              (if skip
                (sql/with-query-results rows [(str "SELECT key, data FROM asset where key > ?" lim) skip]
                  (scan-results rows limit))
                (sql/with-query-results rows [(str "SELECT key, data FROM asset" lim)]
                  (scan-results rows limit)))))))

      (^clojure.lang.IPersistentMap put [^String key ^clojure.lang.IPersistentMap item]
        (sql/with-connection settings
          (let [s (json/generate-string item)]
            (sql/update-or-insert-values "asset" ["key=?" key] {:key key :data s}) ;;the return value of this is inconsistent, not usable.
            item)))

      (^clojure.lang.IPersistentMap get [^String key]
        (sql/with-connection settings
          (sql/with-query-results rows [(str "SELECT data FROM asset where key=?;") key]
            (json/parse-string (:data (first rows))))))

      (^boolean delete [^String key]
        (sql/with-connection settings
          (let [n (first (sql/delete-rows "asset" ["key=?" key]))]
            (not (= 0 n))))))))

(defn h2-structstore
  "Return an IStructStore instance connected to an H2 database with the given name. It is created if it doesn't exist"
  [name]
  (jdbc-structstore {:classname "org.h2.Driver" :subprotocol "h2" :subname name}))

(defn sqlite-structstore
  "Return an IStructStore instance connected to a sqlite database with the given name. It is created if it doesn't exist"
  [name]
  (jdbc-structstore {:classname "org.sqlite.JDBC" :subprotocol "sqlite" :subname (str name ".sqlite")}))

