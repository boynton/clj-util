(ns util.storage
  (:import java.io.File java.io.ByteArrayOutputStream java.io.FileOutputStream java.io.FileInputStream java.net.URLConnection)
  (:use [clojure.java.io :as io])
  (:use clojure.walk)
  (:require [clj-json [core :as json]]))

;;
;; A generic structured storage interface
;;
(definterface IStructStore
  (^clojure.lang.IPersistentMap put [^String key ^clojure.lang.IPersistentMap struct] "puts a map to storage. Returns the (possibly modified) map")
  (^clojure.lang.IPersistentMap get [^String key] "gets a struct from storage and returns it as a map")
  (^clojure.lang.IPersistentMap scan [^String prefix ^String skip ^Number limit] "gets a range of key/values as a sequence of entries")
  (^clojure.lang.ISeq multiget [^clojure.lang.ISeq keys] "gets a list of structs matching the list of keys")
  (^Boolean multiput [^clojure.lang.IPersistentMap bindings] "merges the bindings into the store")
  (^Boolean delete [^String key] "deletes the key/struct binding from the store, returns the number of records effected")
  (^Boolean clear [] "deletes all bindings, making the store empty"))

(defn ^clojure.lang.IPersistentMap put-struct [^util.storage.IStructStore store ^String key ^clojure.lang.IPersistentMap struct]
  (.put store key struct))

(defn ^Boolean put-structs [^util.storage.IStructStore store ^clojure.lang.IPersistentMap bindings]
  (.multiput store bindings))

(defn ^clojure.lang.IPersistentMap get-struct [^util.storage.IStructStore store ^String key]
  (.get store key))

(defn ^clojure.lang.IPersistentMap get-structs [^util.storage.IStructStore store ^clojure.lang.ISeq keys]
  (.multiget store keys))

(defn ^clojure.lang.IPersistentMap scan-structs [^util.storage.IStructStore store & {:keys [prefix skip limit]}]
  (.scan store prefix skip limit))

(defn ^boolean delete-struct [^util.storage.IStructStore store ^String key]
  (.delete store key))

(defn ^String struct->json [^clojure.lang.IPersistentMap dat]
  (json/generate-string dat))

(defn ^clojure.lang.IPersistentMap json->struct [^String s]
  (keywordize-keys (json/parse-string s)))

;;
;; A simple memory-based implementation
;;
(defn mem-structstore []
  (let [state (atom (sorted-map))
        scan-results (fn [s limit]
                       (let [results (apply array-map (flatten s))
                             [last-key last-struct] (last results)
                             base (if (and last-key limit (= (count results) limit)) {:more last-key} {})]
                         (assoc base :structs (seq results))))
        ]
    
    (proxy [util.storage.IStructStore] []
      
      (^clojure.lang.IPersistentMap put [^String key ^clojure.lang.IPersistentMap struct]
        (swap! state assoc key struct))
          
      (^clojure.lang.IPersistentMap get [^String key]
        (get @state key))

      (^clojure.lang.ISeq multiget [^clojure.lang.ISeq keys]
        (let [s @state]
          (doall (map (fn [key] (get s key)) keys))))
      
      (^Boolean multiput [^clojure.lang.IPersistentMap bindings]
        (swap! state merge bindings)
        true)
    
      (^clojure.lang.IPersistentMap scan [^String prefix ^String skip ^Number limit]
        (let [s @state]
          (if prefix
            (let [stop (str (.substring prefix 0 (dec (count prefix))) (char (inc (int (last prefix)))))
                  results (if skip (subseq s > skip < stop) (subseq s >= prefix < stop))]
              (scan-results (if limit (take limit results) results) limit))
            (if skip
              (let [results (subseq s > skip)]
                (scan-results (if limit (take limit results) results) limit))
              (if limit
                (scan-results (take limit (seq s)) limit)
                {:structs s})))))
      
      (^Boolean delete [^String key]
        (swap! state dissoc key)
        true)

      (^Boolean clear []
        (swap! state sorted-map))
      )))


(defn write-struct-file [store filename]
  (let [f (File. filename)]
    (with-open [w (clojure.java.io/writer f)]
      (doall
       (map (fn [[key val]]
              (.write w (str key \tab (json/generate-string val) \newline))
              true)
            (:structs (scan-structs store)))))))

(defn read-struct-file [store filename & {:keys [clear batch] :or {batch 1000}}]
  (let [f (File. filename)]
    (when (.exists f)
      (if (.isDirectory f)
        (throw (Exception. (str "file-structstore: not a valid file: " filename))))
      (when clear
        (.clear store))
      (with-open [r (clojure.java.io/reader f)]
        (doall (map (fn [chunk]
                      (let [tmp (transient {})]
                        (doseq [line chunk]
                          (let [[key val] (.split line "\t") ;5.4 sec for this only
                                item (keywordize-keys (json/parse-string val))] ;13.4 if you include this
                            (assoc! tmp key item)
                            ))
                        (.multiput store (persistent! tmp))))
                    (partition batch batch nil (line-seq r))))
        true))))

;;
;; binary data plus minimal metadata. The data can be a byte array or an InputStream.
;;
(defrecord Blob [data content-length content-type last-modified])

(defn slurp-bytes [in]
  (let [out (ByteArrayOutputStream.)]
    (io/copy in out)
    (.toByteArray out)))

(defn file-type [name]
  (URLConnection/guessContentTypeFromName name))

(defn ^util.storage.Blob blob [b & {:keys [content-type last-modified] :or {content-type "application/octet-stream" last-modified (java.util.Date.)}}]
  (Blob. b (and b (count b)) content-type last-modified))

(defn ^util.storage.Blob string-blob [^String data & {:keys [content-type last-modified] :or {content-type "text/plain" last-modified (java.util.Date.)}}]
  (blob (.getBytes data "UTF-8") :content-type content-type :last-modifier last-modified))

(defn ^util.storage.Blob file-blob [file & {:keys [content-type last-modified] :or {content-type "application/octet-stream"}}]
  (let [f (if (string? file) (File. file) file)
        b (FileInputStream. f)
        len (int (.length f))
        type (file-type (.getName f))
        modified (or last-modified (java.util.Date. (.lastModified f)))]
    (Blob. b len type modified)))

(defn ^String write-blob-file [^util.storage.Blob blob ^String filename]
  (with-open [out (FileOutputStream. filename)]
    (let [data (:data blob)]
      (if (instance? java.io.InputStream data)
        (io/copy data out)
        (.write out data))))
  filename)

(defn ^util.storage.Blob read-blob-fully [^util.storage.Blob blob]
  (let [data (:data blob)
        len (:content-length blob)]
    ;;sanity check? too big and we'll run out of memory!
    (if (instance? java.io.InputStream data)
      (Blob. (slurp-bytes data) len (:content-type blob) (:last-modified blob))
      blob)))


;;
;; A generic blob storage interface
;;
(definterface IBlobStore
  (^boolean put [^String key ^util.storage.Blob blob] "put a blob to storage. The data in the blob can be either a byte array or an input stream")
  (^util.storage.Blob get [^String key] "get a blob from storage as a stream")
  (^clojure.lang.IPersistentMap scan [^String prefix ^String skip ^Number limit] "scans for blob info. The resulting map has :summaries and optionally a :more value that can be passed as :skip on the next call")
  (^boolean delete [^String key] "remove blob from storage"))

(defn ^clojure.lang.PersistentArrayMap scan-blobs [^util.storage.IBlobStore store & {:keys [prefix skip limit]}]
  (.scan store prefix skip limit))

(defn ^boolean put-blob [^util.storage.IBlobStore store ^String key ^util.storage.Blob val]
  (.put store key val))

(defn ^util.storage.Blob get-blob [^util.storage.IBlobStore store ^String key]
  (.get store key))
