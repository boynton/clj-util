(ns util.storage
  (:import java.io.File java.io.ByteArrayOutputStream java.io.FileOutputStream java.io.FileInputStream java.net.URLConnection)
  (:use [clojure.java.io :as io])
  (:gen-class))

;;
;; A generic structured storage interface
;;
(definterface IStructStore
  (^clojure.lang.IPersistentMap put [^String key ^clojure.lang.IPersistentMap struct] "puts a map to storage. Returns the (possibly modified) map")
  (^clojure.lang.IPersistentMap get [^String key] "gets a struct from storage and returns it as a map")
  (^clojure.lang.PersistentArrayMap scan [^String prefix ^String skip ^Number limit] "gets a range of key/values as a sequence")
  ;;  (multiget [keys] "gets a list of structs matching the list of keys")
  ;;  (multiput [keys structs] "puts a list of structs matching the list of keys")
  (^boolean delete [^String key] "deletes the key/struct binding from the store, returns the number of records effected"))

(defn ^clojure.lang.IPersistentMap put-struct [^util.storage.IStructStore store ^String key ^clojure.lang.IPersistentMap struct]
  (.put store key struct))

(defn ^clojure.lang.IPersistentMap get-struct [^util.storage.IStructStore store ^String key]
  (.get store key))

(defn ^clojure.lang.PersistentArrayMap scan-structs [^util.storage.IStructStore store & {:keys [prefix skip limit]}]
  (.scan store prefix skip limit))

(defn ^boolean delete-struct [^util.storage.IStructStore store ^String key]
  (.delete store key))

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
