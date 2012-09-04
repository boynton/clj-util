(ns util.file
  (:use util.uuid)
  (:use util.storage)
  (:use [clojure.java.io :as io])
  (:import java.io.File java.io.ByteArrayOutputStream java.io.FileOutputStream java.io.FileInputStream java.net.URLConnection))

(defn file-blobstore [dirname]
  ;; files are stored in the directory structure. This requires path parsing. i.e. "<dirname>/some/path/to/my/file"
  ;; this class can be wrapped around any directory, all metadata is inferred from the filesystem
  (let [root (File. dirname)]
    (if (.exists root)
      (if (not (.isDirectory root))
        (throw (Exception. (str "file-blobstore: not a directory: " dirname))))
      (.mkdirs root))

    (proxy [util.storage.IBlobStore] []

      (^boolean put [^String key ^util.storage.Blob blob]
        (let [file (File. root  key)
              data (:data blob)]
          (.mkdirs (.getParentFile file))
          (with-open [out (FileOutputStream. file)]
            (if (instance? java.io.InputStream data)
              (io/copy data out)
              (.write out data))
            true)))

      (^util.storage.Blob get [^String key]
        (let [file (File. root key)]
          (if (.exists file)
            (file-blob file))))

      (^boolean delete [^String key]
        (let [file (File. root key)]
          (.delete file))))))















