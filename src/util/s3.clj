(ns util.s3
  (:use util.storage)
  (:use util.aws)
  (:use [clojure.java.io :as io])
  (:import
   com.amazonaws.services.s3.AmazonS3
   com.amazonaws.services.s3.AmazonS3Client
   com.amazonaws.auth.BasicAWSCredentials
   com.amazonaws.services.s3.model.ObjectMetadata
   com.amazonaws.services.s3.model.ListObjectsRequest
   com.amazonaws.services.s3.model.GetObjectRequest
   com.amazonaws.services.s3.model.PutObjectRequest)
  (:gen-class))

(defn s3-buckets []
  (let [cred (aws-credentials)
        client (AmazonS3Client. (BasicAWSCredentials. (:access cred) (:secret cred)))
        buckets (.listBuckets client)]
    (doall (map (fn [b] (.getName b)) buckets))))

(defn s3-blobstore [bucket]
  (let [cred (aws-credentials)
        client (AmazonS3Client. (BasicAWSCredentials. (:access cred) (:secret cred)))]
    (try
      (let [loc (.getBucketLocation client bucket)]
        '(println "bucket" bucket "exists, location is" loc))
      (catch Exception e
        (if (= 404 (.getStatusCode e))
          (do (println "Bucket not found:" bucket " -> " (str e)) nil)
          (do (println "Cannot access bucket:" bucket " -> " (str e)) nil))))

    (proxy [util.storage.IBlobStore] []

      (^boolean put [^String key ^util.storage.Blob blob]
        (let [data (:data blob)
              len (:content-length blob)
              meta (ObjectMetadata.)]
          (.setContentType meta (:content-type blob))
          (if (instance? (Class/forName "[B") data)
            (let [instream (java.io.ByteArrayInputStream. data)]
              (.setContentLength meta (or len (count data)))
              (.putObject client (PutObjectRequest. bucket key instream meta)))
            (let [instream data]
              (if len
                (.setContentLength meta len))
              (.putObject client (PutObjectRequest. bucket key instream meta))))
          true))

      (^util.storage.Blob get [^String key]
        (let [s3obj (.getObject client (GetObjectRequest. bucket key))
              data (.getObjectContent s3obj)
              meta (.getObjectMetadata s3obj)
              len (.getContentLength meta)
              type (.getContentType meta)
              modified (.getLastModified meta)]
          (util.storage.Blob. data len type modified)))

      (^clojure.lang.IPersistentMap scan [^String prefix ^String skip ^Integer limit]
        (let [req (-> (ListObjectsRequest.) (.withBucketName bucket))]
          (if prefix
            (.setPrefix req prefix))
          (if limit
            (.setMaxKeys req limit))
          (if skip
            (.setMarker req skip))
          (let [res (.listObjects client req)
                marker (.getNextMarker res)
                summaries (.getObjectSummaries res)
                lst (apply array-map (flatten (map (fn [s] (let [k (.getKey s)] [k (blob nil :content-length (.getSize s) :content-type (file-type k) :last-modified (.getLastModified s))])) summaries)))]
            (if marker
              {:summaries lst :more marker}
              {:summaries lst}))))
      
      (^boolean delete [^String key]
        (.deleteObject client bucket key)
        true))))
