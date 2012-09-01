(ns util.dynamo
  (:use util.storage)
  (:require [clj-json [core :as json]])
  (:use [clojure.java.io :as io])
  (:use util.aws)
  (:import
   com.amazonaws.auth.BasicAWSCredentials
   com.amazonaws.services.dynamodb.AmazonDynamoDBClient
   com.amazonaws.services.dynamodb.model.DescribeTableRequest
   com.amazonaws.services.dynamodb.model.PutItemRequest
   com.amazonaws.services.dynamodb.model.GetItemRequest
   com.amazonaws.services.dynamodb.model.QueryRequest
   com.amazonaws.services.dynamodb.model.ScanRequest
   com.amazonaws.services.dynamodb.model.DeleteItemRequest
   com.amazonaws.services.dynamodb.model.AttributeValue
   com.amazonaws.services.dynamodb.model.Key))

(defn dynamo-structstore
  "Return an IStructStore instance connected to the specified Amazon DynamoDB table. The credentials for the AWS account should be in your environment (see aws.clj)"
  [table]
  (let [cred (aws-credentials)
        client (AmazonDynamoDBClient. (BasicAWSCredentials. (:access cred) (:secret cred)))
        compound-key-delim ":"
        tbl-descr (try (.getTable (.describeTable client (.withTableName (DescribeTableRequest.) table)))
                       (catch Exception e (do (println "DynamoDB Table not found:" table " -> " (str e)) nil)))
        scan-results (fn [res]
                       (apply array-map
                              (flatten (map (fn [i]
                                              (let [h (.getS (get i "key_hash"))
                                                    r (.getS (get i "key_range"))]
                                                [ (if (= r "-") h (str h compound-key-delim r)) (json/parse-string (.getS (get i "data"))) ]))
                                            (.getItems res)))))
        compound-key (fn [key]
                       (let [[key-hash key-range] (.split key compound-key-delim)]
                         (-> (Key.) (.withHashKeyElement (AttributeValue. key-hash)) (.withRangeKeyElement (AttributeValue. (or key-range "-"))))))
        flatten-key (fn [key]
                      (let [h (.getS (.getHashKeyElement key))
                            r (.getS (.getRangeKeyElement key))]
                        (if (= r "-") h (str h compound-key-delim r)))) ]
    (if tbl-descr
      (proxy [util.storage.IStructStore] []

        (^clojure.lang.PersistentMap scan [^String prefix ^String skip ^Number limit]
          (if prefix
            (let [req (QueryRequest.)
                  stop (str (.substring prefix 0 (dec (count prefix))) (char (+ 1 (int (last prefix)))))]
              (.setTableName req table)
              (.setHashKeyValue req (AttributeValue. prefix))
              (if limit (.setLimit req (int limit)))
              (if skip
                (let [[hash-key range-key] (.split skip compound-key-delim)]
                  (if (not (= hash-key prefix))
                    (throw (Exception. "dynamo-store/range: skip argument must start with the prefix")))
                  (.setExclusiveStartKey req  (compound-key skip))))
              (let [res (.query client req)
                    next-skip (.getLastEvaluatedKey res)
                    base (if next-skip {:more (flatten-key next-skip)} {})]
                (println "next-skip:" next-skip)
                (assoc base :structs (scan-results res))))

            (let [req (ScanRequest. table)]
              (if limit (.setLimit req (int limit)))
              (if skip
                (let [[hash-key range-key] (.split skip compound-key-delim)]
                  (.setExclusiveStartKey req (compound-key skip))))
              (let [res (.scan client req)
                    next-skip (.getLastEvaluatedKey res)
                    base (if next-skip {:more (flatten-key next-skip)} {})]
                (assoc base :structs (scan-results res))))))

        (^clojure.lang.IPersistentMap put [^String key ^clojure.lang.IPersistentMap item]
          (let [[key-hash key-range] (.split key compound-key-delim)
                req {"key_hash" (AttributeValue. key-hash) "key_range" (AttributeValue. (or key-range "-"))
                     "data" (AttributeValue. (json/generate-string item))}
                tmp (.putItem client (PutItemRequest. table req))]
            item))

        (^clojure.lang.IPersistentMap get [^String key]
          (let [req (.withAttributesToGet (GetItemRequest. table  (compound-key key)) ["data"])
                res (.getItem client req)]
            (let [attrmap (.getItem res)]
              (and attrmap (json/parse-string (.getS (.get attrmap "data")))))))

        (^boolean delete [^String key]
          (let [req (DeleteItemRequest. table  (compound-key key))
                res (.deleteItem client req)]
            true))))))
