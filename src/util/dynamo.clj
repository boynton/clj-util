(ns util.dynamo
  (:use util.storage)
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
   com.amazonaws.services.dynamodb.model.BatchWriteItemRequest
   com.amazonaws.services.dynamodb.model.WriteRequest
   com.amazonaws.services.dynamodb.model.PutRequest
   com.amazonaws.services.dynamodb.model.BatchGetItemRequest
   com.amazonaws.services.dynamodb.model.KeysAndAttributes
   com.amazonaws.services.dynamodb.model.Key))

(defn dynamo-structstore
  "Return an IStructStore instance connected to the specified Amazon DynamoDB table. The credentials for the AWS account should be in your environment (see aws.clj)"
  [table]
  (let [cred (aws-credentials)
        client (AmazonDynamoDBClient. (BasicAWSCredentials. (:access cred) (:secret cred)))
        compound-key-delim "~"
        tbl-descr (try (.getTable (.describeTable client (.withTableName (DescribeTableRequest.) table)))
                       (catch Exception e (do (println "DynamoDB Table not found:" table " -> " (str e)) nil)))
        scan-results (fn [res]
                       (apply array-map
                              (flatten (map (fn [i]
                                              (let [h (.getS (get i "key_hash"))
                                                    r (.getS (get i "key_range"))]
                                                [ (if (= r "-") h (str h compound-key-delim r)) (json->struct (.getS (get i "data"))) ]))
                                            (.getItems res)))))
        compound-key (fn [key]
                       (let [[key-hash key-range] (.split key compound-key-delim)]
                         (-> (Key.) (.withHashKeyElement (AttributeValue. key-hash)) (.withRangeKeyElement (AttributeValue. (or key-range "-"))))))
        flatten-key (fn [key]
                      (let [h (.getS (.getHashKeyElement key))
                            r (.getS (.getRangeKeyElement key))]
                        (if (= r "-") h (str h compound-key-delim r))))
        ]
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
                     "data" (AttributeValue. (struct->json item))}
                tmp (.putItem client (PutItemRequest. table req))]
            item))

        (^clojure.lang.IPersistentMap get [^String key]
          (let [req (.withAttributesToGet (GetItemRequest. table  (compound-key key)) ["data"])
                res (.getItem client req)]
            (let [attrmap (.getItem res)]
              (and attrmap (json->struct (.getS (.get attrmap "data")))))))

        (^Boolean multiput [^clojure.lang.IPersistentMap bindings]
          ;;this call is pretty useless unless you have a table provisioned with very high throughput.
          (doall
           (map (fn [chunk]
                  (loop [request-items (doall (map (fn [[k v]]
                                                     (let [[key-hash key-range] (.split k compound-key-delim)
                                                           item {"key_hash" (AttributeValue. key-hash)
                                                                 "key_range" (AttributeValue. (or key-range "-"))
                                                                 "data" (AttributeValue. (struct->json v))}]
                                                       (-> (WriteRequest.) (.withPutRequest (-> (PutRequest.) (.withItem item))))))
                                                   chunk))]
                    (let [req (-> (BatchWriteItemRequest.) (.withRequestItems {table request-items}))
                          res (.batchWriteItem client req)
                          unprocessed (get (.getUnprocessedItems res) table)]
                      (if (= (count unprocessed) 0)
                        true
                        (do
                          (println "Failed to process" (count unprocessed) "items, due to bandwidth constraints. Retrying after a pause...")
                          (Thread/sleep 1000) ;ugh. The affordable write rate is something like 5/sec                          
                          (recur unprocessed))))))
                (partition 25 25 nil bindings))) ;;DynamoDB is hardwired to 25 max writes per batch
          true)

        ;;note: the returned items are not in the same order as the requested keys.
        ;;bug: need to add retry logic like multiput
        (^clojure.lang.ISeq multiget [^clojure.lang.ISeq keys]
          (doall
           (flatten
            (map
             (fn [chunk]
               (loop [request-keys (map compound-key chunk)
                      done nil]
                 (let [req (-> (BatchGetItemRequest.)
                               (.withRequestItems {table (-> (KeysAndAttributes.)
                                                             (.withKeys request-keys)
                                                             (.withAttributesToGet ["data"]))}))
                       res (.batchGetItem client req)]
                   (let [batch-response (get (.getResponses res) table)
                         items (.getItems batch-response)
                         processed (map (fn [m] (.getS (.get m "data"))) items)
                         un (get (.getUnprocessedKeys res) table)
                         unprocessed (and un (.getKeys un))]
                     (if (= (count unprocessed) 0)
                       (concat processed done)
                       (do
                         (println "Failed to process" (count unprocessed) "items, due to bandwidth constraints. Retrying after a pause...")
                         (Thread/sleep 1000) ;ugh. The affordable write rate is something like 10/sec                          
                         (recur unprocessed (concat done processed))))))))
             (partition 100 100 nil keys))))) ;;DynamoDB is hardwired to 100 max reads per batch

        (^Boolean clear []
          false)

        (^Boolean delete [^String key]
          (let [req (DeleteItemRequest. table  (compound-key key))
                res (.deleteItem client req)]
            true))))))
