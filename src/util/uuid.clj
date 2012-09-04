(ns util.uuid
  (:import com.fasterxml.uuid.Generators
           com.fasterxml.uuid.NoArgGenerator
           com.fasterxml.uuid.EthernetAddress
           com.fasterxml.uuid.impl.NameBasedGenerator
           java.security.MessageDigest
           java.util.UUID))

(defn ^java.util.UUID uuid
  "Return the UUID for the argument, whcih must be a UUID or the string representation of one"
  [o]
  (if (= java.util.UUID (type o))
    o
    (java.util.UUID/fromString o)))

(defn ^java.util.UUID uuid-from-current-time []
  "Create a time-based UUID from the current time"
  (let [addr (EthernetAddress/constructMulticastAddress (java.util.Random. (System/currentTimeMillis)))
        gen (Generators/timeBasedGenerator addr)]
    (.generate gen)))

(defn ^java.util.UUID uuid-from-name [^java.util.UUID namespace-uuid ^String name]
  "Create a name-based UUID for the specified name, in the specified namespace (itself a UUID)"
  (.generate (Generators/nameBasedGenerator namespace-uuid (MessageDigest/getInstance "MD5")) name))

(defn ^java.util.UUID uuid-from-url [^String url]
  "Create a name-based UUID int he URL namespace for the specified URL string"
  (uuid-from-name NameBasedGenerator/NAMESPACE_URL url))

(defn ^String uuid-to-sortable-string [^java.util.UUID uuid]
  "Returns a string that lexically sorts as you would expect a time-based UUID to"
  (let [u (.replace (str uuid) "-" "")
        v (vec (map (fn [y] (apply str y)) (doall (map force (partition 2 u)))))]
    (apply str (seq [(v 6) (v 7) (v 4) (v 5) (v 0) (v 1) (v  2) (v 3)  (v 8) (v 9) (apply str (drop 10 v))]))))
