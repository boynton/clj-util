# clj-util

A library of simple utilities and syntactic sugar for commonly used functionality.

## Usage

### Installation

The maven artifact is available from [Clojars](http://clojars.org/boynton/clj-util).
The [Leiningen](http://github.com/technomancy/leiningen) dependency should look like this:
Leiningen

```clojure
[boynton/clj-util "0.0.9"]
```

### Using UUIDs

The UUID API is a set of a few simple wrappers for the UUID library written by [Tatu Saloranta](https://github.com/cowtowncoder/java-uuid-generator).
You can create uuids based on time, URLs, and names (using a UUID as the namespace). Additionally, a function `uuid-to-sortable-string`
function produces a string for the UUID that sorts correctly for time-based UUIDs.

```clojure
(use 'util.uuid)

(let [namespace-uuid (uuid-from-url "https://github.com/boynton")
      name-uuid (uuid-from-name namespace-uuid "foo")
      revision-uuid (uuid-from-current-time)]
  (println "url-based UUID from 'https://github.com/boynton':" namespace-uuid);                                                                              
  (println "using that as a namespace, the name-based UUID for 'foo' is" name-uuid)
  (println "here is a time-based UUID:" revision-uuid)
  (println "here is that same UUID as a sortable string:" (uuid-to-sortable-string revision-uuid)))
```	

### Simple Storage Abstractions

The `util.storage` namespace provide a couple of simple storage abstractions, one for BLOB storage and one for Structured storage.

#### BlobStorage

The Blob storage is defined by an interface `IBlobStorage`, with methods to put, get, scan and delete Blob objects. A Blob
object is a record with the data as well as content-length, content-type, and last-modified attributes.

This interface can be implemented in Java, but a few implementations are provided in clojure: `util.file` provides a simple
wrapper around a local filesystem directory, and 'util.s3' provides a wrapper around Amazon's S3.

For example:

```clojure
(use 'util.storage)
(use 'util.file)

(let [store (file-blobstore "dirname")
      blob1 (string-blob "This is a test" :content-type "text/plain")
      blob2 (file-blob "some-file.jar")
      file (java.io.File "README.md")
      blob3 (blob (java.io.FileInputStream file) :content-type "text/markdown" :content-length (.length file) :last-modified (.getLastModified file))]
   (put-blob store "one" blob1)
   (put-blob store "two" blob2)
   (put-blob store "three" blob3)
   (println (get-blob store "one"))
   (let [byte-array (:data (read-blob-fully (get-blob store "blob3")))]
      (println "byte array retrieved with length" (count byte-array)))
   (let [b (get-blob store "two")]
        (write-blob-file b "some-other-file.jar")) ; in this example, the streams are copied, it isn't inmemory all at once
   (loop [chunk (scan-blobs store)]
      (doseq [info (:summaries chunk)]
             (println info))
      (let [more (:more chunk)]
         (if more
           (do
                (println "-------")
                (recur (scan-blobs store :skip more)))))))
```

#### StructStorage

Structed storage is aimed at smaller structured values in a key/value store. AN interface `IStructStorage` is provided for this
that provides put, get, scan, and delete operations, and the value passed around is a `clojure.lang.IPersistentMap`, with the
requirement that the object be serializable to JSON. The value size is limited to 64k to be compatable with a variety of
storage services.

The java implementations include `util.jdbc`, which provides a generic JDBC implementation and both SQLite and H2 examples,
and a `util.dynamo` implementation that provides identical functionality on Amazon's DynamoDB service.

For example:
```clojure
(use 'util.storage)
(use 'util.jdbc)

(let [store (sqlite-structstore "filename")]
     (put-struct store "one" {:title "This is a test" :foo [1 2 3]})
     (put-struct store "two" {:title "Another one" :bar {:bletch 23}})
     (println "one:" (get-struct store "one"))
   (loop [chunk (scan-structs store)]
      (doseq [[key struct] (:structs chunk)]
             (println key "->" struct))
      (and (:more chunk)
           (recur (scan-blobs store :skip (:more chunk))))))
```

### AWS services

The above examples work with S3 and DynamoDB, respectively. Both require that you define your amazon credentials in
the environment, then access them with `util.aws` as follows:

```clojure
(use 'util.storage)
(use 'util.aws)
(use 'util.s3)

(let [s3 (s3-blobstore "my-s3-bucket-name")]
   (put-blob store "one" blob1)
   ...
```

The credentials are fetched with the `aws-credentials` function in util.aws.

#### EC2

There also is an interface to EC2 for provisioning and talking to clusters of machines in the cloud.

For example:

```clojure
(use 'util.aws)
(use 'util.ec2)

(let [machines (create-cluster "myname" 3 :ami "ami-aecd60c7" :type "t1.micro" :keypair  "ec2keypair" :user "ec2-user" :security "default")]
     (map (fn [machine] (command machine "sudo yum -y install emacs")) machines)
     (put-file (first machines) "my/local/file" "remote/file")
     (get-file (second machines) "remote/file" "local/file")
     ...)
```

A more involved example is in the works.

#### Supervise, Zookeeper, Storm, S4

These are minimal (as in: very primitive) utilities to deploy Bernstein's [daemontools](http://cr.yp.to/daemontools.html),
[Apache Zookeeper](http://zookeeper.apache.org), and [Storm](https://github.com/nathanmarz/storm) to the cloud, using the `util.ec2` package.

For example, to deploy a 6 node (1 zookeeper, 1 nimbus, and 4 workers) Storm cluster to EC2:

```clojure
(use 'util.storm)

(deploy-storm :workers 4)
```

It takes less than 15 minutes for everything to get running, initialized, deployed, and launched under supervision. It automatically
modifies your local `~/.storm/storm.yaml` file so that the local storm client connects to it. It uses a bunch of defaults which
should probably be made overridable, but it is a starting point, anyway. This is not meant to replace a more comprehensive solution
like Pallet/JClouds, but I had trouble debugging problems with that system, so I wrote this quick hack for playing around. Don't
take it too seriously :-)

The same thing works for [S4](http://incubator.apache.org/s4/), now that v0.5 makes things easier:

```clojure
(use 'util.s4)

(deploy-s4 "my-cluster" :nodes 4)

(status-s4 "my-cluster")
```

The difference is that for s4, the cluster is dedicated to a logical s4 "cluster". Different partitioning (and node allocation)
requires deploying different clusters.


## License

Copyright (C) 2012 Lee Boynton

Distributed under the Eclipse Public License, the same as Clojure.
