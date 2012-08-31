(ns util.ec2
  (:use util.aws)
  (:require [util.ssh :as ssh])
  (:import com.amazonaws.services.ec2.AmazonEC2Client
           com.amazonaws.auth.BasicAWSCredentials
           com.amazonaws.services.ec2.model.RunInstancesRequest
           com.amazonaws.services.ec2.model.RunInstancesResult
           com.amazonaws.services.ec2.model.DescribeInstancesRequest
           com.amazonaws.services.ec2.model.StopInstancesRequest
           com.amazonaws.services.ec2.model.StartInstancesRequest
           com.amazonaws.services.ec2.model.TerminateInstancesRequest
           com.amazonaws.services.ec2.model.BlockDeviceMapping
           com.amazonaws.services.ec2.model.CreateTagsRequest
           com.amazonaws.services.ec2.model.Tag)
  (:gen-class))

(defn- ec2 [cred]
  (AmazonEC2Client. (BasicAWSCredentials. (:access cred) (:secret cred))))

;; default to the credentials in the environment. 
(def ^:dynamic *ec2* (ec2 (aws-credentials)))

(defmacro with-credentials [cred & body]
  "Use the specified credentials while executing the body. The credentials are unique per thread"
  `(binding [*ec2* (ec2 ~cred)]
     (do ~@body)))

(defn use-credentials [cred]
  "Use the specified credentials for subsequent interface with EC2"
  (set! *ec2* (ec2 cred)))


(defn- ec2-instances [& args]
  (let [cluster (first args)]
    (let [reservations (.describeInstances *ec2*)
          filter-instance (if cluster (fn [instance] (some #(and (= "Cluster" (.getKey %)) (= cluster (.getValue %))) (.getTags instance))) identity)]
      (flatten
       (map (fn [reservation] (filter filter-instance (.getInstances reservation)))
            (.getReservations reservations))))))

(defn- ec2-instance [id]
  (let [instReq (DescribeInstancesRequest.)]
    (.setInstanceIds instReq [id])
    (let [instDesr (.describeInstances *ec2* instReq)]
      (first (.getInstances (first (.getReservations instDesr)))))))


(defn- ec2-stop-instances [ids]
  (.stopInstances *ec2* (StopInstancesRequest. ids)))

(defn- ec2-start-instances [ids]
  (.startInstances *ec2* (StartInstancesRequest. ids)))

(defn- ec2-destroy-instances [ids]
  (.terminateInstances *ec2* (TerminateInstancesRequest. ids)))

(defn- ec2-regions []
  (map (fn [x] (bean x)) (seq (.getRegions (.describeRegions *ec2*)))))

(defn- ec2-zones []
  (.describeAvailabilityZones *ec2*))

(defn- ec2-tag-instances [ids name user]
  (let [cnt (count ids)
        names (map (fn [i] (str name "-" i)) (range 0 cnt))
        user-tag (Tag. "User" user)
        cluster-tag (Tag. "Cluster" name)]
    (map (fn [inst-id inst-name]
           (let [tr (CreateTagsRequest.)]
             (.setResources tr [inst-id])
             (.setTags tr [(Tag. "Name" inst-name) cluster-tag user-tag])
             (.createTags *ec2* tr)
             inst-id))
         ids
         names)))

;; Create a cluster of machines. The cluster name is required, and used to tag the instances, Returns the list of instance-ids for the machines
;; instance id
(defn- ec2-create-instances [cnt ami type keypair security blockdev]
  (let [req (RunInstancesRequest.  ami (Integer. cnt) (Integer. cnt))]
    (.setKeyName req keypair)
    (.setInstanceType req type)
    (.setSecurityGroups req [security])
    (if blockdev
      (let [[bdev vdev] (vec (.split blockdev "="))]
        (.setBlockDeviceMappings req [(-> (BlockDeviceMapping.) (.withDeviceName bdev) (.withVirtualName vdev))])))
    (let [resp (.runInstances *ec2* req)
          instances (.getInstances (.getReservation resp))]
      (map (fn [i] (.getInstanceId i)) instances))))

;; Wait for as long as the instance maintains the specified state, or until the wait time in seconds elapses. The id is returned for successful state change, nil for timeout
(defn- ec2-wait-while-state [id state wait]
  (let [end-time (+ (* 1000 (max 1 wait)) (System/currentTimeMillis))
        sleep-duration (* 1000 (min 5 wait))]
    (loop [inst (ec2-instance id)]
      (if (= (.getName (.getState inst)) state)
        (if (<= (System/currentTimeMillis) end-time)
          (do
            (Thread/sleep sleep-duration)
            (recur (ec2-instance id)))
          nil)
        id))))

;; Machine is a simple abstraction for an executing instance
(defrecord Machine
    [id name cluster state host ip cluster-ip keypair user ami type region arch security]
  )

(defn- make-machine [instance]
  (and
   instance
   (let [id (.getInstanceId  instance)
         tags (apply hash-map (flatten (map (fn [t] (list (keyword (.getKey t)) (.getValue t))) (.getTags instance))))
         name (:Name tags)
         cluster (:Cluster tags)
         user (:User tags)
         host (.getPublicDnsName instance)
         ip (.getPublicIpAddress instance)
         cluster-ip (.getPrivateIpAddress instance)
         keypair (.getKeyName instance)
         ami (.getImageId instance)
         type (.getInstanceType instance)
         state (.getName (.getState instance))
         region (.getAvailabilityZone (.getPlacement instance))
         arch (.getArchitecture instance)
         security (.getGroupName (first (.getSecurityGroups instance)))
         ]
     (Machine. id name cluster state host ip cluster-ip keypair user ami type region arch security))))

(defn machines
  ([]  (filter #(not= "terminated" (:state %)) (map make-machine (ec2-instances))))
  ([cluster] (filter #(not= "terminated" (:state %)) (map make-machine (ec2-instances cluster)))))

(defn machine [id]
  (make-machine (ec2-instance id)))

(defn pending-machines
  ([]  (filter #(= "pending" (:state %)) (machines)))
  ([cluster] (filter #(= "pending" (:state %)) (machines cluster))))

(defn running-machines
  ([]  (filter #(= "running" (:state %)) (machines)))
  ([cluster] (filter #(= "running" (:state %)) (machines cluster))))

(defn stopped-machines
  ([]  (filter #(= "stopped" (:state %)) (machines)))
  ([cluster] (filter #(= "stopped" (:state %)) (machines cluster))))

(defn stop-machine [m]
  (ec2-stop-instances [(:id m)]))
(defn stop-machines [lst]
  (ec2-stop-instances (map :id lst)))
(defn stop-cluster [cluster]
  (ec2-stop-instances (map :id (running-machines cluster))))
(defn stop-all []
  (ec2-stop-instances (map :id (running-machines))))

(defn start-machine [m]
  (ec2-start-instances [(:id m)]))
(defn start-cluster [cluster]
  (ec2-start-instances (map :id (stopped-machines cluster))))
(defn start-all []
  (ec2-start-instances (map :id (stopped-machines))))

(defn destroy-machine [m]
  (and (ec2-destroy-instances [(:id m)]) true))
(defn destroy-machines [lst]
  (and (ec2-destroy-instances (map :id lst)) true))
(defn destroy-cluster [cluster]
  (and (ec2-destroy-instances (map :id (machines cluster))) true))
(defn destroy-all []
  (and (ec2-destroy-instances (map :id (machines))) true))

(defn command [m cmd & {:keys [wait capture append] :or {wait 60 capture :console append true}}]
  "Execute a remote command on the specified Machine"
  (and (= (:state m) "running")
       (ssh/command m cmd :wait wait :capture capture :append append)))

(defn put-file [m src dst]
  "Puts a file to the remote machine via scp"
  (and m
       (= (:state m) "running")
       (ssh/put-file m src dst :wait 300)))

(defn get-file [m src dst]
  "Gets a file from the remote machine via scp"
  (and m
       (= (:state m) "running")
       (ssh/get-file m src dst :wait 300)))


(defn- wait-for-ssh [m & {:keys [wait] :or {wait 60}}]
  (let [end-time (+ (* 1000 (max 1 wait)) (System/currentTimeMillis))
        sleep-duration (* 1000 (min 5 wait))]
    (loop []
      (if (> (System/currentTimeMillis) end-time)
        nil
        (or
         (try
           (command m "echo hello" :capture :silent :wait 15)
           (catch Exception e false))
         (do (Thread/sleep sleep-duration) (recur)))))))

(defn wait-for-pending-ids [ids timeout]
  (let [alive (doall (map (fn [id] (if (= :timeout (ec2-wait-while-state id "pending" timeout)) nil id)) ids))]
    (doall (map (fn [id] (if id (if (= :timeout (wait-for-ssh (machine id) :timeout timeout)) nil id) nil)) alive))))


(defn create-cluster [cluster cnt & {:keys [ami type keypair user security count wait blockdev]
                                     :or {ami "ami-aecd60c7" type "t1.micro"
                                          keypair  "ec2keypair" user "ec2-user" security "default" wait 300}}]
  (let [ms (machines cluster)]
    (if (seq ms)
      (throw (Exception. (str "Cluster already exists: " cluster)))
      (let [ids (doall (ec2-create-instances cnt ami type keypair security blockdev))]
        (doall (ec2-tag-instances ids cluster user))
        (if wait
          (wait-for-pending-ids ids wait))
        (map machine ids)))))


(defmacro create-machine [name & rest]
  `(first (create-cluster ~name 1 ~@rest)))

'(defn create-machine [name & {:keys [ami type keypair user security count timeout blockdev]
                               :or {ami "ami-aecd60c7" type "t1.micro"
                                    keypair  "ec2keypair" user "ec2-user" security "default" wait 300}}]
   (let [m (first (create-cluster name 1 :wait true :timeout wait))]
     (if wait
       (wait-for-ssh m :timeout wait))
     (machine (:id m)))) ;;get an up-to-date reading on its state
