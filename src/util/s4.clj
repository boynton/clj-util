(ns util.s4
  "deploys and runs an Apache S4 cluster on EC2"
  (:use util.supervise)
  (:use util.zookeeper)
  (:use util.ec2))

(def S4_CLUSTER_NAME "s4-node")
(def S4_LOG "s4-install.log")
(def S4_VERSION "0.5.0-incubating")

(defn install-s4 [m zk cluster-name & {:keys [capture nodes port] :or {capture S4_LOG}}]
  (when m
    (deploy-supervise m :capture capture) ;;make sure we have supervise deployed already
    (command m (format "curl -s -o apache-s4-%s-bin.zip http://www.apache.org/dist/incubator/s4/s4-%s/apache-s4-%s-bin.zip && echo downloaded" S4_VERSION S4_VERSION S4_VERSION) :capture capture)
    (command m (format "unzip -oq apache-s4-%s-bin.zip && ln -sf apache-s4-%s-bin s4 && echo unzipped" S4_VERSION S4_VERSION) :capture capture)
	(let [home (.trim (command m "pwd" :capture :string))
	      zk-ip (:cluster-ip zk)
		  run-script (format "#!/bin/sh\ncd %s/s4\n./s4 node -zk=%s -c=%s &>>%s/s4/s4.log\n" home zk-ip cluster-name home)]
		(write-remote-file m run-script "s4/run.sh" :mode 0755)
		(if (and nodes port)
			(command m (format "(cd s4; ./s4 newCluster -c=%s -nbTasks=%d -flp=%d -zk=%s:2181)" cluster-name nodes port zk-ip) :capture capture))
		(supervise m "s4-node" (format "%s/s4/run.sh" home) :capture capture)))
  m)

;;this just dumps the output to the console.
(defn status-s4 [cluster-name]
	(let [zk (first (machines "zookeeper"))
	      node (first (machines cluster-name))]
	(and zk node
		(let [zk-connect (format "%s:2181" (:cluster-ip zk))]
			(command node (format "(cd s4; ./s4 status -zk=%s)" zk-connect))))))

(defn deploy-s4-app [cluster-name app]
	"Fix me!!!")

(defn run-s4 [cluster-name & {:keys [nodes capture first-port] :or {nodes 2 capture S4_LOG first-port 3000}}]
  (let [zk (deploy-zookeeper :capture capture)
	    zk-address (and zk (:cluster-ip zk))
        s4-nodes (create-cluster cluster-name nodes :type "m1.large")
        cluster (cons zk s4-nodes)]
	(install-s4 (first s4-nodes) zk cluster-name :nodes nodes :port first-port :capture capture)  ;; the first one must also init the cluster itself
    (doall (map (fn [node] (install-s4 node zk cluster-name :capture capture)) (rest s4-nodes)))
    cluster))

(defn s4-deployed? [cluster-name]
  (if (first (machines cluster-name)) true false))

(defn deploy-s4 [cluster-name & {:keys [nodes capture] :or {nodes 2 capture S4_LOG}}]
  (let [m (first (machines cluster-name))]
    (or m (run-s4 cluster-name :nodes nodes :capture capture))))

(defn stop-s4 [cluster-name & {:keys [and-zookeeper]}]
  (destroy-cluster cluster-name)
  (if and-zookeeper
    (destroy-cluster ZK_NAME))
  true)
