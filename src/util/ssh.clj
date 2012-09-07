(ns util.ssh
  (:import java.io.File
           java.io.IOException
           java.io.FileOutputStream
           java.io.ByteArrayInputStream
           java.util.concurrent.TimeUnit
           net.schmizz.sshj.SSHClient
           net.schmizz.sshj.xfer.FileSystemFile
           net.schmizz.sshj.transport.verification.HostKeyVerifier
           net.schmizz.sshj.userauth.keyprovider.OpenSSHKeyFile
           net.schmizz.sshj.common.IOUtils
           net.schmizz.sshj.common.StreamCopier))

(defn- endpoint-connection [endpoint wait]
  (if (string? endpoint)
    (endpoint-connection {:host endpoint} wait)
    (let [connection (SSHClient.)
          host (or (:host endpoint) (throw (Exception. (str "Missing host parameter on endpoint: " endpoint))))
          keypair (or (:keypair endpoint) "id_rsa")
          user (or (:user endpoint) (get (System/getenv) "USER"))
          keypairpath (str (get (System/getenv) "HOME") "/.ssh/" keypair)]
      (.addHostKeyVerifier connection (proxy [HostKeyVerifier] [] (verify [arg0 arg1 arg2] true)))
      (.connect connection host)
      (let [kfile (OpenSSHKeyFile.)
            kname (if (or (= "id_rsa" keypair) (.endsWith ".pem" keypair)) keypair (str keypair ".pem"))]
        (.init kfile (File.  (str (System/getProperty "user.home") "/.ssh/" kname)))
        (.authPublickey connection user [kfile])
        connection))))

(defn command [endpoint cmd & {:keys [wait capture append] :or {wait 60 capture :console append true}}]
  "Execute a remote command on the specified endpoint. An endpoint can be a hostname, or a map with :host, :user, and :keypair keys. The :capture argument determines what to do with output, and can either be :console (the default), :string to return all output as a string, :silent to drop it, or a filename to write to. The :append argument defaults to true, but can be set to false to first truncate the file"
  (let [connection (endpoint-connection endpoint wait)]
    (try
      (let [session (.startSession connection)]
        (try
          (.allocateDefaultPTY session) ; so sudu will work
          (let [c (.exec session cmd)
                strm (.getInputStream c)]
            (case capture
              :string
              (let [tmp (if strm (IOUtils/readFully strm) nil)
                    out (if tmp (.toString tmp) nil)]
                (.join c wait TimeUnit/SECONDS)
                (let [status (.getExitStatus c)]
                  (if (= 0 (.getExitStatus c))
                    out
                    (throw (IOException. (str "Remote error (" status "): " out))))))
              :console
              (let [copier (StreamCopier. strm System/out)]
                (.copy copier)
                (.join c wait TimeUnit/SECONDS)
                (let [status (.getExitStatus c)]
                  (or
                   (= 0 status)
                   (throw (IOException. (str "Remote error (" status ")"))))))
              :silent
              (do
                (.join c wait TimeUnit/SECONDS)
                (or
                 (= 0 (.getExitStatus c))
                 (throw (IOException. (.toString (IOUtils/readFully strm))))))
              ;;file
              (let [outstrm (FileOutputStream. capture append)
                    copier (StreamCopier. strm outstrm)]
                (try
                  (.copy copier)
                  (finally (.close outstrm)))
                (.join c wait TimeUnit/SECONDS)
                (let [status (.getExitStatus c)]
                  (or
                   (= 0 status)
                   (throw (IOException. (str "Remote error (" status ")"))))))))
          (finally (.close session))))
      (finally (.disconnect connection)))))

(defn string-file-source [name contents & {:keys [mode] :or {mode 0644}}]
  (let [buf (.getBytes contents "UTF-8")
        len (count buf)]
    (proxy [net.schmizz.sshj.xfer.LocalSourceFile] []
      (^String getName [] name)
      (^long getLength [] len)
      (getInputStream  [] (ByteArrayInputStream. buf))
      (^int getPermissions [] mode)
      (isFile [] true)
      (isDirectory [] false)
      (providesAtimeMtime [] false)
      (getChildren [] nil)
      (getLastAccessTime [] nil)
      (getLastModifiedTime [] nil))))

(defn write-remote-file [endpoint contents dst & {:keys [wait mode] :or {wait 300}}]
  "Puts the string as the contnts a file on the remote machine via scp"
  (let [connection (endpoint-connection endpoint wait)
        name (last (.split dst "/"))]
    (try
      (do (.upload (.newSCPFileTransfer connection) (string-file-source name contents :mode (or mode 0644)) dst) true)
      (finally (.disconnect connection)))))

(defn put-file [endpoint src dst & {:keys [wait mode] :or {wait 300}}]
  "Puts a file to the remote machine via scp"
  (let [connection (endpoint-connection endpoint wait)]
    (try
      (let [s (FileSystemFile. src)]
        (if mode (.setPermissions s mode))
        (.upload (.newSCPFileTransfer connection) s dst)
        true)
      (finally (.disconnect connection)))))

(defn get-file [endpoint src dst & {:keys [wait] :or {wait 300}}]
  "Gets a file from the remote machine via scp"
  (let [connection (endpoint-connection endpoint wait)]
    (try
      (do (.download (.newSCPFileTransfer connection) src (FileSystemFile. dst)) true)
      (finally (.disconnect connection)))))
