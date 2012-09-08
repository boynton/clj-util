(ns util.supervise
  "Sets up Bernstein's daemontools on a generic linux image"
  (:use util.ec2))

(defn generate-supervise-install-script []
"#!/bin/sh
if [ -e /etc/init/svscan.conf ]; then exit 0 ; fi
mkdir -p /package || exit 1
chmod 1755 /package || exit 1
cd /package || exit 1
curl -o daemontools-0.76.tar.gz http://cr.yp.to/daemontools/daemontools-0.76.tar.gz || exit 1
tar -xzpf daemontools-0.76.tar.gz || exit 1
cd admin/daemontools-0.76 || exit 1
mv src/conf-cc original-src-conf-cc || exit 1
sed 's/write-strings/write-strings -include \\/usr\\/include\\/errno.h/' original-src-conf-cc > src/conf-cc || exit 1
cp -p /etc/inittab /etc/inittab.bak || exit 1
package/install || exit 1
mv /etc/inittab.bak /etc/inittab || exit 1
cd /etc/init
echo 'start on runlevel [345]' > svscan.conf || exit 1
echo 'respawn' >> svscan.conf || exit 1
echo 'exec /command/svscanboot' >> svscan.conf || exit 1
initctl reload-configuration || exit 1
initctl start svscan || exit 1
mkdir -p /var/service || exit 1
")

(defn- generate-supervise-run-script [cmd user]
  (let [c (if user (str "setuidgid " user " " cmd) cmd)]
    (format"#!/bin/sh\n\nexec %s\n" c)))

(defn supervise-deployed? [m]
  (try (command m "ls /etc/init/svscan.conf" :capture :string) true (catch Exception e false)))

(defn deploy-supervise
  "deploy's the daemontools package to the target machine. No options on where it goes :-)"
  [m & {:keys [capture] :or {capture "supervise-install.log"}}]
  (if (not (supervise-deployed? m))
    (let [fname "./install-daemontools.sh"
          conf (generate-supervise-install-script)]
      (command m "sudo yum install -y make" :capture capture)
      (command m "sudo yum install -y gcc-c++" :capture capture)
      (write-remote-file m conf fname :mode 0755)
      (command m (format "sudo %s" fname :capture capture))))
  m)

(defn supervise
  "Create a supervision directory in /var/service, and generate the run script to call the given command, with optional user to run as. Call start-supervising to actually start it up"
  [m name cmd & {:keys [user capture] :or {capture "supervise-install.log"}}]
  (command m (format "sudo mkdir -p /var/service/%s/supervise" name) :capture capture)
  (command m (format "sudo chmod 1755 /var/service/%s" name) :capture capture)
  (let [tmpfile (format "/tmp/supervise-%s" (int (rand 1000)))]
    (write-remote-file m (generate-supervise-run-script cmd user) tmpfile)
    (command m (format "sudo cp %s /var/service/%s/run"  tmpfile name))
    (command m (format "sudo chmod 700 /var/service/%s/run" name))
    (command m (format "sudo ln -s /var/service/%s /service/%s" name name) :capture capture)
    m))
