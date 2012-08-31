(defproject boynton/clj-util "0.0.1"
  :description "Some simple clojure utilities"
  :url "https://github.com/boynton/clj-util"
  :aot :all
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [com.fasterxml.uuid/java-uuid-generator "3.1.3"]
                 [clj-json "0.5.1"]
                 [org.clojure/java.jdbc "0.2.3"]
                 [com.h2database/h2 "1.3.168"]
                 [sqlitejdbc "0.5.6"]
                 [com.amazonaws/aws-java-sdk "1.3.14"]
                 [net.schmizz/sshj "0.8.1"]
                 [org.bouncycastle/bcprov-jdk16 "1.46"]
                 [org.slf4j/slf4j-nop "1.6.4"]
                 ])
