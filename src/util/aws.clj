(ns util.aws)

(defn aws-credentials [& {:keys [access secret]
                          :or {access (get (System/getenv) "AWS_ACCESS_KEY") secret (get (System/getenv) "AWS_SECRET_KEY")}}]
  {:access access :secret secret :provider "aws"})


