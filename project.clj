(defproject com.cemerick/s3-photo-archiver "1.0.0-SNAPSHOT"
  :min-lein-version "2.0.0"
  :dependencies [[com.amazonaws/aws-java-sdk "1.10.20"]
                 [commons-codec "1.8"]
                 [org.clojure/clojure "1.7.0" :scope "test"]]
  :source-paths ["src/main/java"]
  :java-source-paths ["src/main/java"]
  :main com.cemerick.s3photo.Archiver
  :prep-tasks ["javac" "compile"]
  :profiles {:uberjar {:uberjar-name "s3-photo-archiver.jar"}})
