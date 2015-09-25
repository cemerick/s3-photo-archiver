(ns com.cemerick.s3photo.Archiver)

; here only for easy `lein run` usage, when convenient
(defn -main [& args]
  (.. com.cemerick.s3photo.Archiver (main (into-array String args))))
