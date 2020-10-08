(ns practitioner-test
  (:require [practitioner :as sut]
            [clojure.test :refer [deftest]]
            [clj-yaml.core]
            [clojure.string :as str]))


(deftest test-practitioner

  (spit "/tmp/res.yaml"
        (clj-yaml.core/generate-string
         (->>
          (str/split (slurp "data/pract.csv") #"\n")
          (mapv sut/map-pract))))
  



  )
