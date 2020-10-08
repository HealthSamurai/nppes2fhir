(ns loader
  (:require [clojure.java.shell :as shell]))

(defn bash [cmd]
  (time (println ">>" (shell/sh "bash" "-c" cmd))))

(def ctx {:wdir "/Users/niquola/nppes2fhir/data"})

(defn load-nucc-taxonomy [ctx]
  (bash (format "curl https://nucc.org/images/stories/CSV/nucc_taxonomy_201.csv > %s/nucc_taxonomy.csv" (:wdir ctx))))

(defn load-nppes [ctx]
  (bash (format "curl https://download.cms.gov/nppes/NPPES_Data_Dissemination_September_2020.zip > %s/nppes.zip" (:wdir ctx)))
  (bash (format "cd %s && unzip nppes.zip" (:wdir ctx)))
  (bash (format "cd %s && rm nppes.zip" (:wdir ctx))))

(comment
  (load-nucc-taxonomy ctx)

  (load-nppes ctx)


  )

