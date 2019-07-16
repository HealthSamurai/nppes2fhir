(ns core
  (:require [cheshire.core :as json]
            [clojure.string :as str]
            [cheshire.core :as json]
            [clj-yaml.core :as yaml]
            [clojure.java.io :as io])
  (:import
   [java.io PrintWriter InputStreamReader ByteArrayInputStream ByteArrayOutputStream]
   [java.util.zip GZIPInputStream GZIPOutputStream]))

(set! *warn-on-reflection*)

(defonce nucc-idx
  (with-open [rdr (io/reader "data/nucc_taxonomy_191.csv")]
    (let [ls (line-seq rdr)]
      (reduce (fn [acc x]
                (let [data (str/split x #",")]
                  (assoc acc (nth data 0) (nth data 3))))
              {} (rest ls)))))

(defn expand-i [dsl i]
  (cond
    (map? dsl) (reduce (fn [acc [k v]] (assoc acc k (expand-i v i))) {} dsl)
    (sequential? dsl) (mapv #(expand-i % i) dsl)
    (and (string? dsl))
    (str/replace dsl #"\{i\}" (str i))
    :else dsl))

(defn *compile-mapping [dsl]
  (cond (map? dsl)
        (if-let [tp (:$type dsl)]
          (cond (= tp "map")
                (if-let [idx (or (:map dsl)
                                 (when-let [r (:ref dsl)]
                                   (if (= r "nucc")
                                     nucc-idx
                                     (assert false (pr-str dsl)))))]
                  (fn [gget]
                    (when-let [v (gget (str/replace (:el dsl) #"^\." ""))]
                      (let [vv (or (get idx (keyword v)) (get idx v))]
                        (if (and (string? vv) (str/starts-with? vv "."))
                          (gget (subs vv 1))
                          vv)))))

                (= tp "each")
                (let [r (:range dsl)
                      expr (:each dsl)
                      trs (doall (mapv (fn [i] (*compile-mapping (expand-i expr i))) (range 1 r)))]
                  (fn [gget]
                    (let [res (reduce (fn [res tr]
                                        (if-let [v (tr gget)]
                                          (conj res v)
                                          res))
                                      [] trs)]
                      (when-not (empty? res)
                        res))))
                :else (assert false dsl))
          (let [trs (doall (reduce (fn [res [k v]] (assoc res k (*compile-mapping v))) {} dsl))
                constants (reduce (fn [res [k v]]
                                    (if (and (string? v) (not (str/starts-with? v ".")))
                                      (conj res k)
                                      res))
                                  [] dsl)]
            (fn [gget]
              (let [res (reduce (fn [res [k tr]]
                                  (if-let [v (tr gget)]
                                    (assoc res k v)
                                    res))
                                {} trs)]
                (when-not (empty? (apply dissoc res constants))
                  res)))))

        (sequential? dsl)
        (let [trs (doall (mapv *compile-mapping dsl))]
          (fn [gget]
            (let [res (reduce (fn [res tr]
                                (if-let [v (tr gget)]
                                  (conj res v)
                                  res))
                              [] trs)]
              (when-not (empty? res)
                res))))

        (string? dsl)
        (if (str/starts-with? dsl ".")
          (fn [gget]
            (gget (subs dsl 1)))
          (constantly dsl))))

(defn compile-mapping [mapping]
  (let [m (*compile-mapping mapping)]
    (fn [hdx row]
      (let [f (fn [k]
                (let [v (get row (get hdx k))]
                  (when (and v (not (str/blank? v))) v)))]
        (m f)))))

(defn load-mapping []
  (clj-yaml.core/parse-string (slurp "mapping.yaml")))


(def pr-mapping (compile-mapping (load-mapping)))

(defn to-practitioner [hdx row]
  (pr-mapping hdx row))

(defn parse-line [x]
  (let [is (str/split x #"\",\"")]
    (-> is
        (update 0 #(str/replace % #"^\"" ""))
        (update (dec (count is)) #(str/replace % #"\"$" "")))))



(defn dump [& [limit]]
  (time
   (with-open [^java.io.OutputStream out (clojure.java.io/output-stream "practitioner.ndjson.gz")
               ^GZIPOutputStream gzip   (GZIPOutputStream. out)
               ^PrintWriter w (PrintWriter. gzip)]
     (with-open [rdr (io/reader "data/npidata_pfile_20050523-20190609.csv")]
       (let [ls (line-seq rdr)
             headers (mapv (fn [x] (str/replace (str/lower-case x) #"[^a-z0-9]+" "_"))
                           (parse-line (first ls)))
             headers-idx (->> headers
                              (map-indexed (fn [i x] [i x]))
                              (reduce (fn [acc [i x]]
                                        (assoc acc x i)) {}))]

         ;; (println (pr-str headers))
         ;; (println (pr-str headers-idx))

         ;; (println (str/join "\n" headers-idx))
         (doseq [x (if limit
                     (take limit (rest ls))
                     (rest ls))]
           (let [data (parse-line x)]
             (when (= "1" (second data))
               ;; (println (to-practitioner headers-idx data))
               ;; (.write w ^String (clj-yaml.core/generate-string (to-practitioner headers-idx data)))
               (.write w ^String (cheshire.core/generate-string (to-practitioner headers-idx data)))
               (.write w "\n"))))

         )))))


(comment 
  (dump 10)
  (dump)
  )





