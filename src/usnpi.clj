(ns usnpi
  (:require [cheshire.core :as json]
            [clojure.string :as str]
            [cheshire.core :as json]
            [clj-yaml.core :as yaml]
            [clojure.java.io :as io])
  (:import
   [java.io PrintWriter InputStreamReader ByteArrayInputStream ByteArrayOutputStream]
   [java.util.zip GZIPInputStream GZIPOutputStream]))

(set! *warn-on-reflection* true)

(def nucc-idx
  (with-open [rdr (io/reader "data/nucc_taxonomy_191.csv")]
    (let [ls (line-seq rdr)]
      (reduce (fn [acc x]
                (let [data (->> (str/split x #",")
                                (mapv #(str/replace % #"(^\"|\"$)" "")))]
                  (assoc acc (nth data 0)
                         (let [v1 (nth data 3)
                               v2 (nth data 2)]
                           (if (str/blank? v1) v2 v1)))))
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
          (cond
            (= tp "concat")
            (let [trs   (->> (:items dsl) (mapv *compile-mapping))]
              (fn [gget]
                (reduce
                 (fn [res tr]
                   (let [v (tr gget)]
                     (cond
                       (sequential? v) (into res v)
                       v (conj res v)
                       :else res)))
                 [] trs)))

            (= tp "map")
            (if-let [idx (or (:map dsl)
                             (when-let [r (:ref dsl)]
                               (if (= r "nucc")
                                 nucc-idx
                                 (assert false (pr-str dsl)))))]
              (fn [gget]
                (when-let [v (gget (str/replace (:el dsl) #"^\." ""))]
                  (let [vv (or (get idx (keyword v)) (get idx v))
                        res (if (and (string? vv) (str/starts-with? vv "."))
                              (gget (subs vv 1))
                              vv)]
                    (if (and res (not (str/blank? res)))
                      res
                      (println "Missed nucc code" v))))))

            (= tp "each")
            (let [r    (:range dsl)
                  expr (:each dsl)
                  trs (doall (mapv (fn [i] (*compile-mapping (expand-i expr i))) (range 1 r)))]
              (assert expr ":each required for each")
              (assert r ":range required for each")
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

(defn load-mapping [file-name]
  (compile-mapping
   (clj-yaml.core/parse-string (slurp file-name))))


(def pr-mapping (load-mapping "practitioner.map.yaml"))
(def org-mapping (load-mapping "organization.map.yaml"))

(defn to-practitioner [hdx row]
  (pr-mapping hdx row))

(defn to-organization [hdx row]
  (org-mapping hdx row))

(defn parse-line [x]
  (let [is (str/split x #"\",\"")]
    (-> is
        (update 0 #(str/replace % #"^\"" ""))
        (update (dec (count is)) #(str/replace % #"\"$" "")))))


(defn gz-file-writer [file-name]
  (let [^java.io.OutputStream out (clojure.java.io/output-stream file-name)
        ^GZIPOutputStream gzip   (GZIPOutputStream. out)
        ^PrintWriter w (PrintWriter. gzip)]
    w))

(defn read-line-seq [file-name f]
  (with-open [rdr (io/reader file-name)]
    (f (line-seq rdr))))

(defn header-index [l]
  (->> (parse-line l)
       (mapv (fn [x] (->
                      (str/lower-case x)
                      (str/replace  #"[^a-z0-9]+" "_")
                      (str/replace #"_+$" ""))))
       (map-indexed (fn [i x] [i x]))
       (reduce (fn [acc [i x]]
                 (assoc acc x i)) {})))

(defn dump-samples [data-file dump-file]
  (time
   (with-open [w (io/writer dump-file)]
     (read-line-seq data-file
                    (fn [ls]
                      (let [headers-idx (header-index (first ls))]
                        (doseq [l (rest ls)]
                          (let [data (parse-line l)
                                item (if (practitioner? data)
                                       (to-practitioner headers-idx data)
                                       (to-organization headers-idx data))]
                            (.write w ^String (clj-yaml.core/generate-string item))
                            (.write w "\n---\n")))))))))

(defn practitioner? [data]
  (= "1" (second data)))

(defn dump [page-size & [num-pages]]
  (time
   (read-line-seq "data/npidata_pfile_20050523-20190609.csv"
                  (fn [ls]
                    (let [headers-idx (header-index (first ls))]
                      (loop [cnt 0
                             pg-cnt 1
                             ^java.io.Writer w (gz-file-writer "result/practitioners-0000001.ndjson.gz")
                             [l & ls] (rest ls)]
                        (if (and num-pages (> pg-cnt num-pages))
                          (do (.close w)
                              (println "Done."))
                          (let [[pg-cnt cnt ^java.io.Writer w] (if (= cnt page-size)
                                                                 (do (.close w)
                                                                     (println "Page: " pg-cnt)
                                                                     [(inc pg-cnt) 0 (gz-file-writer (format "result/practitioners-%07d.ndjson.gz" (inc pg-cnt)))])
                                                                 [pg-cnt cnt w])]
                            (let [data (parse-line l)]
                              (if (practitioner? data)
                                (do
                                  (.write w ^String (cheshire.core/generate-string (to-practitioner headers-idx data)))
                                  (.write w "\n")
                                  (recur (inc cnt) pg-cnt w ls))
                                (recur cnt pg-cnt w ls)))))))))))


(comment
  (dump 100 1)

  (dump)
  )

(dump-samples "data/datasample.csv" "sample.yaml")




