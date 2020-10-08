(ns practitioner
  (:require [clojure.string :as str]
            [mappers.simple]
            [clojure.java.io :as io]))

(def nucc-idx
  (with-open [rdr (io/reader "data/nucc_taxonomy.csv")]
    (let [ls (line-seq rdr)]
      (reduce (fn [acc x]
                (let [data (->> (str/split x #",")
                                (mapv #(str/replace % #"(^\"|\"$)" "")))]
                  (assoc acc (nth data 0)
                         (let [v1 (nth data 3)
                               v2 (nth data 2)]
                           (if (str/blank? v1) v2 v1)))))
              {} (rest ls)))))

;; TODO: cleanup phones

(defn license-fn [license]
  (let [l (if-let [txt (get nucc-idx (:taxonomy license))]
            (assoc license :text txt)
            license)]
    (if (= "Y" (:primary l))
      (assoc l :primary true)
      (dissoc l :primary))))

(defn identifier-fn [identifier]
  (if (= "01" (:system identifier))
    (-> identifier (dissoc :issuer) (assoc :system (:issuer identifier)))
    (update identifier :system (fn [x] (get {"01" "OTHER"
                                            "02" "MEDICARE UPIN"
                                            "04" "MEDICARE ID-TYPE UNSPECIFIED"
                                            "05" "MEDICAID"
                                            "06" "MEDICARE OSCAR/CERTIFICATION"
                                            "07" "MEDICARE NSC"
                                            "08" "MEDICARE PIN"} x x)))))

(defn date-fmt [x]
  (when x
    (let [prts (str/split x #"/")]
      (str (last prts) "-" (first prts) "-" (second prts)))))

(def mapping
  {0 [:id]
   ;; "Entity Type Code"
   ;; 1 :ignore
   ;; "Replacement NPI"
   2 [:link {:type "replaced-by" :other {:resourceType "Practitioner"}} :other :id]
   ;; "Employer Identification Number (EIN)"
   3 [:identifier {:system "EIN"} :value]
   ;; "Provider Organization Name (Legal Business Name)"
   4 [:organization 0]
   ;; "Provider Last Name (Legal Name)"
   5 [:name 0 :family]
   ;; "Provider First Name"
   6 [:name 0 :given 0]
   ;; "Provider Middle Name"
   7 [:name 0 :given 1]
   ;; "Provider Name Prefix Text"
   8 [:name 0 :prefix 0]
   ;; "Provider Name Suffix Text"
   9 [:name 0 :suffix 0]
   ;; "Provider Credential Text"
   10 [:name 0 :prefix 1]
   ;; "Provider Other Organization Name"
   11 [:organization 1]
   ;; "Provider Other Organization Name Type Code"
   ;; 12 [:other :organization :type]
   ;; "Provider Other Last Name"
   13 [:name 1 :family]
   ;; "Provider Other First Name"
   14 [:name 1 :given 0]
   ;; "Provider Other Middle Name"
   15 [:name 1 :given 1]
   ;; "Provider Other Name Prefix Text"
   16 [:name 1 :prefix 0]
   ;; "Provider Other Name Suffix Text"
   17 [:name 1 :suffix 0]
   ;; "Provider Other Credential Text"
   18 [:name 1 :prefix 1]
   ;; "Provider Other Last Name Type Code"
   19 [:name 1 :_type]

   ;; "Provider First Line Business Mailing Address"
   20 [:address 0 :line 0]
   ;; "Provider Second Line Business Mailing Address"
   21 [:address 0 :line 1]
   ;; "Provider Business Mailing Address City Name"
   22 [:address 0 :city]
   ;; "Provider Business Mailing Address State Name"
   23 [:address 0  :state]
   ;; "Provider Business Mailing Address Postal Code"
   24 [:address 0  :postalCode]
   ;; "Provider Business Mailing Address Country Code (If outside U.S.)"
   25 [:address 0 :county]
   ;; "Provider Business Mailing Address Telephone Number"
   26 [:phone 0]
   ;; "Provider Business Mailing Address Fax Number"
   27 [:fax 0]

   ;; "Provider First Line Business Practice Location Address"
   28 [:address 1 :line 0]
   ;; "Provider Second Line Business Practice Location Address"
   29 [:address 1 :line 1]
   ;; "Provider Business Practice Location Address City Name"
   30 [:address 1 :city]
   ;; "Provider Business Practice Location Address State Name"
   31 [:address 1 :state]
   ;; "Provider Business Practice Location Address Postal Code"
   32 [:address 1 :postalCode]
   ;; "Provider Business Practice Location Address Country Code (If outside U.S.)"
   33 [:address 1 :county]
   ;; "Provider Business Practice Location Address Telephone Number"
   34 [:phone 1]
   ;; "Provider Business Practice Location Address Fax Number"
   35 [:fax 1]

   ;; "Provider Enumeration Date"
   36 [:dates :create {:$fmt date-fmt}]
   ;; "Last Update Date"
   37 [:dates :update {:$fmt date-fmt}]

   ;; "NPI Deactivation Reason Code"
   38 [:deactivation :reason]
   ;; "NPI Deactivation Date"
   39 [:dates :deactivation {:$fmt date-fmt}]
   ;; "NPI Reactivation Date"
   40 [:dates :reactivation {:$fmt date-fmt}]

   ;; "Provider Gender Code"
   41 [:gender {:$fmt (fn [x] (get {"M" "male" "F" "female"} x x))}]

   ;; "Authorized Official Last Name"
   42 [:authorized_official :family]
   ;; "Authorized Official First Name"
   43 [:authorized_official :given 0]
   ;; "Authorized Official Middle Name"
   44 [:authorized_official :given 1]
   ;; "Authorized Official Title or Position"
   45 [:authorized_official :title]
   ;; "Authorized Official Telephone Number"
   46 [:authorized_official :phone]

   ;; "Healthcare Provider Taxonomy Code_1"
   [47 4 15] {:key :license
              :mapping {0 [:taxonomy]
                        1 [:number]
                        2 [:state]
                        3 [:primary]}
              :post-proc license-fn}

   [107 4 50] {:key :identifier
               :mapping {0 [:value]
                         1 [:system]
                         2 [:state]
                         3 [:issuer]}
               :post-proc identifier-fn}

   ;; "Is Sole Proprietor"
   307 [:sole_proprietor]
   ;; "Is Organization Subpart"
   ;; 308 [:is_org_subpart]
   ;; "Parent Organization LBN"
   309 [:parent_org_lbn]
   ;; "Parent Organization TIN"
   310 [:parent_org_tin]

   ;; "Authorized Official Name Prefix Text"
   311 [:authorized_official? :name]
   ;; "Authorized Official Name Suffix Text"
   312 [:authorized_official? :suffix]
   ;; "Authorized Official Credential Text"
   313 [:authorized_official? :credential]

   ;; "Healthcare Provider Taxonomy Group_1"
   [314 1 15] {:key :taxonomy
               :mapping {0 [:group]}}

   ;; "Certification Date"
   330  [:certification_date]})



(defn parse-line [x]
  (->> 
   (str/split (str/replace x #"(^\"|\"$)" "") #"\",\"")
   (mapv (fn [x] (if (str/blank? x) nil x)))))

(defn map-pract [l]
  (let [parts (parse-line l)]
    (when (not (= "2" (second parts)))
      (let [pr (->> mapping
           (sort-by (fn [[k _]] (if (int? k) k (first k))))
           (reduce (fn [acc [n pth]]
                     (if (int? n)
                       (if-let [v (nth parts n nil)]
                         (mappers.simple/put-in acc pth v)
                         acc)
                       (let [[from page times] n
                             {key :key mapping' :mapping post-proc :post-proc} pth]
                         (loop [times times
                                from from
                                acc acc]
                           (if (= 0 times)
                             acc
                             (recur 
                              (dec times)
                              (+ from page)
                              (let [obj (->> mapping'
                                             (reduce
                                              (fn [obj [n' pth']]
                                                (if-let [v (nth parts (+ from n') nil)]
                                                  (mappers.simple/put-in obj pth' v)
                                                  obj)) {}))]
                                (if-not (empty? obj)
                                  (update acc key (fn [x] (conj (or x []) (if post-proc (post-proc obj) obj))))
                                  acc))))))))
                   {}))]

        (-> pr
            (update :address dedupe)
            (update :phone dedupe)
            (update :fax dedupe))))))
