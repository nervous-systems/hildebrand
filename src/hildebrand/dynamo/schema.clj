(ns hildebrand.dynamo.schema
  (:require [clojure.walk :as walk]
            [glossop :refer :all :exclude [fn-> fn->>]]
            [hildebrand.dynamo.util :refer :all]
            [miner.herbert :as h]
            [miner.herbert.generators :as hg]
            [plumbing.core :refer :all]
            [plumbing.map]))

(def attr-type #{:S :N :M :SS :NS :BS :L :B :BOOL :NULL})
(def consumed-capacity #{:indexes :total :none})
(def return-values #{:none :all-old :updated-old :all-new :updated-new})

(def common-elements
  '{:table-name (str ".+")
    :attr-def   {:attribute-name (str ".+")
                 :attribute-type (pred hildebrand.dynamo.schema/attr-type)}
    :return-cc (pred hildebrand.dynamo.schema/consumed-capacity)
    :return-values (pred hildebrand.dynamo.schema/return-values)
    :attr-value (& (:= M {(pred hildebrand.dynamo.schema/attr-type)
                          (or (str ".+")
                              [M]
                              [(str+ ".+")]
                              {(str+ ".+") M})})
                   (when (= 1 (count M))))
    :item {(str+ ".+") :interp/attr-value}
    :provisioned-throughput {:read-capacity-units int :write-capacity-units int}})

(defn interpolate-schema [s elements]
  (walk/prewalk
   (fn [x]
     (cond
       (map? x)
       (let [{:keys [interp/keys*]} x
             m  (into (dissoc x :interp/keys*)
                  (for [k keys*]
                    [k :interp/key*]))]
         (for-map [[k v] m]
           k (if (= v :interp/key*)
               (keyword "interp" (name k))
               v)))
       (and (keyword? x) (= "interp" (namespace x)))
       (-> x name keyword elements)
       :else x))
   s))

(def CreateTable*
  (interpolate-schema
   '{:attribute-definitions
     [(+ :interp/attr-def)]
     :key-schema [(+ {:attribute-name (str ".+")
                      :key-type (or :hash :range)})]
     :interp/keys* #{:table-name
                     :provisioned-throughput}}
   common-elements))

(defn conforming [schema m]
  (if (h/conforms? schema m)
    m
    (throw (Exception. (str "Invalid input" (pr-str m))))))

(def DeleteTable*
  '{:table-name (str ".+")})

(def DescribeTable*
  '{:table-name (str ".+")})

(def PutItem*
  (interpolate-schema
   '{:interp/keys* #{:table-name :item}
     (* :return-consumed-capacity) :interp/return-cc
     (* :return-values) :interp/return-values}
   common-elements))

(def DeleteItem*
  (interpolate-schema
   '{:key {(str+ ".+") :interp/attr-value}
     (* :return-consumed-capacity) :interp/return-cc
     :interp/keys* #{:table-name}}
   common-elements))

(def GetItem*
  (interpolate-schema
   '{:key {(str+ ".+") :interp/attr-value}
     (* :consistent-read) bool
     :interp/keys* #{:table-name}}
   common-elements))
