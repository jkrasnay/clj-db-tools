(ns clj-db-tools.core
  (:require
    [clojure.pprint :refer [print-table]]
    [next.jdbc :as jdbc]
    [next.jdbc.result-set :as rs]))


(defn result-set-as-maps
  "Accepts a result set and returns a vector of maps, one for each result set
  row.
  "
  [rs]
  (rs/datafiable-result-set rs {:builder-fn rs/as-unqualified-kebab-maps}))


(defn columns
  "Returns a seq of maps, each representing a column in the given table.
  `opts` is a map that supports the following keys:

  :schema      | Name of the schema in which to look.
  "
  [db table-name opts]
  (jdbc/on-connection [c db]
   (-> c
      .getMetaData
      (.getColumns nil (:schema opts) table-name nil)
      result-set-as-maps)))


(defn schemas
  "Returns all schemas in the database.
  "
  [db]
  (jdbc/on-connection [c db]
    (map :table-schem (-> c
                          .getMetaData
                          .getSchemas
                          result-set-as-maps))))


(defn tables
  "Returns all tables visible from the given connection.
  `opts` is a map that supports the following keys:

  :pattern     | Table name pattern, using % as a wildcard.
  :schema      | Name of the schema in which to look.
  :table-types | Vector of table types of the tables to return.
  "
  [db opts]
  (jdbc/on-connection [c db]
    (let [{:keys [pattern
                  schema
                  table-types]} opts]
      (-> c
          .getMetaData
          (.getTables nil
                      schema
                      (or pattern "%")
                      (when table-types
                        (into-array table-types)))
          result-set-as-maps))))


(defn table-types
  "Returns all table types in the database.
  "
  [db]
  (jdbc/on-connection [c db]
   (map :table-type (-> c
                       .getMetaData
                       .getTableTypes
                       result-set-as-maps))))


;;------------------------------------------------------------
;; Description functions
;;
;; These functions print descriptions in tabular format to stdout.
;;

(defn describe-tables
  "Prints a table describing some database tables.
  `opts` is a map that supports the following keys:

  :pattern     | Table name pattern, using % as a wildcard.
  :schema      | Name of the schema in which to look.
  :table-types | Vector of table types of the tables to return.
  "
  [db opts]
  (->> (tables db opts)
       (print-table [:table-schem :table-name :table-type :remarks])))


(defn describe-table
  "Prints a table with the columns in the given table.
  `opts` is a map that supports the following keys:

  :schema      | Name of the schema in which to look.
  "
  [db table-name opts]
  (->> (columns db table-name opts)
       (map #(assoc % :null? (when (= "NO" (:is-nullable %)) "not null")))
       (print-table [:column-name :type-name :null? :remarks])))


(comment

  (require '[clojure.edn :as edn])

  (def db (-> (slurp "test-db.edn")
              edn/read-string
              jdbc/get-datasource))

  (jdbc/with-transaction [c db]
    (jdbc/execute! c ["select 'hi'"]))
  (schemas db)
  (table-types db)
  (describe-tables db {:schema "public"
                       :table-types ["TABLE"]})
  (describe-table db "event_audit_log" {:schema "public"})
  (describe-table db "widget" {:schema "public"})

  )
