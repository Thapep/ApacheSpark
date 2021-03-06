# ApacheSpark
Apache Spark project for Database II course 





<h5 style="text-align:center">Μέρος 1ο : Υπολογισμός Αναλυτικών Ερωτημάτων με τα APIs του Apache Spark</h5>

---





- [x] *Ζητούμενο 1:* Φόρτωση αρχείων csv στο hdfs

- [x] *Ζητούμενο 2:* Μετατροπή αρχείων csv σε parquet για επεξεργασία (οδηγίες [εδώ](https://parquet.apache.org/) και [εδώ](https://spark.apache.org/docs/2.4.4/sql-programming-guide.html#parquet-files)). Μετα από αυτό θελουμε 6 αρχεία στο hdfs (3 csv, 3 parquet)

- [ ] *Ζητούμενο 3:* Για τα Q1-Q5 υλοποίηση λύσης: μια με `RDD API` και μια με `Spark SQL` Το `RDD` θα γίνει πάνω στα csv ενώ το `Spark SQL` πάνω στα csv και στα parquet 

    | Queries | RDD  | Spark SQL |
    | :-----: | :--: | :-------: |
    |   Q1    | :heavy_check_mark:  |    [ ]    |
    |   Q2    | :heavy_check_mark:  |    [ ]    |
    |   Q3    | :heavy_check_mark:  |    [ ]    |
    |   Q4    | :heavy_check_mark:  |    [ ]    |
    |   Q5    | :heavy_check_mark:  |    [ ]    |

- [ ] *Ζητούμενο 4:* Εκτέλεση των παραπάνω queries:

  - [x] Map Reduce Queries – RDD API
  - [ ] Spark SQL με είσοδο το csv αρχείο (συμπεριλάβετε infer schema)
  - [ ] Spark SQL με είσοδο το parquet αρχείο
  - [ ] Δώστε τους χρόνους εκτέλεσης σε ένα ραβδόγραμμα, ομαδοποιημένους ανά Ερώτημα Σχολιάστε τα αποτελέσματα σε κάθε query. Τι παρατηρείται με τη χρήση του parquet ? Γιατί δεν χρησιμοποιείται το infer schema?






<h5 style="text-align:center">Μέρος 2ο : Υλοποίηση και μελέτη συνένωσης σε ερωτήματα και Μελέτη του βελτιστοποιητή του Spark</h5>

---





- [ ] *Ζητούμενο 1:* Υλοποιείστε το broadcast join στο RDD API (Map Reduce)
- [ ] *Ζητούμενο 2:* Υλοποιείστε το repartition join στο RDD API (Map Reduce)
- [ ] *Ζητούμενο 3:* Απομονώστε 100 γραμμές του πίνακα movie genres σε ένα άλλο CSV. Συγκρίνετε τους χρόνους εκτέλεσης των δύο υλοποιήσεων σας για την συνένωση των 100 γραμμών με τον πίνακα ratings και συγκρίνετε τα αποτελέσματα. Τι παρατηρείτε? Γιατί?
- [ ] *Ζητούμενο 4:* Συμπλήρωση του κώδικα της αναφοράς στη σελίδα 6 για την απενεργοποίηση του join από τον βελτιστοποιητή 
