# ApacheSpark
Apache Spark project for Database II course 





<h5 style="text-align:center">Μέρος 1ο : Υπολογισμός Αναλυτικών Ερωτημάτων με τα APIs του Apache Spark</h5>

---





- [ ] *Ζητούμενο 1:* Φόρτωση αρχείων csv στο hdfs

- [ ] *Ζητούμενο 2:* Μετατροπή αρχείων csv σε parquet για επεξεργασία (οδηγίες [εδώ](https://parquet.apache.org/) και [εδώ](https://spark.apache.org/docs/2.4.4/sql-programming-guide.html#parquet-files)). Μετα από αυτό θελουμε 6 αρχεία στο hdfs (3 csv, 3 parquet)

- [ ] *Ζητούμενο 3:* Για τα Q1-Q5 υλοποίηση λύσης: μια με `RDD API` και μια με `SPARK SQL` 

  - [ ] | Queries | RDD  | Spark SQL |
    | :-----: | :--: | :-------: |
    |   Q1    | [ ]  |    [ ]    |
    |   Q2    | [ ]  |    [ ]    |
    |   Q3    | [ ]  |    [ ]    |
    |   Q4    | [ ]  |    [ ]    |
    |   Q5    | [ ]  |    [ ]    |

- [ ] *Ζητούμενο 4:* Εκτέλεση των παραπάνω queries:

  - [ ] Map Reduce Queries – RDD API
  - [ ] Spark SQL με είσοδο το csv αρχείο (συμπεριλάβετε infer schema)
  - [ ] Spark SQL με είσοδο το parquet αρχείο







<h5 style="text-align:center">Μέρος 2ο : Υλοποίηση και μελέτη συνένωσης σε ερωτήματα και Μελέτη του βελτιστοποιητή του Spark</h5>

---





- [ ] *Ζητούμενο 1:* Υλοποιείστε το broadcast join στο RDD API (Map Reduce)
- [ ] *Ζητούμενο 2:* Υλοποιείστε το repartition join στο RDD API (Map Reduce)
- [ ] *Ζητούμενο 3:* Απομονώστε 100 γραμμές του πίνακα movie genres σε ένα άλλο CSV. Συγκρίνεται τους χρόνους εκτέλεσης των δύο υλοποιήσεων σας για την συνένωση των 100 γραμμών με τον πίνακα ratings και συγκρίνετε τα αποτελεσμάτων. Τι παρατηρείται? Γιατί?
- [ ] *Ζητούμενο 4:* Συμπλήρωση του κώδικα της αναφοράς στη σελίδα 6 για την απανεργοποίηση του join από τον βελτιστοποιητή 