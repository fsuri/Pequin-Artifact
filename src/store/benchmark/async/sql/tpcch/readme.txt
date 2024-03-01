The TPC-CH Benchmark is an add-on to the TPCC benchmark that includes several different read queries. 

To run the benchmark:

(1) Run the table generators in both TPCC and TPCCH 

(2) Run psql and create a database called "tpcch"

(3) In the "tpcch" database, copy the init.sql in src/store/benchmark/async/sql/tpcch/resources to create the table schema.

(4) Copy the generated data from the table generators into the tables in the tpcch database. 

(5) In src/pg-manyclient-tester.sh, change the USR_NAME variable to your computer's user name.

(6) Run pg-manyclient-tester.sh 