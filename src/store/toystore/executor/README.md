# Custom-engine executor

How to use:

(1) Create a QueryExecutor object in server.

(2) Call addTable whenever a table is added in server.

(3) Call 'insert' whenever loading a table or inserting new values. 'Write' object's 
Timestamp and Value types should be the same as the Timestamp and Value types used to 
create VersionedValueStore.

(4) Call 'scan' to get values from table whose primary key index is in range [low, high]
