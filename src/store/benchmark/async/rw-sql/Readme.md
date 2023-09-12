For testing Peloton.


First just make it toy query esque, where we just issue a few reads writes to test.

Then make it such that it issues Read Modify Writes:

Each read-modify write picks the number of keys to read (select lower and upper, that are k apart)
I.e. just issue Update statements, each update where  lower < x < upper 
Keep a large table of discrete integers. Each Tx 


Table:
- one table. K-V store. Key = integers from [0, size]
- Could add extra knob that controls number of tables and which to access

Two knobs: 
- pick lower randomly (control skew)
- pick k randomly (control skew)

-- bonus: control number of Update statements.
-- create some read only statements.
-- create some insert only statements.

Serverside:
-- add Loading:
-- can implement a naive version that just Creates Table + does a bunch of inserts


//TODO: Create parallel version? This would need to use the SyncClient Wait interface