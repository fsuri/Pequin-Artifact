## SQL-SMR Usage Guide <a name="sqlsmr"></a>

### Abstract

The SQL-SMR (SQL state machine replication) project was designed as a way of benchmarking our current system against an idealized state machine replication system with a sql backend. This project is at a state where tests are able to be run, however, there is a lot of development code present in order to properly test this in a local environment. The datastore being used for this is called hotstuffpgstore.

### Design

The design of this system follows the standard path of a client to a shard client, which then sends requests to replicas that perform state machine replication before sending their information down to the server level. After the server is done executing, it returns the results through the replica to the shardclient which consolidates the results it receives before sending them up to the client. Most of this design is well known, so I want to take this space to go over where the SQL-SMR system differs from other stores.

#### HotStuff SMR

The first thing I want to cover is the use of HotStuff SMR. The Pequin project has used HotStuff in the past, so its usage is not altogether new or intersting. It should be noted, however, that this new datastore links to hotstuff as used by the original hotstuffstore so that it is unnecessary to have two identical versions of HotStuff built on the system. This is all well and good, but in development some slight changes to HotStuff were made that should be addressed. Namely, development was done on a local system, so HotStuff had to be configured to read from local configs, this code is located at `src/store/hotstuffstore/libhotstuff/examples/indicus_interface.h`. Other than that, there is debugging code in `src/store/hotstuffstore/libhotstuff/examples/hotstuff_app.cpp` so as to better trace execution, but it is unnecessary to the functionality of the program.

#### Client

This client is fashioned after the hotstuffstore client and doesn't have many differences. The client does have a new parameter, `deterministic`. This parameter is used to indicate if the backend has deterministic state. For the client, this is simply a pass through parameter that is actually utilised by the shard client. The other thing of note with the client are the Commit and Abort functions. There is old, commented out code for these functions still in client. This can likely be deleted, but for now it is held in comments in the case it is needed in the future. The other notable difference is the `Query` function. This is a basic function designed to pass a query function down to the shard client layer and provide a query result wrapper on the returned value.

#### Shard Client

The shard client is also designed after the `hotstuffstore` shard client. It differs more, however, than the client did. THis file does have old commit and abort code that has been commented out for the same reasons as the client's code. The old code is not so important as the way I've elected to manage Querys, Commits, and Aborts in this code. All of these functions first define the type that they are intended to pass down (Query -> Inquiry [Shir: SQL_RPC], Commit -> Apply [Shir: TryCommit], Abort -> Rollback). After this, they simply go ahead and package the message as a request. This is so that it can easily be passed through the state machine replication layer, regardless of the type of command. Currently the query and commit code both have commented code where each request replicated 5 times. This is a product of local testing and can be removed in actual testing such that only one request of each type gets passed down to the replica layer. (HotStuff requires a certain amount of traffic to function).
The other code that has been added to the shard client is message handlers for the responses to queries(inquiries), commits(applies), and aborts(rollbacks). These all take the form `Handle______Reply`, and are designed after the `HandleTransactionDecision` function. I'll start by explaining the inquiry reply, this code has two different ways to function depending on if the backend is deterministic or non-deterministic. As with client, there is a parameter to the shard client that determines this that is called `deterministic`. If it is deterministic, this code collects replies and returns a reply if it receives f + 1 replies that have the same return value, or fails if it gets f + 1 failures. On the other hand, if the backend is non-deterministic, then the code is designed to collect f + 1 responses including the leader replica (in this case replica 0). As such, the determinism flag is only functional if messages are signed (otherwise the shard client cannot tell who the lead replica is). This is done so that even with a non-deterministic backend, results are consistent with a single view (the lead replica's view). The apply handler works in the same way when returning results.

#### Replica

The replica code isn't changed much at all as every message is passed through it as a request, negating the need for any specialization of a query request. The one thing that is added is an `asyncServer` flag. This is meant to indicate if the server requests are supposed to be asynchronous or not. In the executeSlots_internal function, there are two different versions of code to actually pose these requests, one of them using a callback function and a separate thread, making it asynchronous. My warning with this is that due to time constraints, the asynchronous version has not been thoroughly tested.

#### Server

The server code is where the main body of code lives. The execute function has been bolstered now to include the ability to handle queries(inquiries), commits(applies), and aborts(rollbacks). This all relies on the database. As it is currently set up, one needs to have the database running prior to attempting to run the server code (though it should be relatively straightforward to link some code to server start up that could start up the database as well). The constructor's `connection_str` variable defines the postgres server and database to connect to (I currently have it set to connect to my local machine on a specific port with it dynamnically choosing a database based on it's id). Each server has its own database. Once it's connected, a connection pool is created that future transactions take from.
The main body of code being used with the server is the `HandleInquiry` function. This is called from the execute function and its logic is as follows: It first looks to see if there is a transaction in process for that client using the `client_id` and `txn_seq_num`. If so, it takes that transaction and applies the query through the transaction's associated connection. If no such transaction exists, then it creates a new transaction, pulling a new connection from the connection pool. Afterwards, it gets the result of the execution which is parsed into a `QueryResultProtoBuilder` object, before being serialized to be passed back up to the client along with any success or failure messages.
The `HandleApply` function handles the commits to the system. It has a similar flow to queries, choosing transactions based on the `client_id` and `txn_seq_num`. If it finds it, then it commits the transaction. If not, currently it starts up a new transaction in the system that it immediately commits. This is a redundancy in the system that probably doesn't need to exist.
The `HandleRollback` function is almost the same as apply except it just aborts the transaction. It should be noted that if no such transaction exists, it does not create a new transaction.
The final important thing with the server code is that I have written a new function, `execute_callback``. This is similar to execute, however, it takes a callback function as a parameter, allowing for an asynchronous execution of a server request.

#### Protobufs

There were a few protobufs added to this code to allow for the desired functionality (all in server-proto):
- `Inquiry` - Contains its request id and query, as well as the client id and txn seq num to manage the transaction chosen.
- `Apply` - Only contains its request id alongside client id and txn seq num (no extra information is required to commit something).
- `Rollback` - Only has the client id and txn seq num so it knows what to abort.
- `InquiryReply` - Contains the aforementioned request id, the status of the execution, and the serialized result.
- `ApplyReply` - Contains the aforementioned request id, as well as the status of the execution.

#### Postgres Database

The postgres database is the final part of this project. Once you have postgres on your machine, your going to want to find an easy way to start and stop the server. Here's the code I use:
- Start server: `su - postgres -c "export PATH=$PATH:/usr/lib/postgresql/12/bin/; pg_ctl -D /var/lib/postgresql/datadb1 -o \"-p 5433\" -l logfiledb1 start"`
- Stop server: `su - postgres -c "export PATH=$PATH:/usr/lib/postgresql/12/bin/; pg_ctl -D /var/lib/postgresql/datadb1 stop"`

Other than that, it's nice to be able to connect to the database manually to set up for any tests or otherwise, however, connecting to that database depends on the hardware being used. As such, I would encourage referencing online sources for more in-depth details on using postgres.

The final facet of the postgres database is running it on a mounted file system in order to get it to run similar to how an in-memory key value store would. To set your postgres database up like that, the following article is helpful: https://blog.vergiss-blackjack.de/2011/02/run-postgresql-in-a-ram-disk/. As I have been working in a docker container, the following article was also helpful as it allowed me to set up the tmpfs easily in docker: https://stackoverflow.com/questions/42226418/how-to-move-postresql-to-ram-disk-in-docker. I have elected to just link the articles here because they provide some nice context for the process and drawbacks. Also regerence this code: https://gist.github.com/johaness/5081009. It's used in creating the ramdisk with `pg_createcluster`.

There are 3 scripts in `Pequin-Artifact/pg_setup`. `server_starter` will start up a totally new server, `dropper` will drop that server. `postgres_install` may need to be run prior to the server starter as `initdb` may not be on the system. This is easy to chain before `server_starter` to ensure that everything is installed properly. (the install postgres script adds `ssl-cert` to group so that postgres can be installed before removing that line. It prevents dpkg statoverride errors.)

#### Running Tests

I have been running tests using the `toy_client`. To do this, navigate to `src/store/benchmark/async/toy/toy_client.cc` and write out what queries you want executed and what you expect to receive from them. Afterwards, run the server tester and client testers to see if you are getting expected results. Currently the configurations are set up for my local machine, configurations are specified in the `server-tester.sh` and `client-tester.sh` files.

#### Known Issues

When running with replicated queries (sending 5 instead of 1) after a commit, a new transaction is not executed because the count for the pending executions gets incremented further than it should. This should not be an issue with non-replicated commands as the sequence number will get incremented the proper amount.

The other things that I should mention is that the `src/lib/threadpool.cc` code on this branch is slightly different than the normal code. Do to hardware limitations of my local machine, this was necessary to allow for execution of the code.


#### Shir:
1. initialize the postgres server with: `su - postgres -c "export PATH=$PATH:/usr/lib/postgresql/12/bin/; pg_ctlcluster 12 main start"`
2. order of things: thhe following is something you do one time to configure your enviorment (i) install postgres (`postgres_install`) (ii) `server starter` 
 then you do 1




#### Postgres information:
1. `pg_lsclusters -h` provides information on existing clusters and their status
2. `posgres_service.sh` is the script that handles installation and creating\removing clusters.
    use it as is in order to create the necceary cluster (it will also install postgres, if not already installed).
    Use it with flag `-u` to uninstall postgres completely.
    Use it with flag `-r` to drop the created cluster and all of its data. This action is unrecoverable.
    Use it with flag `-n <db_num>` to create db_num databases on the cluster. The default number without using this option is 1. When running locally, you'll want to create a unique db for each server. When running remotly, all servers can access a database with the same name and this option is not necessary.
3. `server.cc` now starts the cluster upon construction of a Server, and turns it off when destructing. For now, the name of the cluster and the postgres version is hardcoded in that file.

Helpful info regarding mounting :
1. "df" command shows the list of mounted devices
2. Use `umount /path/to/mounted_data` in order to unmount. I'm not sure why but sometimes you would have to do it more than once.
3. Folders can be removed only after they are unmounted.


Clarify next:
3. change #define LOCAL_CONFIG_DIR "/home/sc3348/Pesto/Pequin-Artifact/src/scripts/config/" in '/home/sc3348/Pesto/Pequin-Artifact/src/store/hotstuffstore/libhotstuff/examples/local_config_dir.h'
4. pipelined hotstuff
5. code is run from src folder
