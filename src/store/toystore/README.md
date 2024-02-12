# Custom Engine for Pequinstore

This houses the custom engine for executing queries with Pequin. It is separated into a store 

## Usage
TODO

## Motivation
The previous engine being used was the Peloton query engine (`src/store/pequinstore/query-engine`), which was hard to modify to suit our purposes.

## High-level description

<!-- OLD:

We follow [Postgres's path of a query](https://www.postgresql.org/docs/current/query-path.html).

The parser will check the user-inputted query for proper syntax and creates a *query tree*.

The planner/optimizer will take the *query tree* and creates a *query plan* which is the input to the executor. We will implement a proper optimizer in later revisions. -->
