# Custom Engine for Pequin

This houses the custom engine for executing queries with Pequin.

## Usage

```
make
./parser
```


## Motivation
- Using the previous Peloton query engine (located in `../query-engine`) required significant editing for our purposes
- TODO: fill in and add goals

## High-level description

We follow [Postgres's path of a query](https://www.postgresql.org/docs/current/query-path.html).

The parser will check the user-inputted query for proper syntax and creates a *query tree*.

The planner/optimizer will take the *query tree* and creates a *query plan* which is the input to the executor. We will implement a proper optimizer in later revisions.
