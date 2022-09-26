# jepsen-tigerbeetle

### A [Jepsen](https://github.com/jepsen-io/jepsen) Test for [TigerBeetle](https://github.com/tigerbeetledb/tigerbeetle).

## Why TigerBeetle?

- sound grounding in research
- test culture
- devs are helpful in the chat

## Why Jepsen?

TigerBeetle does similar testing during development, so why try Jepsen?

Jepsen is good:

- real running clusters
- real environmental faults
- existing tests and models that evolved by finding bugs and increasing understanding time and time again

My personal belief is that Jepsen's the map is not the territory and highly adapted property generators will find a bug. Even in a meaningfully tested system like TigerBeetle. I also think that it will be necessary to enhance the existing bank test to be most fruitful. Let's see what we can learn...

----

## Developing the Jepsen Test

Jepsen has a [tutorial](https://github.com/jepsen-io/jepsen/blob/main/doc/tutorial/index.md).

We will be following the same steps as the tutorial but using TigerBeetle vs etcd.

----

## Current Status

And now that we have a client, running a simple test of `:read`s and `:transfer`s:

```clj
:invoke	:transfer	{:from 2, :to 1, :amount 3}
:ok	:transfer	{:from 2, :to 1, :amount 3}
:invoke	:transfer	{:from 1, :to 2, :amount 2}
:ok	:transfer	{:from 1, :to 2, :amount 2}
:invoke	:read	nil
:ok	:read	{1 -3, 2 3}
```

### We found our first bug!

```
Jepsen starting  /root/tigerbeetle/tigerbeetle start --addresses=192.168.122.101:3000 0_0.tigerbeetle
info(io): opening "0_0.tigerbeetle"...
info(main): 0: cluster=0: listening on 192.168.122.101:3000
info(message_bus): connection from client 317247975010753124847524919045321411137
error(storage): short read: buffer.len=706953216 offset=65536 bytes_read=0
thread 840 panic: data file inode size was truncated or corrupted
```

Doing a sequence of transactions with the Java client will crash the database.

Opened issue: [Multiple operations, ~10, using the Java client crash the server.](https://github.com/tigerbeetledb/tigerbeetle-java/issues/9)

PR to demonstrate bug in integration tests: [Enhance integration tests to do multiple account create/lookups to crash the server.](https://github.com/tigerbeetledb/tigerbeetle-java/pull/10)

----

## Change Log

(Follows tutorial. Reverse chronological order.)

### [Writing a Client](https://github.com/jepsen-io/jepsen/blob/main/doc/tutorial/03-client.md).

Using the [tigerbeetle-java](https://github.com/tigerbeetledb/tigerbeetle-java) client, a Jepsen [bank client](https://github.com/nurturenature/jepsen-tigerbeetle/blob/main/src/tigerbeetle/bank.clj) was created.

The client is used to create the accounts at database setup.

It can `:read` accounts and `:transfer` amounts between accounts.

The `checker` insures:
  - all totals match
  - `:negative-balances?` is respected
  - stats and plots 

----
### [Database Automation](https://github.com/jepsen-io/jepsen/blob/main/doc/tutorial/02-db.md)

Jepsen's DB protocol was [implemented for TigerBeetle](https://github.com/nurturenature/jepsen-tigerbeetle/blob/main/src/tigerbeetle/db.clj).

- can install/remove, start/kill, pause/resume TigerBeetle
  - individual node or cluster 
- snarfs log files from each node

----

### The Jepsen [test scaffolding](https://github.com/jepsen-io/jepsen/blob/main/doc/tutorial/01-scaffolding.md) was setup in [PR](https://github.com/nurturenature/jepsen-tigerbeetle/commit/44f5ca213aed97b0add265e3e07b42b84e74d28d)

We'll be using:

- latest Jepsen `0.2.8-SNAPSHOT`
- existing [bank test](https://jepsen-io.github.io/jepsen/jepsen.tests.bank.html)

At this stage the test:

-  generates operations to transfer amounts between accounts and read values
-  logs the operations vs executing them
-  uses the `unbridled-optimism` model/checker
```
lein run test
```
```clj
:invoke	:transfer	{:from 4, :to 6, :amount 1}
:invoke	:read	nil
...
{:valid? true,
 :count 614,
 :ok-count 614,
 ...}
```
```
 Everything looks good! ヽ(‘ー`)ノ
 ```