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

Now that we can bring up a TigerBeetle cluster, lets create a client for TigerBeetle.

[Writing a Client](https://github.com/jepsen-io/jepsen/blob/main/doc/tutorial/03-client.md) in the tutorial.

----

## Change Log

(Follows tutorial. Reverse chronological order.)

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