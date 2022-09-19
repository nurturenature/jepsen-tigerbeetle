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

Will be back after setting up the [test scaffolding](https://github.com/jepsen-io/jepsen/blob/main/doc/tutorial/01-scaffolding.md)...