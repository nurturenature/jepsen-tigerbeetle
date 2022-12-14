# `jepsen-tigerbeetle`

### A [Jepsen](https://github.com/jepsen-io/jepsen) Test for [TigerBeetle](https://github.com/tigerbeetledb/tigerbeetle).

## Why TigerBeetle?

- solidly grounded in research
- test culture
- devs are helpful in the chat

## Why Jepsen?

TigerBeetle is [meaningfully tested](https://github.com/tigerbeetledb/viewstamped-replication-made-famous) during development, so why try Jepsen?

Jepsen is [good](http://jepsen.io/analyses):

- real running clusters
- real environmental faults
- existing tests and models evolved by finding bugs and increasing understanding time and time again

The thesis is that Jepsen's [the map is not the territory](https://en.wikipedia.org/wiki/Map%E2%80%93territory_relation) black-boxing combined with its highly adapted property generators will be useful in contributing to TigerBeetle's development, serving to experientially validate and learn from the existing tests. The extensive existing testing will also necessitate enhancing the existing bank test to be most fruitful. Let's see what we can learn...

----

## Developing the Jepsen Test

`jepsen-tigerbeetle`:

- builds
  - [tigerbeetledb/tigerbeetle](https://github.com/tigerbeetledb/tigerbeetle) 
  - [tigerbeetledb/tigerbeetle-java](https://github.com/tigerbeetledb/tigerbeetle-java) (Jepsen is Clojure so uses Java client)
- can install/remove, setup/teardown, and start/stop arbitrary TigerBeetle replicas
- capture stdout/stderr, logs, and data files

----

### Bank Test

Uses Jepsen's [bank test](https://jepsen-io.github.io/jepsen/jepsen.tests.bank.html).

```clj
; sample bank client operations

:invoke	:transfer	{:debit-acct 2, :credit-acct 1, :amount 3}
:ok	:transfer	{:debit-acct 2, :credit-acct 1, :amount 3}
:invoke	:transfer	{:debit-acct 1, :credit-acct 2, :amount 2}
:ok	:transfer	{:debit-acct 1, :credit-acct 2, :amount 2}
:invoke	:read	nil
:ok	:read	{1 -3, 2 3}
```

Jepsen's bank `checker` insures:
  - all totals match
  - `:negative-balances?` is respected
  - stats and plots
  - [snapshot isolation](http://jepsen.io/consistency/models/snapshot-isolation) level of consistency

----
  
### Account Creation/Lookup

Treats TigerBeetle's immutable accounts, their creation, lookup, and ledgers as a grow only set.

Uses Jepsen's [set-full](https://jepsen-io.github.io/jepsen/jepsen.checker.html#var-set-full) checker.

The grow only set generates random operations of:
```clj
; sample set-full client operations

:invoke	:add [ledger account]
:ok	:add [ledger account]
:invoke	:read [ledger nil]
:ok	:read [ledger #{account account' ...}]
```

TigerBeetle [is designed](https://tigerbeetle.com/index.html#home_safety) for [strict serializability](http://jepsen.io/consistency/models/strict-serializable) so `:linearizable? true` is set for the checker. 

----

## Faults

- Partitioning

- Process kill, pause, and resume

- Packet corruption

- File corruption

----

## Current Status

### Leader-Stalker Nemesis ????

Tests can target the leader for faults.
Leadership can be like that :-).

Leader is determined by examining the logs for leadership view changes.

```clj
[jepsen node n1] Leader view on  n1  :  12
[jepsen node n2] Leader view on  n2  :  10
[jepsen node n3] Leader view on  n3  :  11
:nemesis :info :start-partition [:isolated {"n1" #{"n2",  "n3"}, "n2" #{"n1"}, "n3" #{"n1"}}]
```

### Panics

Leader partitioning is finding a new, to us, panic:

```zig
ring_buffer.zig:137:38: 0x3327e8 in ring_buffer.RingBuffer(vsr.replica.Prepare,32,ring_buffer.enum:10:27.array).push_assume_capacity (tigerbeetle)
                error.NoSpaceLeft => unreachable,
                                     ^
```

### False Panics

When cluster loses quorum, can inappropriately panic, IOW:
  - \# client sessions <= client count * client max concurrent  
yet:

```
thread 91174 panic: session evicted: too many concurrent client sessions
```

### We'll check back after continuing to enhance the tests...

----

## Experience Log

### Long running no fault tests are now long running. ??? ????

Thundering hordes can come and go, kicking up some compaction on the way, and then they are all served and on their way.

![thundering hordes latency](./doc/images/thundering-hordes.png)

All tests run to completion w/o errors.
All reads/writes check consistent.

The experience prompted looking up the etymology of deterministic:

> Middle English, from Anglo-French determiner, from Latin determinare,
> 
> from de- + terminare to limit,
>  
> from terminus boundary, limit

----

### And these are known issues already identified by the LSM fuzzer. Tests were running against prior commits. ????

There are some panics and cluster unavailability that `jepsen-tigerbeetle` is finding that are new.

An issue has been opened, [random transactions (w/no environmental faults) can panic or cause the replica to become unavailable](https://github.com/tigerbeetledb/tigerbeetle/issues/215).


```
# no environmental faults
  assert(it.data_block_index == Table.index_data_blocks_used(it.index_block));
  assert(!compaction.data.writable);
  and replica unavailability with looping timeouts in the logs

# with partitioning
  assert(header.op <= self.op_checkpoint_trigger());
  assert(self.op >= self.commit_max);
```

### Confirming VOPR/LSM bugs

Now that we can run longer tests, let's run a test until it fails:

```
debug(tree): Transfer.id: compact_start: op=4295 snapshot=4293 beat=4/4
debug(tree): Transfer.id: compact_tick() for immutable table to level 0
thread 1361 panic: reached unreachable code
/root/tigerbeetle/zig/lib/std/debug.zig:225:14: 0x269bcb in std.debug.assert (tigerbeetle)
    if (!ok) unreachable; // assertion failure
             ^
/root/tigerbeetle/src/lsm/compaction.zig:450:19: 0x48b380 in lsm.compaction.CompactionType(lsm.table.TableType(u128,lsm.groove.IdTreeValue,lsm.groove.IdTreeValue.compare_keys,lsm.groove.IdTreeValue.key_from_value,554112867134706473364364839029663282043,lsm.groove.IdTreeValue.tombstone,lsm.groove.IdTreeValue.tombstone_from_key),storage.Storage,lsm.table_immutable.TableImmutableIteratorType).cpu_merge_start (tigerbeetle)
            assert(!compaction.data.writable);
```

And it turns out this was also found by TigerBeetle's LSM fuzzers: [lsm_forest_fuzz](https://github.com/tigerbeetledb/tigerbeetle/issues/183).

**How wonderful! ????**

Possible data-point (or just confirming my biases ????):

  - ??? VOPR(+LSM) is proactively finding bugs that would express in real applications
  - ??? Application fuzzing can meaningfully explore the database state through the client

It's a nice mutual affirmation!

And with the [bug fix](https://github.com/tigerbeetledb/tigerbeetle/pull/177) in `main`, our tests pass too!

----

### TigerBeetle is making good progress.

We can now run a straightforward relatively low rate test end-to-end.

### Here's one replica with one client and no faults:

![1x1 no-faults latency raw](doc/images/1x1-no-faults-latency-raw.png)

### Increasing to 3 replicas and 8 clients shows a clear latency wave pattern:

![3x8 no-faults latency raw](doc/images/3X8-no-faults-latency-raw.png)

Doing a range of different replica and client counts shows the latency distribution is *very* patterned,
latency spikes/cycles ~128 operations,
and correlates to WAL checkpoint messages in the log.

Seemed worthy of an issue, [latency spike/cycle every ~128 operations, correlates with: replica: on_request: ignoring op=... (too far ahead, checkpoint=...)](https://github.com/tigerbeetledb/tigerbeetle/issues/205) and it's a known issue.

### So, for now, we'll be testing with a 3 replica, 3-5 client environment and see what we can learn.

----

### (P.S. It's premature to partition or introduce other faults too heavily, doing so results in an un-runnable test.)

***But*** after reading [A Database Without Dynamic Memory Allocation](https://tigerbeetle.com/blog/a-database-without-dynamic-memory/):

> Messages contain a checksum to detect random bit flips.
>  
> ...
> 
> When combined with assertions, static allocation is a force multiplier for fuzzing! 

 had to try some file corrupting bit-flipping:

```clj
:bitflip {"n3" {:file "/root/tigerbeetle/data",
                :probability 0.01}}
```
 
 and checksums FTW!

```
error(superblock): quorum: checksum=5049e012706e7b6148fc22259365415 parent=2667ef9fa317bccf04938708b745b0d2 sequence=2 count=1 valid=false
thread 5736 panic: superblock parent quorum lost
/root/tigerbeetle/src/vsr/superblock.zig:1530:21: 0x30c19f in vsr.superblock.SuperBlockType(storage.Storage).read_sector_callback (tigerbeetle)
                    return error.ParentQuorumLost;
                    ^
```
