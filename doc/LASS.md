# Ledger, Assumed Strict Serializable

### A Jepsen test for a strict serializable transactional ledger.

Jepsen's transactional consistency checker, [Elle](https://github.com/jepsen-io/elle), currently supports:

- list-append (most powerful inference rules)
- rw-register (weaker inference rules)
- per-process and realtime ordering 

And as we're going for [peak](https://jepsen.io/consistency) Jepsen, 
let's explore what it would take to implement Elle's analysis and explaining functions to support a ledger.

Our reasoning, design, and implementation should conform to Kingsbury &amp; Alvaro, [Elle: Inferring Isolation Anomalies from Experimental Observations](https://arxiv.org/abs/2003.10554).

----

## Happened Before, Happened After, Don't Know

Observing just Jepsen's log of reads and writes and their values, what can be asserted re transaction ordering?

Transactions will use discrete credit/debit amounts to flip bits in the account values.

### compare r r'

Read and read' can be compared by comparing their values and what bits are flipped.

\> reads are > the number of flipped bits.

### compare w r

w < r if w's bits not flipped

### compare w w'
  - cannot say anything

### order an r in a history

```clj
(read-relations r1 sample-history)
[[:happened-before #{:r0 :w1}]
 [:read :r1] 
 [:happened-after #{:r2 :r3 :w2 :w3}]]
```

### order a w in a history

```clj
(write-relations w2 sample-history)
[[:happened-before #{:r0 :r1}]
 [:write :w2]
 [:happened-after #{:r2 :r3}]]
```

### multiple r's in a transaction

Can order all writes for accounts read relative to this read.

### multiple w's in a transaction

Can order all reads for all debit/credit accounts updated relative to this write.

### Discrete Anomalies

- r and r' claim to have read some values in common, and in addition, each has read unique values

----

## Data and Operations

```clj
; account
{:id :id
 :debits-posted  :u64
 :credits-posted :u64}

; transfer
{:id     :id
 :amount :u64
 :debit-acct  :id
 :credit-acct :id}
```
  