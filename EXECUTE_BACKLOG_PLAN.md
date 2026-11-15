# EXECUTE_BACKLOG_PLAN

Tracking doc for improving `accord.local.cfk.ExecuteTxnBacklog`. The goals are multi-key backlog execution,
taking over any backlog (not only after recovery), and steering execution so that conflicting transactions
run in one DC, spread across the replicas there.

Status legend: `[ ]` todo · `[~]` in progress · `[x]` done · `[-]` dropped

---

## 0. Handover (read this first)

**Where things are.** Branch `execute-backlog` in both repos. Accord commits on top of `fc4a7698`:

| accord | Cassandra | what |
|---|---|---|
| `2c40382c` | `ebed9f91aaf` | Phase 1: multi-key triggering, dedupe, release; `ReadData`/`LocalExecute` bug fixes |
| `743cd85b`, `e91f9a35` | `f4f38e0eaf6`, `ba8c28a3e5e` | Side quest: `Apply.Kind.NoDeps`; `send_minimal` is now `MAXIMAL`/`MINIMAL`/`MINIMAL_NO_DEPS` |
| `b0381f8d` | `eb2ee1f006e` | Phase 1b: `ReadTxnDataIfReady` + `ExecuteBacklogTxn` (definition collected from replicas) |
| `af8c309a` | `b8cdf65ce86` | Phase 2a: `BacklogClaim`, pause/resume at the original coordinator |
| `e464ddbf` | `1e1166b2ca9` | Phase 2b: locality hook, yield to nearby coordinators, burn DCs, burn latency stats |
| branch HEAD | branch HEAD | burn `isSlowCoordinator`, `HappyBurnTest` (happy, 3 DCs, 80 seeds: 0 lost, p50 ≈ 0.5s, p99 ≈ 60s) |

A standalone patch for the `ReadData` sliced-definition bug is in the Cassandra root
(`accord-readdata-sliced-definition.patch`), applicable to unmodified `fc4a7698`.

**Main classes:**
- `accord.local.cfk.ExecuteTxnBacklog`: the node-level sink, triggering, dedupe, yield, claims (both
  sides), obligations, paused executions.
- `accord.coordinate.ExecuteBacklogTxn`: the backlog coordinator.
- `accord.messages.ReadTxnDataIfReady`, `ReadData.ReadyWithDefinition`, `CommitOrReadNack.NotReady`.
- `accord.messages.BacklogClaim`; `ExecuteTxn.onClaimed`/`Paused`.
- `Apply.Kind.NoDeps`; `CoordinationFlags.withStableSent`.
- `TopologySorter.Supplier.isNearby`.

Burn: `Cluster` (`dcs`, `happy`), `DcAwareSorter`, `ExecutionStats`, `HappyBurnTest`.

**Config (all off or unchanged by default):**
- `coordinator_backlog_execution` (existing)
- `coordinator_backlog_execution_multi_key`
- `coordinator_backlog_execution_claims` (new message)
- `coordinator_backlog_execution_yield_to_nearby` (default on)
- `send_minimal: MINIMAL_NO_DEPS` (new encoding)

The new wire formats (`ReadTxnDataIfReady`, `ReadyWithDefinition`, `NotReady`, `BacklogClaim`,
`Apply.Kind.NoDeps`) are only sent when the corresponding option is enabled; no serialization
version bump.

**Next steps, in priority order:**
1. **Evaluate in a real multi-DC environment.** Burn can't establish efficacy: it has high fault and
   recovery rates and synthetic latencies. Measure client latency, duplicate executions (coordinator
   vs backlog executor vs recovery), `Insufficient` Apply replies under `MINIMAL_NO_DEPS`, and claim
   outcomes (paused / completed / resumed). Add metrics for these first (§6, Phase 2b follow-ups).
2. [x] **`HappyBurnTest` modelling gaps** (fixed, see "Happy burn evaluation" in Phase 2b):
   - The p99 of ~60s was a startup artifact. `Cluster.run` spends at least a minute of logical time
     starting up, and the initial requests' latency clock started before then. It now starts at
     submission. This also inflated the means (but not p50/p90) in the non-happy table below.
   - The ~10% recovery attempts came from `ListAgent.slowCoordinatorDelay` (100–1000ms × attempt),
     `slowReplicaDelay` and `Cluster`'s message slow/expiry timeouts, not from `BurnTestBase`
     `coordinationDelays`/`progressDelays`/`timeoutDelays`, which are unused (left in place).
     Happy mode now models Cassandra's defaults: `recover_txn` 5s × attempt, `fetch_txn` 2s × attempt,
     `expire_txn` 5s, and a slow delay of twice the maximum simulated RTT.
3. [x] **Re-run the backlog A/B under `HappyBurnTest`.** With the default (uncontended) happy
   workload the effect on latency is noise-level. With hot keys accessed together (`BurnWorkload`),
   backlog execution cuts mean client latency by 16–24%, at the cost of up to ~20% duplicate
   executions. See "Contended happy evaluation" in Phase 2b.
4. **Phase 3:** replica-side claims and tokens, to suppress the original coordinator's in-flight reads
   (~70% of backlog-attributable duplicates under contention; claims to the coordinator don't reduce
   them), then superseding tokens (§4.7). Competing backlog executors cause the other ~30%, which is
   Phase 3 tokens / Phase 4's deterministic pick.
5. **Phase 2b leftovers:** coordinator liveness in `shouldYieldTo`; wait for or fetch the definition
   if an obligated `ExecuteBacklogTxn` gets data before the definition; defer to live local recovery
   coordinators.
6. **Side-quest follow-ups:** an `Insufficient` metric and deferral window; investigate the high
   volume of `Maximal` Applies in burn; `TinyEnumSet.equals` is broken for subclasses (e.g.
   `ExecuteFlags`).

**Known limitations, by design for now:**
- A coordinator paused by a claim resumes only after `selfExpiresAt(READ_REQ)`. If the claimant
  fails, the client sees extra latency.
- Cassandra only reports `txn_data` results remotely (`AccordAgent.reportRemoteSuccess`). For other
  result kinds, a paused coordinator always resumes by timeout and may then fail with `Exhausted`.
- Claims for multi-key txns are sent at the readiness point, after which some data replies may
  still arrive out of order, so the executor abandons (rare).

---

## 1. Goals

1. **Take over any backlog**, whether or not recovery is involved. Once a node is executing a chain of
   STABLE, unapplied transactions on a key, it keeps going rather than waiting for each original
   coordinator. Each original coordinator may be across the WAN, so each link would cost a WAN round trip.
2. **Multi-key support.** Today only `SingleKey` Read/Write transactions are eligible.
3. **Avoid wasted work.** Only one live executor per txn where possible:
   - competing backlog executors yield to an earlier one;
   - original coordinators abandon their work (ideally *before* reading data) when a backlog executor
     has it in hand.
4. **Locality.** Conflicting transactions (including across shards of multi-key txns) should tend to
   execute in one DC, spread across that DC's replicas.
5. **Compatibility.** Nothing existing breaks. Serialization only differs from today once the new
   functionality is actually in use.

## 2. Guiding principle

Backlog execution runs Stable transactions at `Ballot.ZERO`. Duplicate execution wastes work but is not
unsafe. So **all coordination in this plan is advisory**:
- hints, claims and handovers may be lost or late;
- claims expire;
- the fallback is always today's behaviour (original coordinator, then progress log / recovery).

The one hard requirement is **deterministic precedence**: every party must resolve a conflict the same
way on its own, so that competitors never all yield at once.

## 3. Current state (baseline, accord `fc4a7698`)

- `ExecuteTxn.LocalExecute.applyInternal` installs `node.executeBacklogSink()` on the coordinator's
  local CFK when `coordinatorBacklogExecution(ballot)` holds and the txn is `SingleKey` and the next
  committed txn is `STABLE` + `SingleKey`. The default mode is `ON_RECOVERY`.
- The sink lives on the global CFK cache entry (`InMemoryCommandStore.GlobalCommandsForKey.overrideSink`
  / Cassandra `SaferCommandsForKey`, i.e. `CommandsForKeyCacheEntry.overrideSink`). It is **never
  explicitly released**; it lingers until eviction or until it is overwritten.
  - Side effect: the chain survives a link being executed by someone else. We still receive
    `notWaiting` for the next txn when that link applies locally.
- `ExecuteTxnBacklog.notWaiting` filters (`SingleKey`, Read/Write, not our own `txnId.node`), then
  loads the command. If it is `ReadyToExecute`, it runs `Adapters.standard().execute(...)` with path
  `BACKLOG` (or `RECOVER` if promised).
  - The `notify` id it computes is used only to skip ourselves; nobody is told in advance.
- After execution, `Node.reportLocalExecution` sends `RemoteSuccess` to `txnId.node`, unless that node
  owns the whole route.
- An original coordinator that is a replica already completes via `onLocalDirectSuccess` when it
  applies locally (writes only).

**Gaps for multi-key:**
- `notWaiting` is per key. If T is still waiting on another key (one with no sink) when the sink key
  fires, the `ReadyToExecute` check fails and nothing re-triggers.
- The same txn can be triggered from several keys or command stores on one node. There is no dedupe.
- Being ready on this store does not mean ready everywhere. Other shards may have their own competing
  executors.

## 4. Concepts

### 4.1 Backlog token (explicit claim)
- `BacklogToken`: a chain identity plus a precedence value.
  - **Precedence = chain origin age.** The chain that originated at the oldest transaction (furthest
    back in the chain) wins.
  - Concretely: the origin txn's `executeAt` (chains are ordered by executeAt, so this also compares
    sensibly across keys when chains meet at a multi-key txn), then `TxnId`, then executor node id as
    tie-breaks.
  - **Lower (older origin) wins.**
  - Handovers and continuations *inherit* the token, so a continued chain keeps its origin.
  - A chain that is released and later picked up again gets a fresh (younger) origin. That is
    acceptable.
  - Still open to other formulations; keep the comparison behind one `BacklogToken.compareTo` so it is
    easy to change.
  - **Reserve a `generation` field now** (always 0 for the moment) and compare it *before* the origin
    age. That lets superseding tokens (§4.7) be added later without changing the token's wire format.
- Held by the executor per chain (per key/CFK it is driving).
- Recorded transiently by replicas that see it: on the CFK cache entry next to `overrideSink`, and/or
  on the command, with an expiry. Never persisted.

### 4.2 Implicit claims (the original idea; keep as extra signals)
- If a backlog executor receives an `Apply` / result for a txn in its chain from **another** backlog
  executor, an earlier executor is live → stop.
- If an original coordinator receives an `Apply` (or `RemoteSuccess`) for its txn, someone has done
  the work → abandon, and don't persist.
- Explicit tokens make these signals robust (ordering, partial delivery, short backlogs) but don't
  replace them.

### 4.3 Direct claim to the next coordinator (plus nearby replicas)
When we are about to send `Apply` for T_n and expect to execute T_{n+1} next:
1. **First, or at the same time**, send a direct `BacklogClaim(T_{n+1}, token)` to T_{n+1}'s original
   coordinator.
   - **v1: abandon immediately.** On receipt, the coordinator abandons its coordination of T_{n+1}.
     It sends no further reads or retries, does not persist or send `Apply`, and only waits for
     `RemoteSuccess` (or the local apply) to complete the client callback. If the executor turns out
     to be faulty, recovery falls to the existing machinery (progress log / recovery); the client may
     time out. We'll make recovery from faulty executors better in later iterations.
   - **Check:** `Node.reportLocalExecution` skips `RemoteSuccess` when `txnId.node` owns the whole
     route. It assumes the coordinator completes via its own local apply, but `onLocalDirectSuccess`
     only fires with `writes != null`. An abandoned coordinator must still complete in this case,
     reads included. Either always send `RemoteSuccess` to a coordinator we claimed from, or make the
     local path complete it.
   - **Later: pause with grace.** Pause instead of abandoning, and resume/take over if no `Apply`
     arrives within a grace period. The grace starts when the claim arrives and targets ~0.5 × RTT
     (coordinator ↔ executor), first as a configurable constant, then RTT-derived.
     - Rationale: an executor with healthy local-DC replicas to answer its reads should get an
       `Apply` out within ~0.5 RTT of the claim landing. If it doesn't, taking over is reasonable.
     - Caveat: local replicas may hold back their replies to the slow executor's messages (e.g. because
       they recorded its claim), which would make our takeover's reads compete with or wait on it.
       Superseding tokens (§4.7) may be the answer. Revisit once the surrounding logic has taken
       shape.
   - A successor is any txn that is STABLE at the executor and waits on T_n.
   - T_{n+1} can't become ready anywhere until T_n applies, so the claim normally reaches the
     coordinator before any data read for T_{n+1} can be served.
2. **Optionally**, tell replicas of T_{n+1}, especially those near T_{n+1}'s coordinator (the ones
   likely to serve its reads), so they can stop a read that is already in flight.
   - They record the claim, and reply to the coordinator's pending read with a dataless `Delegated`
     instead of data.
   - The cheapest carrier is the `Apply`/`Stable` we already send them for T_n (optional field). They
     record the claim **before** T_n's application notifies T_{n+1}.
   - Where we don't send T_n's messages to them, use the standalone `BacklogClaim` instead.
- Best effort throughout. Lost or late claims fall back to today's behaviour, where duplicate
  execution is harmless.

### 4.4 Yielding to a nearby live coordinator
Premise: **a coordinator always tries to execute any waiting backlog** behind its own txn. This builds
on Phase 1's "take over any backlog": the coordinator installs the backlog sink whenever STABLE
successors exist.

So, initially, when the chain reaches T whose original coordinator C is **nearby and live**, we just
**stop our backlog execution**: don't execute T, and release our sink/claim on the chain key. C
executes T and carries on with the backlog.

C counts as nearby and live when it is:
- local (same DC);
- alive (`TopologySorter` status healthy);
- plausibly still coordinating T (txn age vs execute timeout).

C must also be able to continue the chain. Not every coordinator can:
- it must be a replica of the chain key(s), so it has the CFK with the remaining backlog;
- it must execute T locally, since the sink is installed in `LocalExecute`, which only runs when
  `permitCoordinatorLocalExecution() && tryIfUniversal(self)`.

If C can't continue the chain, we don't stop. We either skip T (C still executes it, our sink resumes
at T+1) or execute it as normal.

**Decline (later).** The only decline case worth handling at first is that **C has already abandoned
its coordination** (timed out, or the client gave up):
- Leave the door open with an optional `BacklogHandover(T, token, keys)` notice to C, sent alongside
  or instead of the direct claim in §4.3.
- Its reply is either nothing/`Ack`, or `Declined(reason)` with `reason ∈ {ABANDONED}` for now, and is
  extensible later (e.g. `NO_BACKLOG`, `OVERLOADED`).
- On `Declined` we resume: re-install the sink and execute T ourselves. Until declines are
  implemented, the progress log is the fallback if C has in fact gone away.

### 4.5 Multi-key: one executor per txn, chosen deterministically
- Only the **home shard** starts backlog execution of a multi-key T.
- A non-home executor that reaches T hands it off to
  `pick(homeShardReplicasInLocalDc, hash(txnId))`, inheriting the token.
  - Every shard in that DC computes the same pick, so there is no competition within the DC.
  - Hashing on txnId spreads work across that DC's replicas.
- The non-home executor's chain doesn't end; its sink resumes at the next txn once T applies.
- The receiver executes T when it becomes `ReadyToExecute` locally, and may adopt the home key's chain.
- Where chains meet, the older token wins. The winner's DC executes T and adopts the loser's keys, so
  each connected group of conflicting txns settles into one DC over time.

### 4.6 Locality hook
Accord core has no notion of DC; there is only `TopologySorter`. Add a small API, e.g.
`Agent`/`LocalConfig` `isLocal(Node.Id)` and/or `localPeers(Shard)`, implemented in Cassandra via the
snitch, with a trivial implementation in burn tests (simulated DCs).

### 4.7 Superseding tokens (follow-up idea, not yet scheduled)
An original coordinator that suspects the claiming executor is slow or unreliable (once pause-with-grace
exists, §4.3: grace expired with no `Apply`) could, rather than just resuming:
- **declare a superseding token** that beats any older-origin token, by bumping `generation` (§4.1);
- **ask the older executor to stop** (e.g. a `BacklogStop(token)`, or the superseding token on our
  messages acting as "evidence of an older executor", which the old one obeys under the precedence
  rule).

Things to think through before doing this:
- **Duelling supersessions.** Who may supersede, and when? E.g. only T's original coordinator, only
  for chains reaching T, and only after grace expiry. Generations should only increase, like ballots.
- Interaction with chain merging (§4.5) and DC convergence: a superseding coordinator in another DC
  moves the chain's DC.
- Whether replicas should honour a superseding claim for txns *earlier* than T in the old chain.

## 5. Serialization / compatibility rules

- **No new serialization version.** Stay within `Version.V1` and gate everything by config.
- Everything sits behind a `ProtocolModifiers` / config toggle that is **off by default**.
- New fields are optional: a spare header/flag bit plus a trailing field, written **only** when set.
  - `ReadData` header has spare bits (`ReadDataSerializer`).
  - `Apply` writes `ExecuteFlags.bits()` as a vint.
  - `Commit.Kind` uses `EncodeAsVInt32`.
  - With the feature off, bytes are identical to today.
- New message types (`BacklogHandover`, claim notifications) and new reply kinds (`Delegated`,
  `ClaimedBy`) are only *sent* when the feature is on. New replies are only produced in answer to
  requests carrying a token/flag, so old-style requests always get old-style replies.
- Mixed-version clusters must leave the feature off until all nodes understand it. The
  feature-off bytes are unchanged, so this is purely an operational gate.
- Cassandra serializers to touch: `ReadDataSerializer`, `ApplySerializers`, `CommitSerializers`, new
  serializers for new messages; also the verb/message-type registration.

## 6. Phases

### Phase 1: Multi-key triggering, dedupe, and backlog takeover outside recovery
- [x] ~~Trigger on "became `ReadyToExecute`" (via a `ReadyListener`)~~ **Replaced:** execute **only if
      already ready** when we are notified (see "Readiness gating" below). If a txn is still waiting on
      some other key or dependency, whoever unblocks it is the better candidate. That is typically the
      executor of the blocking chain, whose DC will receive our `Apply` within 0.5 RTT. The original
      coordinator is waiting in any case.
- [x] Readiness gating:
  - **Locally:** the txn must be `ReadyToExecute` in every local store that executes it. Stores that
    only participate (e.g. an earlier epoch) need only be `Stable`.
  - **Remote reads:** BACKLOG executions set the existing `ExecuteFlag.NO_WAIT`, so a replica that is
    `Stable` but not ready reports its ranges unavailable instead of waiting. `ExecuteTxn` abandons on
    any unavailable, `Waiting` or `InsufficientAndWaiting` reply from a BACKLOG read
    (`abandonNotReady`). It reports the new `NotReady` failure, tries no alternatives, and sends no
    further messages; `ExecuteTxnBacklog` doesn't report `NotReady` to the agent.
  - No wire changes. Caveat: unavailable conflates "not ready" with "not safe to read" (e.g.
    bootstrapping); we abandon in both cases for now.
- [x] Node-level dedupe, using two checks:
  - `ExecuteTxnBacklog.active`: a txnId → decision-time map. Entries are removed on completion and
    expire after 1 minute, in case a listener is erased without notifying.
  - `Coordinations.anyMatch(txnId, COORDINATES_STATE_MACHINE)`: skip if this node is already
    coordinating or recovering the txn.
- [x] (Superseded by Phase 1b, which removes the "replicates every participant" restriction.)
      Widen eligibility to multi-key Read/Write, behind
      `ProtocolModifiers.coordinatorBacklogExecutionMultiKey()`.
  - Default is off. Burn randomises it. Cassandra exposes it as
    `accord.coordinator_backlog_execution_multi_key`.
  - Shared helpers: `ExecuteTxnBacklog.mayExecute` / `hasBacklogAfter` / `maybeInstall`. `LocalExecute`
    now installs on every key it executes in the store, not just `scope.get(0)`.
  - **Change from the original plan:** the restriction is not "home shard only" but **"this node
    replicates every participant"**. The backlog executor needs the *full* definition and deps to
    coordinate (`txn.execute`/`txn.result`; `PartialTxn.reconstitute` requires coverage, including the
    query held only at the home shard). We gather them from all local command stores
    (`ExecuteTxnBacklog.Gather`). A single store that already covers the route (e.g. single key)
    takes a fast path.
  - Supporting non-universal executors is §7 / follow-up.
- [~] Takeover outside recovery. `CoordinatorBacklogExecution.ALWAYS` already makes every coordinator
      that executes locally (i.e. replicates the whole txn) install the sink. Still open: coordinators
      that are replicas of some keys but don't execute locally never install. Consider installing via
      the coordinator's own `StableThenRead`/read path, or after local apply.
- [x] Explicit release: `ExecuteTxnBacklog.notWaiting` clears our sink when the txn it's about to
      execute has no eligible STABLE successor. `LocalExecute` still overwrites/clears as before.
      Ownership is per node: the sink is a node singleton, so there's no cross-chain ambiguity on a
      node.
- [x] Burn coverage: `coordinatorBacklogExecutionMultiKey` is randomised alongside
      `coordinatorBacklogExecution`. 600 seeds pass after the fixes below, plus a further 380 with
      readiness gating (javac/direct-launch
      harness; see §9). accord-core unit tests match baseline: the same pre-existing failures
      (`CoordinateSyncPointTest.rangeRemovedGlobally`, `AtomicUniqueAutoStaleTimesTest`,
      `GlobalDurabilityTest`, `AsyncChainsTest`) and the same `TopologyChangeTest` hang.
      `TopologyTest` is a flaky property test (it failed on baseline and timed out with the
      changes).

**Bugs found and fixed while doing Phase 1:**
- `LocalExecute` checked `cfk.get(i)` (which indexes `byId`) with an index into
  `committedByExecuteAt`. So the backlog-install decision looked at an arbitrary txn. Now uses
  `committedByExecuteAt(i)`.
- `ReadData.read`/`unsafeToExecute` preferred the *message's* `partialTxn` over the command's.
  - With `StableFastPath` (`WithTxn.HasNewlyOwnedTxnRanges`), the message's txn may be a slice
    containing none of the keys read. The replica then replied `ReadOk` with empty data and no
    `unavailable`, and the coordinator computed writes without reads (caught by `ListUpdate`'s
    invariant).
  - Latent for any FAST-path coordinator after an epoch change. Backlog executors hit it often:
    BACKLOG also uses `StableFastPath`, and executors frequently fall back to alternative replicas
    after a `Redundant` reply.
  - Fix: prefer the command's definition (which already includes anything we were sent) and fall
    back to the message's.

**Observations (300 burn seeds, all modes mixed):**
- About 28.7k multi-key and 5.6k single-key backlog executions.
- `Exhausted` on 26% of multi-key and 10% of single-key executions. Mostly competition: the same
  txn is backlog-executed concurrently by up to 5 nodes, because with `ALWAYS` every universal
  coordinator installs sinks.
- This is the waste Phases 2–3 target.

**Duplicate-execution counter (added):**
- `accord.impl.list.ExecutionStats` (via `ListAgent.onExecuting`/`onExecuted`) logs per burn run:
  attempts by `ExecutePath`, executed txns, executions, txns executed more than once, and duplicate
  executions. An execution counts once its result is computed and it proceeds to persist.
- Burn accepts `-Daccord.burn.coordinator_backlog_execution=<mode>` and
  `-Daccord.burn.coordinator_backlog_execution_multi_key=<bool>`, which override the random choice
  without perturbing the random stream, for A/B comparisons on the same seeds.
- Baseline A/B with readiness gating (seeds 1–40), `DISABLED` vs `ALWAYS` + multi-key:

  | metric               | `DISABLED` | `ALWAYS` + multi-key |
  |----------------------|------------|----------------------|
  | acks                 | 35,442     | 36,671               |
  | lost                 | 1,651      | 927                  |
  | nacks                | 2,728      | 2,284                |
  | duplicate executions | 8,017      | 12,374               |

  That's +4,357 duplicates from 4,809 BACKLOG attempts. Nearly every backlog execution currently
  duplicates the original coordinator, as expected before Phase 2 claims. Note that `DISABLED`
  already duplicates via recovery.

### Phase 1b: Multi-replica backlog reads that collect the definition
Removes the "replicates everything" restriction for multi-key executors. All three variants are
acceptable. New messages are fine (backwards compatible); backlog execution is only enabled after
upgrade.
1. Optimistic read whose reply also includes the replica's `PartialTxn` (and `PartialDeps`).
2. Optimistic read that **first** replies with the `PartialTxn`/`PartialDeps` if ready, or
   unavailable if not, then later replies with the data.
3. Two rounds: collect `PartialTxn`/`PartialDeps` (plus readiness), then read if every shard is ready.

(2) or (3) is preferred, because they give a point where we **know every shard is ready before any
data returns**, which is when a multi-key claim should be sent (see the claim ordering decision
in §8).
- (2) costs nothing extra on success, but wastes data reads at ready replicas when any shard isn't
  ready.
- (3) wastes no data reads, but adds a round trip (local-DC if we choose nearby replicas).
- Leaning (2), with (3) a fallback if wasted reads show up in the duplicate counter.
- `CheckStatus` almost serves for (3) without new messages, but reports only the *max* status across
  a replica's stores, and readiness needs the min.
- [x] Implemented (2):
  - **Replica: `ReadTxnDataIfReady extends ReadTxnData`** (`ReadType.readTxnDataIfReady = 6`).
    - Stores that are `ReadyToExecute` start reading immediately and contribute their
      `partialTxn`/`partialDeps`.
    - The first store that would WAIT abandons: it replies the new final
      `CommitOrReadNack.NotReady`, and the remaining stores are skipped (they see `!isPending()`).
    - Once every store has been processed, a non-final `ReadyWithDefinition(partialTxn, partialDeps)`
      is sent before the final `ReadOk` can be.
    - Generic hooks added to `ReadData`: `abandonIfNotReady`, `onExecuting(Command)`,
      `onSetupComplete`.
    - TODO (low priority): keep a `Cancellable` for reads already started, so they can be aborted.
  - **Coordinator: `ExecuteBacklogTxn`** (a new `ReadCoordinator`, `CoordinationKind.ExecuteBacklog`).
    - Reads at `executeAt.epoch()` only. If the notifying store holds the full definition and deps
      (e.g. single key), it sends plain `ReadTxnData` with `NO_WAIT` and skips collecting the
      definition.
    - Abandons on any `NotReady`/`Waiting`/`InsufficientAndWaiting`/unavailable (`NotReady`), or
      `Redundant` (`Redundant`).
    - Reconstitutes `Txn`/`Deps` from the `ReadyWithDefinition` replies. If the data completes
      before the definition does (messages reordered), it abandons rather than waits.
    - Sends no Stable messages, and persists with `CoordinationFlags.withStableSent(readFrom)`, so
      the replicas it read from get `NoDeps` under `SendMinimal.MINIMAL_NO_DEPS`.
    - `onExecuting` is reported once deps are known, because Cassandra's metrics need them.
    - TODO (expected): the claim point. When `ReadyWithDefinition` has arrived from every shard,
      every replica we read from is known to be ready.
  - `ExecuteTxnBacklog` no longer needs to replicate every participant, so the local `Gather` is
    removed. `ExecuteTxn` no longer special-cases `BACKLOG`.
  - Cassandra: `ReadDataSerializer` handles the new request type and the `ReadyWithDefinition` reply
    (`0x40`). `NotReady` uses the existing nack encoding. New `BacklogMessageSerializersTest` round
    trips `ReadTxnDataIfReady`, `ReadyWithDefinition`, `NotReady` and `Apply.Kind.NoDeps`.
- [ ] Choose nearby read replicas (local DC) for the definition/readiness round (relies on the
      §4.6 locality hook; `ReadTracker` already prefers the nearest candidates).

**Observations (seeds 1–8, `ALWAYS` + multi-key):**
- 3,641 backlog attempts (3,245 collecting the definition): 813 succeeded, 877 `Redundant` (already
  executed by someone else), 1,951 `NotReady`.
- `NotReady` comes mostly from remote replicas (1,394 `NotReady` + 184 unavailable), and partly from
  self (303: another local store not ready). 17 abandoned because the definition was incomplete
  (reordering).
- Same-seed A/B (1–40), `DISABLED` vs `ALWAYS` + multi-key: lost 1,264 → 932, and duplicate
  executions 7,702 → 10,804. So ~44% of the 7,051 backlog executions that reached the execute
  stage still duplicate another coordinator's work. That's for Phase 2/3 claims to address.

### Phase 2a: Claims to the original coordinator (done)
- [x] New `BacklogClaim(txnId)` request (`MessageType.BACKLOG_CLAIM_REQ`; Cassandra
      `Verb.ACCORD_BACKLOG_CLAIM_REQ` 175, `BacklogClaimSerializers`). Gated by
      `ProtocolModifiers.coordinatorBacklogExecutionClaims()`, default off (Cassandra
      `accord.coordinator_backlog_execution_claims`). Burn randomises it (override with
      `-Daccord.burn.coordinator_backlog_execution_claims`).
- [x] **When the backlog executor claims** (only if `txnId.node` is not us and nobody has promised a
      non-zero ballot):
  - **ready:** a multi-key txn, once `ReadyWithDefinition` has arrived covering every shard (the
    Phase 1b readiness point);
  - **read:** any other txn, once all reads have arrived, just before persisting;
  - **ahead:** the next single-key STABLE txn behind the one being executed, just before persisting
    the latter. Captured in `notWaiting` from the CFK; only claimed if we can mark it active, i.e.
    nobody else on this node is executing it.
- [x] **Obligation:** once we've claimed, we must try to complete the txn rather than abandon it.
  - Claimed-ahead txns wait for local readiness (`ReadyListener`).
  - `ExecuteBacklogTxn` in obligated mode reads without `NO_WAIT`, treats unavailable as
    `ApprovePartial`, `NotReady` as `Reject`, `Waiting` as wait, and `InsufficientAndWaiting` as
    `TryAlternative`.
  - `Redundant` still abandons, since someone else has executed the txn.
  - The pre-claim behaviour stays opportunistic, as in Phase 1.
- [x] **Original coordinator:**
  - On a claim, it pauses its `ExecuteTxn` and hands its callback, and the state needed to resume,
    to `ExecuteTxnBacklog.pause`.
  - It completes on `RemoteSuccess`, or on our own replica execution (`Node.reportLocalExecution`,
    persisting if `replicaExecuteDistributedPersist`).
  - It resumes with a fresh `ExecuteTxn` if neither arrives by `selfExpiresAt(READ_REQ)`.
  - Claims that arrive before the coordinator reaches execution are recorded and consumed when
    `ExecuteTxn` starts.
  - `RemoteSuccess` results that arrive before execution starts, or that race a pause, are kept and
    consumed likewise.
  - It never pauses the privileged-coordinator fast path, or non-zero ballots.
- [x] **Not done here:** v1 abandoned immediately without resuming. Resuming on timeout was cheap,
      since the paused state is just `ExecuteTxn`'s constructor arguments, and it avoids stranding
      clients if the claimant fails, or if the result can't be reported (e.g. Cassandra only reports
      `txn_data` results remotely).
- [x] **Burn (seeds 1–40, `ALWAYS` + multi-key, claims off vs on):**
  - 3,377 claims (2,607 ready, 386 read, 384 ahead). Only 703 arrived while the coordinator was
    executing; 2,645 arrived after it had finished.
  - 408 pauses: 333 completed by `RemoteSuccess`/local execution, 75 resumed. Residual resumes come
    from competing claimants, slow executions and reordering.
  - Duplicate executions are not materially reduced (10,929 → 11,024); lost 881 → 994.
  - **Why:** burn has no locality, so the original coordinator's reads typically complete at the
    same moment as the backlog executor's (both wait for the same replicas to become ready), and
    claims arrive too late. The benefit is expected only when the coordinator is remote. Assessing
    it needs a DC-aware burn topology (Phase 2b).

### Phase 2b: Locality and yielding
- [x] Locality hook: `TopologySorter.Supplier.isNearby(Node.Id)`, which defaults to false (unknown), and
      `Node.isNearby(Id)`.
  - Cassandra: `AccordTopologySorter.Supplier` uses `InOurDc.endpoints()` on the mapped endpoint;
    `CompositeTopologySorter.Supplier` says nearby if any delegate does.
- [x] **Burn DCs:** `Cluster` picks 1 (50%), 2 or 3 DCs (override with `-Daccord.burn.dcs`). Nodes and
      clients are assigned to DCs by `floorMod(id, dcs)`.
  - Links between DCs get a fixed per-link base latency of 20–150ms, plus the usual jitter. Links
    within a DC keep the existing distribution.
  - With more than one DC, each node gets a `DcAwareSorter` (prefer self, then the same DC, then
    intersection size), which also makes read-candidate selection DC-aware. With one DC there is no
    locality information, so behaviour is as before.
- [x] **Burn client latency:** each run logs mean/p50/p90/p99 of logical client latency for `Applied`
      replies.
- [x] **Yield** (`ExecuteTxnBacklog.shouldYieldTo`): when notified of T, skip it if T's original
      coordinator is nearby, not us, and plausibly still coordinating
      (`!agent.isSlowCoordinator(now - txnId.hlc)`), unless we are obligated (have claimed T).
  - Also stop listening on the key if that coordinator is expected to continue the backlog itself
    (`ALWAYS` mode, single-key T, and the coordinator replicates the key).
  - Claim-ahead is not sent to successors we would yield to.
  - Configurable: `ProtocolModifiers.coordinatorBacklogExecutionYieldToNearby()`, default on.
    Cassandra: `accord.coordinator_backlog_execution_yield_to_nearby`. Burn randomises it (override
    with `-Daccord.burn.coordinator_backlog_execution_yield`).
- [ ] Coordinator liveness. "Plausibly still coordinating" is age-based only: the coordinator is
      considered slow once older than our first recovery delay (`isSlowCoordinator(attempt=1)`).
  - Fixed: this used `attempt=0`, which Cassandra's `recover_txn = "5s*attempts"` maps to a max wait
    of 0, so Cassandra **never yielded**. It also passed `now() - hlc` as micros, but burn's HLCs are
    millis (now `TimeService.nowUnits()`).
- [ ] Leave room for decline (§4.4): message/reply shapes that allow `Declined(ABANDONED)` later.
- [ ] Also defer to a live local *recovery* coordinator (non-zero promised ballot).
- [ ] When the claimant abandons after claiming (e.g. definition incomplete due to reordering), wait
      for or fetch the definition instead.

**Burn evaluation, 3 DCs, multi-key backlog execution.** Two batches of 40 seeds each (1–40, 41–80);
the "yield" column toggles yielding to nearby coordinators.

| config (latency in logical ms)  | mean (1–40 / 41–80) | p50 | p90 | lost | duplicate executions |
|---------------------------------|---------------------|-----|-----|------|----------------------|
| `DISABLED`                      | 8,605 / 12,115 | 6,560 / 9,760 | 18,089 / 26,274 | 3,999 / 5,344 | 15,472 / 15,667 |
| `ALWAYS`, no yield, no claims   | 4,922 / 5,739  | 3,179 / 4,467 | 8,216 / 10,081  | 2,592 / 2,852 | 20,463 / 20,203 |
| `ALWAYS`, yield, no claims      | 5,469 / 6,137  | 3,955 / 4,522 | 9,128 / 11,297  | 2,763 / 2,920 | 18,515 / 19,008 |
| `ALWAYS`, no yield, claims      | 5,064 / 5,231  | 3,667 / 3,478 | 8,036 / 9,353   | 2,604 / 2,749 | 20,067 / 20,427 |
| `ALWAYS`, yield, claims         | 4,580 / 6,294  | 2,750 / 4,801 | 7,849 / 11,675  | 2,276 / 3,008 | 18,617 / 18,635 |

- **Backlog execution:** roughly halves mean latency (by 2.4× in the second batch), with p90 cut by
  more than half and ~30–45% fewer lost txns. The cost is ~20–30% more duplicate executions.
- **Yielding:** consistently removes ~7–10% of duplicate executions (~20% fewer backlog executions),
  at a cost of ~8–10% mean latency. This is likely because burn always considers nearby coordinators
  live, and its intra-DC links occasionally have large latency spikes.
- **Claims:** their effect on latency is within the noise between batches, and they don't reduce
  duplicates materially. Only ~10–15% of claims arrive while the coordinator is still executing.
- Most duplicate executions (~15.5k) occur without backlog execution at all: recovery racing
  coordinators. The ~3–5k attributable to backlog execution are the original coordinator and backlog
  executors completing concurrently. Phase 3 (replica-side claims that suppress competing reads) is
  the remaining lever for those.
- **Caveat:** means in this table include ~2s+ of startup artifact (see §0 step 2), and the
  simulation's aggressive recovery timings mean that backlog executors largely stand in for slow
  recovery. The happy evaluation below is a better guide to the common case.

**Happy burn evaluation** (`HappyBurnTest`, 3 DCs, multi-key on, after the modelling fixes; seeds
1–40 / 41–80; 40,000 txns per batch, 0 lost):

| config (latency in logical ms)  | mean      | p50       | p90       | p99           | dup. executions | backlog successes |
|---------------------------------|-----------|-----------|-----------|---------------|-----------------|-------------------|
| `DISABLED`                      | 560 / 573 | 561 / 573 | 853 / 877 | 1,235 / 1,293 | 7 / 1           | –                 |
| `ALWAYS`, no yield, no claims   | 551 / 559 | 546 / 561 | 848 / 851 | 1,288 / 1,212 | 1,065 / 944     | 1,690 / 1,595     |
| `ALWAYS`, yield, no claims      | 555 / 555 | 548 / 559 | 870 / 846 | 1,259 / 1,207 | 722 / 780       | 1,164 / 1,316     |
| `ALWAYS`, no yield, claims      | 556 / 559 | 553 / 558 | 848 / 856 | 1,277 / 1,271 | 942 / 1,015     | 1,584 / 1,782     |
| `ALWAYS`, yield, claims         | 555 / 563 | 552 / 563 | 853 / 860 | 1,231 / 1,241 | 724 / 737       | 1,195 / 1,313     |

- Recovery is essentially absent (≤ 4 attempts per batch), so duplicates are now almost entirely
  backlog-attributable: ~2.5% of executions without yielding, ~1.8% with.
- Latency gains are 1–3%, within noise. Only ~4% of txns are backlog-executed, and about as many
  attempts again abandon as `NotReady`.
- Expected: for T_{n+1} itself, the client learns the result via `RemoteSuccess` (one WAN hop from
  the executor), about as soon as it would have from its own read. The gain is that T_{n+1}'s
  `Apply` reaches replicas a WAN round trip sooner, which unblocks T_{n+2}. So it only helps chains
  of length ≥ 2, which this workload rarely builds (5–19 keys × 5–19 prefixes, concurrency 10–39,
  5–30% range txns, which are ineligible and break chains).
- Yielding removes ~20–30% of duplicates at no latency cost. Claims pause ~35% of the txns they're
  sent for, and every pause completed (one resume in 80 seeds), but they don't reduce duplicates
  materially.

**Contended happy evaluation** (`HappyBurnTest`, seeds 1–40, 40,000 txns per row, 0 lost; backlog
`ALWAYS` + multi-key, no yield, no claims). Workloads: `hN rX` means `hot_keys=N`, `hot_ratio=X`
(see `BurnWorkload`); `c40` means `concurrency=40`. "Wait depth" is the number of links a txn waited
on in sequence after it began executing; "x-DC" counts only links whose coordinators are in
different DCs (see `ExecutionStats.chains`). "Exec" is the time from first beginning to execute a
txn to first completing it.

| workload, DCs | mean (ms) `DISABLED` → `ALWAYS` | p90 | wait depth | x-DC wait depth | exec (ms) | dup. executions |
|---------------|----------------------------------|-----|------------|-----------------|-----------|-----------------|
| no hot keys, 3 | 590 → 570 (−3%)    | 866 → 843     | 3.1 → 2.8 | 2.2 → 2.0 | 339 → 315 | 3 → 1,721   |
| h3 r0.5, 3     | 709 → 644 (−9%)    | 1,035 → 939   | 4.2 → 2.9 | 2.9 → 2.0 | 433 → 354 | 1 → 4,412   |
| h4 r0.8, 3     | 800 → 712 (−11%)   | 1,105 → 995   | 5.1 → 3.6 | 3.5 → 2.4 | 511 → 409 | 0 → 5,235   |
| h2 r0.8, 3     | 915 → 721 (−21%)   | 1,260 → 1,045 | 6.3 → 3.4 | 4.3 → 2.3 | 613 → 400 | 0 → 8,583   |
| h3 r0.5 c40, 3 | 949 → 795 (−16%)   | 1,312 → 1,104 | 5.9 → 3.9 | 4.0 → 2.6 | 641 → 477 | 1 → 6,933   |
| h4 r0.8 c40, 3 | 1,097 → 898 (−18%) | 1,442 → 1,217 | 7.2 → 4.6 | 4.9 → 3.1 | 777 → 568 | 1 → 8,192   |
| h3 r0.5, 2     | 601 → 538 (−10%)   | 888 → 798     | 3.7 → 2.7 | 1.9 → 1.3 | 368 → 299 | 0 → 4,714   |
| h2 r0.8, 2     | 743 → 567 (−24%)   | 1,041 → 798   | 5.2 → 2.9 | 2.6 → 1.3 | 491 → 307 | 25 → 9,413  |

Yield and claims variants (`ALWAYS` + multi-key):

| workload    | variant         | mean (ms) | p90   | dup. executions | backlog successes |
|-------------|-----------------|-----------|-------|-----------------|-------------------|
| h2 r0.8     | neither         | 721       | 1,045 | 8,583           | 13,827            |
| h2 r0.8     | yield           | 738       | 1,065 | 5,785           | 9,746             |
| h2 r0.8     | claims          | 719       | 1,049 | 8,444           | 14,260            |
| h2 r0.8     | yield + claims  | 741       | 1,075 | 5,746           | 9,762             |
| h4 r0.8 c40 | neither         | 898       | 1,217 | 8,192           | 11,852            |
| h4 r0.8 c40 | yield           | 919       | 1,245 | 5,729           | 8,375             |
| h4 r0.8 c40 | claims          | 893       | 1,215 | 8,256           | 12,545            |
| h4 r0.8 c40 | yield + claims  | 925       | 1,246 | 5,579           | 8,199             |

- The gain grows with contention, and comes from halving the cross-DC links that txns wait through.
  Without backlog execution each cross-DC link costs at least a one-way inter-DC trip, since the
  `Apply` of one link must reach the replica serving the next link's coordinator. A backlog executor
  that runs consecutive links itself pays only a local round trip for them.
- Relative gains with 2 DCs are similar to (or slightly larger than) those with 3. Fewer links cross
  DCs, but latencies are lower overall.
- **Yield** removes ~30% of duplicates, but gives back ~10% of the latency gain.
- **Claims** have no measurable effect. In h2 r0.8 they pause ~33% of the txns they're sent for (all
  complete), but duplicates don't fall. Paused coordinators would otherwise have stopped on
  `RemoteSuccess` anyway; the coordinators that duplicate got their reads back before the claim
  arrived. With mostly multi-key txns, claim-ahead (single-key only) rarely applies.
- **Duplicate attribution** (h2 r0.8, no yield/claims): of 8,583 duplicates, ~2,400 are competing
  backlog executors (2,145 txns backlog-executed by ≥ 2 nodes), and ~6,200 are the original
  coordinator completing alongside a backlog executor.
- Without contention, the default `replicaExecution` also hides chains: burn randomises it, and
  single-key blind writes executed at replicas advance chains at local speed. Hence `HappyBurnTest`
  defaults to `NONE`, like Cassandra. Cassandra only permits replica execution for blind writes in
  any case, so read-modify-write txns never qualify.

### Phase 3: Claims and abandonment
- [ ] `BacklogToken` type and precedence (§4.1: oldest chain origin wins); carry it on BACKLOG
      `StableThenRead` / `ReadTxnData` / `Commit` / `Apply` and on `BacklogClaim` via optional
      fields (§5).
- [ ] Replica-side transient claim record on the CFK entry/command, with expiry. Never suppress
      requests from recovery (non-zero ballot).
- [ ] `ClaimedBy(token)` reply for a losing executor's reads. The executor aborts before persisting
      and releases its sink on that key.
- [ ] Executor abandons on an inbound request/`Apply` carrying an older token for a txn in or before
      its chain, and on implicit signals (§4.2).
- [ ] Claims to replicas of T_{n+1} near its coordinator (§4.3 step 2): piggyback on T_n's
      `Stable`/`Apply` where we already send it, otherwise a standalone `BacklogClaim`. Record the
      claim before notification.
- [ ] `Delegated` dataless reply to the original coordinator's pending read when a live foreign claim
      exists. The coordinator then waits for `RemoteSuccess`, with a timeout fallback (re-read with
      the claim ignored).
- [ ] Metrics / coordinator events: claims made, yielded, delegated reads, handovers accepted or
      declined. Reuse `coordinatorEvents()`; today backlog uses `onRecoveryStarted`, so give it its
      own event.

### Phase 4: Home-shard hand-off and DC convergence
- [ ] Deterministic executor pick for multi-key T: `pick(homeShardReplicasInLocalDc, hash(txnId))`.
- [ ] Non-home → home hand-off message (it can likely share `BacklogHandover` with a "continue at T"
      payload). The receiver executes on `ReadyToExecute` and optionally adopts the home-key chain.
- [ ] Merging chains: the older-origin token wins, and the winner adopts the loser's keys (the loser
      releases).
- [ ] Optional load spreading for long single-key chains: hand off every N txns, or under load, to
      `pick(localReplicasOfShard, hash(txnId))`.
- [ ] Burn/simulation: multi-DC topology with cross-shard conflict graphs. Measure DC concentration,
      per-replica spread, wasted reads, and chain latency.

### Follow-ups (unscheduled)
- [ ] Pause-with-grace instead of immediate abandon (§4.3): a configurable constant grace, then
      takeover. Later, an RTT-derived grace (~0.5 × RTT coordinator ↔ executor).
- [ ] Superseding tokens (§4.7): a coordinator that gives up on a claim declares a higher-generation
      token and asks the older executor to stop. This may also fix local replicas not answering a
      takeover while they still hold the slow executor's claim.
- [ ] Decline reasons beyond `ABANDONED` (§4.4).

## 6b. Side quests

### `Apply.Kind.NoDeps`: don't resend deps to replicas expected to be Stable
- [x] New `Apply.Kind.NoDeps`, appended so that the existing kinds keep their encoding. It carries
      neither the definition nor the deps. Cassandra's `ApplySerializers` omits the deps field only
      for `NoDeps`.
- [x] The replica side needed no change. `Commands.apply` already accepts null deps: `KEEP_DEPS` if
      the replica has `DepsKnown`, otherwise `Insufficient`, in which case `Persist` falls back to
      `Maximal`.
- [x] The decision is made per recipient, in `Persist.applyKind(to, status)`: a healthy recipient that we
      **expect to have been sent Stable** gets `NoDeps`. Everyone else keeps the existing kind.
  - This follows the convention of other minimal messages: *assume* earlier messages were delivered,
    and repair with `Maximal` on `Insufficient`.
  - `ExecuteTxn` tracks `sentStable`: replicas sent `StableThenRead`/`Commit`/maximal commit, or self
    via a non-fast `LocalExecute`.
  - It is passed to `Persist` as coordinator-local knowledge through
    `CoordinationFlags.withStableSent` / `isStableSent`, so no adapter signatures change.
  - Paths without this knowledge (recovery, interop, blind-write persist) behave as before.
  - With the default `SendStableMessages.FOR_READS_OR_NONE_IF_FASTEXEC`, only readers are sent Stable,
    so non-readers still receive deps.
- [x] Configured by merging into `send_minimal`: `ProtocolModifiers.SendMinimal { MAXIMAL, MINIMAL,
      MINIMAL_NO_DEPS }`, default `MINIMAL`.
  - Cassandra's `accord.send_minimal` is now this enum (the tests that set `false` now set `MAXIMAL`).
  - `MINIMAL_NO_DEPS` must not be enabled until every node understands `NoDeps`.
- [x] Burn randomises it (override with `-Daccord.burn.send_minimal_no_deps`).
- [ ] Later, if `Insufficient` replies are frequent: a general mechanism, for all phases, to *defer* an
      `Insufficient` reply for a short window, so a slow earlier message (e.g. a large Stable on the
      large-message connection) can be delivered first. Add a metric for `Insufficient` replies to
      decide whether this is needed. `AccordExecutor` should already order a queued Stable before a
      queued Apply.
- Observation (not investigated): `Maximal` Applies are numerous in burn, sometimes more than
  `Minimal`. They come from non-HEALTHY recipients and the recovery/sync point adapters. Possibly
  worth a look.

## 7. Open questions

- ~~Multi-key executors that don't replicate the whole txn~~: resolved by Phase 1b (the definition
  is collected from the replicas we read from).
- Precedence formulation: oldest chain origin is the working choice (§4.1). Revisit if it behaves
  badly, e.g. long-lived chains starving others, or comparing origins across keys.
- Claim expiry duration. Tie it to the execute timeout?
- Recovery from faulty backlog executors, beyond the progress log: pause-with-grace, superseding
  tokens, and how local replicas treat a takeover while they hold the old executor's claim. The
  expectation is that a healthy executor sends `Apply` within ~0.5 RTT of the claim landing, so not
  seeing one is good grounds to take over. Settle the details once the surrounding logic has taken
  shape.
- Should a claim cover a key range/chain, or individual txnIds (the direct claim suggests per-txn,
  with the chain token)?
- Behaviour when the winning DC is unhealthy or overloaded: a fixed vs dynamic affinity escape hatch.
- Alternative kept in reserve: **fixed affinity** (executor = pure function of shard/home key and
  txnId; no claim messages). Simpler, but ignores chain locality and doesn't adapt.

## 8. Decisions log

- Takeover applies to any backlog, not just recovery.
- Coordinators always try to execute any waiting backlog behind their own txn. A backlog executor
  therefore simply **stops** when it reaches a txn with a nearby live coordinator that can continue
  the chain. Decline is not needed initially, except later for "coordinator already abandoned"; keep
  the message shapes open for it.
- Use explicit tokens with **oldest-chain-origin** precedence; keep implicit "received Apply/result"
  signals.
- Claims are sent **directly to the next txn's coordinator**, first or alongside the `Apply` for the
  preceding txn. Optionally they also go to replicas (especially those near that coordinator) to cut
  off in-flight reads.
- **v1:** a coordinator receiving a claim **abandons immediately** and only waits for `RemoteSuccess`.
  Iterate from there to improve recovery from faulty executors. The likely next step is
  pause-with-grace (~0.5 × RTT after receipt, initially a configurable constant), then takeover.
- Superseding tokens (a coordinator overriding an older, slow executor) are a follow-up idea. The
  token format reserves a `generation` field so they can be added later.
- **Only execute what is already ready.** We abandon if any replica reports not ready, using the
  existing `NO_WAIT` flag. This keeps execution with whoever unblocks the txn last.
- **Claim ordering:** claim ahead only for successors known to become ready once our predecessor
  applies. Claim other multi-key txns after readiness is confirmed. No "pause claim" state. The cost
  is occasionally failing to claim before the original coordinator also does the work. That's
  acceptable; watch the duplicate-execution counter and improve if it fires a lot.
- Multi-replica backlog reads will collect the definition (Phase 1b; prefer variant 2 or 3). New
  message types are fine.
- Wire changes are acceptable, but **no new serialization version**: bytes stay unchanged unless the
  feature is in use.
- Release sinks/claims explicitly.
- Phase order: (1) multi-key triggering → (2) coordinator interaction → (3) claims/abandonment →
  (4) home-shard hand-off and DC convergence.

## 9. Testing notes

- Burn knobs (system properties; override the random choice without perturbing the random stream):
  - `accord.burn.dcs`
  - `accord.burn.happy`, or run `accord.burn.HappyBurnTest`
  - `accord.burn.coordinator_backlog_execution=DISABLED|ON_RECOVERY|ALWAYS`
  - `accord.burn.coordinator_backlog_execution_multi_key`
  - `accord.burn.coordinator_backlog_execution_claims`
  - `accord.burn.coordinator_backlog_execution_yield`
  - `accord.burn.send_minimal_no_deps`
- Each run logs `Executions:` (attempts by path, duplicate executions), `Chains:` (wait depth,
  cross-DC wait depth, execution latency) and `Applied latency (ms)`.
- Workload overrides (`BurnWorkload`; `HappyBurnTest` defaults in brackets): `accord.burn.keys`,
  `accord.burn.prefixes` [1], `accord.burn.concurrency`, `accord.burn.range_ratio` [0],
  `accord.burn.replica_execution` [`NONE`], `accord.burn.hot_keys` / `accord.burn.hot_ratio`.
  For a contended workload, try `-Daccord.burn.hot_keys=2 -Daccord.burn.hot_ratio=0.8`.
- TRACE logging on `accord.local.cfk.ExecuteTxnBacklog` and `accord.coordinate.ExecuteBacklogTxn`
  reports each backlog attempt, its outcome, claims, pauses, resumes and yields.
- `ListAgent.isSlowCoordinator` mirrors Cassandra: slow once `elapsed` exceeds the maximum
  `slowCoordinatorDelay` for that attempt (`attempt × 1s`, or `attempt × 5s` when happy).
- Burn latency is measured from submission. The initial requests are only submitted after
  `Cluster.run` has spent at least a minute of logical time starting up.

- The accord module can be built without gradle: `javac` all of `accord-core/src/{main,test}/java`
  against `accord-core/build/dependencies/{main,test}/libs`, then run
  `accord.burn.BurnTest -c N` (or `-s seed`) with `-Dlogback.configurationFile=burn-logback.xml`.
- Unit tests run via the JUnit platform launcher (`/usr/share/java/junit5`, excluding vintage, plus
  objenesis from `build/test/lib/jars`).
- Cassandra-side changes compile via the `cassandra-run-tests` overlay.
