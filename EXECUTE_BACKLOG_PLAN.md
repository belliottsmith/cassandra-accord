# EXECUTE_BACKLOG_PLAN

Tracking doc for improving `accord.local.cfk.ExecuteTxnBacklog`. The goals are multi-key backlog execution,
taking over any backlog (not only after recovery), and steering execution so that conflicting transactions
run in one DC, spread across the replicas there.

Status legend: `[ ]` todo · `[~]` in progress · `[x]` done · `[-]` dropped

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
- [x] Widen eligibility to multi-key Read/Write, behind
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
- [ ] Choose (2) or (3) and implement. The executor reconstitutes `Txn`/`Deps` from the replies.
- [ ] Choose nearby read replicas (local DC) for the definition/readiness round.

### Phase 2: Original coordinator interaction (no tokens yet)
- [ ] Locality hook (§4.6) in accord core, plus Cassandra and burn implementations.
- [ ] Make sure coordinators always pick up waiting backlog behind their own txn (relies on Phase 1
      takeover), so that yielding to them is sufficient.
- [ ] Coordinator "plausibly still coordinating" heuristic: txn age vs execute timeout, plus liveness.
- [ ] Yield (§4.4): stop our chain at T when C is nearby, live and able to continue the chain; release
      our sink. Otherwise skip T or execute it.
- [ ] Direct `BacklogClaim(T_{n+1})` to the next coordinator, sent first or alongside `Apply` for T_n
      (§4.3 step 1), **only for successors we already know will be ready** once T_n applies:
      single-key successors, or successors whose only remaining local wait is on T_n. Claim other
      multi-key txns only once every shard is confirmed ready (Phase 1b readiness point), still
      before our `Apply`. **v1:** the coordinator's `ExecuteTxn` abandons immediately: no further reads or
      retries, no persist/`Apply`, and it only waits for `RemoteSuccess` (or the local apply) to
      complete the client.
- [ ] Make sure an abandoned coordinator always gets completed. Today `reportLocalExecution` skips
      `RemoteSuccess` when the coordinator owns the whole route, and `onLocalDirectSuccess` needs
      writes (§4.3 "Check").
- [ ] Coordinator abandons on evidence: local apply (exists for writes via `onLocalDirectSuccess`;
      extend to reads), `RemoteSuccess` (exists), incoming `Apply` for its txn. Abandoning must not
      persist/send `Apply`, and must not leave the client callback hanging.
- [ ] Leave room for decline (§4.4): message/reply shapes that allow `Declined(ABANDONED)` later.
      Implementing it is optional in this phase.
- [ ] Also defer to a live local *recovery* coordinator (non-zero promised ballot).

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

### `Apply.Kind.NoDeps`: don't resend deps to replicas known to be Stable
- [x] New `Apply.Kind.NoDeps`, appended so that the existing kinds keep their encoding. It carries
      neither the definition nor the deps. Cassandra's `ApplySerializers` omits the deps field only
      for `NoDeps`.
- [x] The replica side needed no change. `Commands.apply` already accepts null deps: `KEEP_DEPS` if
      the replica has `DepsKnown`, otherwise `Insufficient`, in which case `Persist` falls back to
      `Maximal`.
- [x] The decision is made per recipient, in `Persist.applyKind(to, status)`: a healthy recipient
      that the coordinator *knows* is Stable gets `NoDeps`. Everyone else keeps the existing kind.
  - `ExecuteTxn` tracks `sentStable`: replicas sent `StableThenRead`/`Commit`/maximal commit, or self
    via a non-fast `LocalExecute`.
  - `knownStable` is the subset of those that replied ok or `Waiting`.
  - It is passed to `Persist` as coordinator-local knowledge through
    `CoordinationFlags.withKnownStable` / `isKnownStable`, so no adapter signatures change.
  - Paths without this knowledge (recovery, interop, blind-write persist) behave as before.
- [x] We use "acknowledged Stable" rather than merely "sent Stable". Cassandra splits small and large
      messages onto different connections, so a large Stable could be overtaken by a small `NoDeps`
      Apply. The replica would then reply `Insufficient` and we'd send `Maximal`, costing more than
      we saved. This mostly forgoes the saving for remote-DC replicas whose Stable ack hasn't
      arrived by the time we persist.
  - Possible follow-up: also send `NoDeps` to replicas we've merely sent Stable to, and have the
    replica wait briefly for an in-flight Stable before replying `Insufficient`.
- [x] Gated by `ProtocolModifiers.applyOmitsDepsIfKnownStable()`, default off. Cassandra:
      `accord.apply_omits_deps_if_known_stable`. Must stay off until every node understands
      `NoDeps`.
- [x] Burn randomises it (override with `-Daccord.burn.apply_omits_deps_if_known_stable`). Same-seed
      A/B over 40 seeds: identical outcomes and `APPLY_REQ` counts, so no `Insufficient` fallbacks.
      Over 8 seeds, 40–85% of what would have been `Minimal` Applies were sent as `NoDeps`.
- Observation (not investigated): `Maximal` Applies are numerous in burn, sometimes more than
  `Minimal`. They come from non-HEALTHY recipients and the recovery/sync point adapters. Possibly
  worth a look.

## 7. Open questions

- **Multi-key executors that don't replicate the whole txn** need the full definition and deps. Options:
  - fetch from the original coordinator (has the full txn, but may be remote or gone);
  - assemble from each shard, e.g. replicas include their `PartialTxn`/`PartialDeps` slice in the read
    reply, or a `CheckStatus`-style fetch first. Costs an extra round trip unless piggybacked;
  - keep the "replicates everything" restriction, and rely on handing off to such a node (Phase 4
    `pick` should then choose among nodes that replicate the whole txn when possible).
  This affects §4.5: the home shard has the query but not the other shards' read/update.
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

- The accord module can be built without gradle: `javac` all of `accord-core/src/{main,test}/java`
  against `accord-core/build/dependencies/{main,test}/libs`, then run
  `accord.burn.BurnTest -c N` (or `-s seed`) with `-Dlogback.configurationFile=burn-logback.xml`.
- Unit tests run via the JUnit platform launcher (`/usr/share/java/junit5`, excluding vintage, plus
  objenesis from `build/test/lib/jars`).
- Cassandra-side changes compile via the `cassandra-run-tests` overlay.
