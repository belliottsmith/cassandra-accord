package accord.coordinate;

import java.util.Set;

import com.google.common.base.Preconditions;

import accord.coordinate.tracking.ReadTracker;
import accord.coordinate.tracking.ReadTracker.ReadShardTracker;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.Callback;
import accord.primitives.TxnId;
import accord.topology.Shard;
import accord.topology.Topologies;

abstract class QuorumReadCoordinator<Reply> implements Callback<Reply>
{
    enum Outcome { Abort, Continue, Accept, AcceptFinal, Success }

    enum Done { Failed, Exhausted, ReachedQuorum, Success }

    static class QuorumReadShardTracker extends ReadShardTracker
    {
        private int responseCount;
        public QuorumReadShardTracker(Shard shard)
        {
            super(shard);
        }

        public boolean recordReadResponse(Id node)
        {
            Preconditions.checkArgument(shard.nodes.contains(node));
            ++responseCount;
            --inflight;
            return true;
        }

        @Override
        public boolean recordReadSuccess(Id node)
        {
            if (!super.recordReadSuccess(node))
                return false;

            ++responseCount;
            return true;
        }

        public boolean hasReachedQuorum()
        {
            return responseCount >= shard.slowPathQuorumSize;
        }

        public boolean hasInFlight()
        {
            return inflight > 0;
        }
    }

    static class Tracker extends ReadTracker<QuorumReadShardTracker>
    {
        public Tracker(Topologies topologies)
        {
            super(topologies, QuorumReadShardTracker[]::new, QuorumReadShardTracker::new);
        }

        void recordReadResponse(Id node)
        {
            if (!recordResponse(node))
                return;

            forEachTrackerForNode(node, QuorumReadShardTracker::recordReadResponse);
        }

        public boolean hasReachedQuorum()
        {
            return all(QuorumReadShardTracker::hasReachedQuorum);
        }

        public boolean hasInFlight()
        {
            return any(QuorumReadShardTracker::hasInFlight);
        }
    }

    final Node node;
    final TxnId txnId;
    final Tracker tracker;
    private boolean isDone;
    private Throwable failure;

    QuorumReadCoordinator(Node node, Topologies topologies, TxnId txnId)
    {
        this.node = node;
        this.txnId = txnId;
        this.tracker = new Tracker(topologies);
    }

    void start()
    {
        contact(tracker.computeMinimalReadSetAndMarkInflight());
    }

    abstract void contact(Set<Id> nodes);
    abstract Outcome process(Id from, Reply reply);

    abstract void onDone(Done done, Throwable failure);

    @Override
    public void onSuccess(Id from, Reply reply)
    {
        if (isDone)
            return;

        try
        {
            switch (process(from, reply))
            {
                default: throw new IllegalStateException();
                case Abort:
                    isDone = true;
                    break;

                case Continue:
                    break;

                case Accept:
                    tracker.recordReadResponse(from);
                    if (tracker.hasReachedQuorum())
                    {
                        isDone = true;
                        onDone(Done.ReachedQuorum, null);
                    }
                    break;

                case AcceptFinal:
                    tracker.recordReadSuccess(from);
                    if (!tracker.hasCompletedRead())
                        break;

                case Success:
                    isDone = true;
                    onDone(Done.Success, null);
            }
        }
        catch (Throwable t)
        {
            onFailure(from, t);
        }
    }

    @Override
    public void onSlowResponse(Id from)
    {
        tracker.recordSlowRead(from);
        Set<Id> readFrom = tracker.computeMinimalReadSetAndMarkInflight();
        if (readFrom != null)
            contact(readFrom);
    }

    @Override
    public void onFailure(Id from, Throwable failure)
    {
        if (isDone)
            return;

        if (this.failure == null) this.failure = failure;
        else this.failure.addSuppressed(failure);

        if (tracker.recordReadFailure(from))
        {
            Set<Id> readFrom = tracker.computeMinimalReadSetAndMarkInflight();
            if (readFrom != null)
            {
                contact(readFrom);
            }
            else
            {
                isDone = true;
                if (tracker.hasFailed()) onDone(null, failure);
                else onDone(Done.Exhausted, null);
            }
        }
    }

    @Override
    public void onCallbackFailure(Throwable failure)
    {
        isDone = true;
        if (this.failure != null)
            failure.addSuppressed(this.failure);
        onDone(null, failure);
    }
}
