package accord.messages;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

import accord.api.Result;
import accord.api.RoutingKey;
import accord.local.Command;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.Status;
import accord.primitives.AbstractRoute;
import accord.primitives.Ballot;
import accord.primitives.PartialDeps;
import accord.primitives.PartialRoute;
import accord.primitives.PartialTxn;
import accord.primitives.Route;
import accord.primitives.RoutingKeys;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.topology.Topologies;

import static accord.messages.TxnRequest.computeScope;

public class CheckStatus implements Request
{
    // order is important
    public enum IncludeInfo
    {
        No, Route, All
    }

    final TxnId txnId;
    final RoutingKeys someKeys;
    final long epoch;
    final IncludeInfo includeInfo;

    public CheckStatus(TxnId txnId, RoutingKeys someKeys, long epoch, IncludeInfo includeInfo)
    {
        this.txnId = txnId;
        this.someKeys = someKeys;
        this.epoch = epoch;
        this.includeInfo = includeInfo;
    }

    public CheckStatus(Id to, Topologies topologies, TxnId txnId, RoutingKeys someKeys, IncludeInfo includeInfo)
    {
        Preconditions.checkState(topologies.currentEpoch() == topologies.oldestEpoch());
        this.txnId = txnId;
        if (someKeys instanceof AbstractRoute)
            this.someKeys = computeScope(to, topologies, (AbstractRoute) someKeys, 0, AbstractRoute::sliceStrict, PartialRoute::union);
        else
            this.someKeys = computeScope(to, topologies, someKeys, 0, RoutingKeys::slice, RoutingKeys::union);
        this.epoch = topologies.currentEpoch();
        this.includeInfo = includeInfo;
    }

    public void process(Node node, Id replyToNode, ReplyContext replyContext)
    {
        Reply reply = node.mapReduceLocal(someKeys, epoch, instance -> {
            Command command = instance.command(txnId);
            Route route = null;
            if (includeInfo != IncludeInfo.No)
            {
                RoutingKeys keys = command.route();
                if (keys != null)
                    route = keys.toRoute(command.homeKey());
            }
            switch (includeInfo)
            {
                default: throw new IllegalStateException();
                case No:
                case Route:
                    return new CheckStatusOk(command.status(), command.promised(), command.accepted(),
                                             node.isCoordinating(txnId, command.promised()),
                                             command.isGloballyPersistent(), route, command.homeKey());
                case All:
                    PartialDeps committedDeps = command.status().compareTo(Status.Committed) >= 0 ? command.savedPartialDeps() : null;
                    return new CheckStatusOkFull(command.status(), command.promised(), command.accepted(),
                                                 node.isCoordinating(txnId, command.promised()),
                                                 command.isGloballyPersistent(), route, command.homeKey(),
                                                 command.partialTxn(), command.executeAt(), committedDeps,
                                                 command.writes(), command.result());
            }
        }, CheckStatusOk::merge);

        if (reply == null)
            reply = CheckStatusNack.nack();

        node.reply(replyToNode, replyContext, reply);
    }

    public interface CheckStatusReply extends Reply
    {
        boolean isOk();
    }

    public static class CheckStatusOk implements CheckStatusReply
    {
        public final Status status;
        public final Ballot promised;
        public final Ballot accepted;
        public final boolean isCoordinating;
        public final boolean hasExecutedOnAllShards;
        public final @Nullable AbstractRoute route;
        public final @Nullable RoutingKey homeKey;

        CheckStatusOk(Status status, Ballot promised, Ballot accepted, boolean isCoordinating, boolean hasExecutedOnAllShards, @Nullable AbstractRoute route, @Nullable RoutingKey homeKey)
        {
            this.status = status;
            this.promised = promised;
            this.accepted = accepted;
            this.isCoordinating = isCoordinating;
            this.hasExecutedOnAllShards = hasExecutedOnAllShards;
            this.route = route;
            this.homeKey = homeKey;
        }

        @Override
        public boolean isOk()
        {
            return true;
        }

        @Override
        public String toString()
        {
            return "CheckStatusOk{" +
                   "status:" + status +
                   ", promised:" + promised +
                   ", accepted:" + accepted +
                   ", hasExecutedOnAllShards:" + hasExecutedOnAllShards +
                   ", isCoordinating:" + isCoordinating +
                   ", route:" + route +
                   ", homeKey:" + homeKey +
                   '}';
        }

        public CheckStatusOk merge(CheckStatusOk that)
        {
            if (that.status.compareTo(this.status) > 0)
                return that.merge(this);

            // preferentially select the one that is coordinating, if any
            CheckStatusOk prefer = this.isCoordinating ? this : that;
            CheckStatusOk defer = prefer == this ? that : this;

            // then select the max along each criteria, preferring the coordinator
            CheckStatusOk maxStatus = prefer.status.compareTo(defer.status) >= 0 ? prefer : defer;
            CheckStatusOk maxPromised = prefer.promised.compareTo(defer.promised) >= 0 ? prefer : defer;
            CheckStatusOk maxAccepted = prefer.accepted.compareTo(defer.accepted) >= 0 ? prefer : defer;
            CheckStatusOk maxHasExecuted = !defer.hasExecutedOnAllShards || prefer.hasExecutedOnAllShards ? prefer : defer;
            CheckStatusOk maxHomeKey = prefer.homeKey != null || defer.homeKey == null ? prefer : defer;
            AbstractRoute mergedRoute = AbstractRoute.merge(prefer.route, defer.route);

            // if the maximum (or preferred equal) is the same on all dimensions, return it
            if (maxStatus == maxPromised && maxStatus == maxAccepted && maxStatus == maxHasExecuted
                && maxStatus.route == mergedRoute && maxStatus == maxHomeKey)
            {
                return maxStatus;
            }

            // otherwise assemble the maximum of each, and propagate isCoordinating from the origin we selected the promise from
            boolean isCoordinating = maxPromised == prefer ? prefer.isCoordinating : defer.isCoordinating;
            return new CheckStatusOk(maxStatus.status, maxPromised.promised, maxAccepted.accepted, isCoordinating,
                                     maxHasExecuted.hasExecutedOnAllShards, mergedRoute, maxHomeKey.homeKey);
        }

        @Override
        public MessageType type()
        {
            return MessageType.CHECK_STATUS_RSP;
        }
    }

    public static class CheckStatusOkFull extends CheckStatusOk
    {
        public final PartialTxn partialTxn;
        public final Timestamp executeAt;
        public final PartialDeps committedDeps; // only set if status >= Committed
        public final Writes writes;
        public final Result result;

        CheckStatusOkFull(Status status, Ballot promised, Ballot accepted, boolean isCoordinating, boolean hasExecutedOnAllShards, AbstractRoute route,
                          RoutingKey homeKey, PartialTxn partialTxn, Timestamp executeAt, PartialDeps committedDeps, Writes writes, Result result)
        {
            super(status, promised, accepted, isCoordinating, hasExecutedOnAllShards, route, homeKey);
            this.partialTxn = partialTxn;
            this.executeAt = executeAt;
            this.committedDeps = committedDeps;
            this.writes = writes;
            this.result = result;
        }

        @Override
        public String toString()
        {
            return "CheckStatusOk{" +
                   "status:" + status +
                   ", promised:" + promised +
                   ", accepted:" + accepted +
                   ", executeAt:" + executeAt +
                   ", hasExecutedOnAllShards:" + hasExecutedOnAllShards +
                   ", isCoordinating:" + isCoordinating +
                   ", deps:" + committedDeps +
                   ", writes:" + writes +
                   ", result:" + result +
                   '}';
        }

        /**
         * This method assumes parameter is of the same type and has the same additional info (modulo partial replication).
         * If parameters have different info, it is undefined which properties will be returned.
         *
         * This method is NOT guaranteed to return CheckStatusOkFull unless the parameter is also CheckStatusOkFull.
         * This method is NOT guaranteed to return either parameter: it may merge the two to represent the maximum
         * combined info, (and in this case if the parameter were not CheckStatusOkFull, and were the higher status
         * reply, the info would potentially be unsafe to act upon when given a higher status
         * (e.g. Accepted executeAt is very different to Committed executeAt))
         */
        public CheckStatusOk merge(CheckStatusOk that)
        {
            CheckStatusOk max = super.merge(that);
            if (this == max || that == max) return max;

            CheckStatusOk maxSrc = this.status.compareTo(that.status) >= 0 ? this : that;
            if (!(maxSrc instanceof CheckStatusOkFull))
                return max;

            CheckStatusOkFull fullMax = (CheckStatusOkFull) maxSrc;
            CheckStatusOk minSrc = maxSrc == this ? that : this;
            if (!(minSrc instanceof CheckStatusOkFull))
            {
                return new CheckStatusOkFull(max.status, max.promised, max.accepted, max.isCoordinating, max.hasExecutedOnAllShards, max.route,
                                             max.homeKey, fullMax.partialTxn, fullMax.executeAt, fullMax.committedDeps, fullMax.writes, fullMax.result);
            }

            CheckStatusOkFull fullMin = (CheckStatusOkFull) minSrc;
            PartialDeps committedDeps = null;
            if (fullMin.committedDeps != null) committedDeps = fullMax.committedDeps.with(fullMin.committedDeps);
            else if (fullMax.committedDeps != null) committedDeps = fullMax.committedDeps;

            return new CheckStatusOkFull(max.status, max.promised, max.accepted, max.isCoordinating, max.hasExecutedOnAllShards, max.route,
                                         max.homeKey, fullMax.partialTxn.with(fullMin.partialTxn), fullMax.executeAt, committedDeps, fullMax.writes, fullMax.result);
        }
    }

    public static class CheckStatusNack implements CheckStatusReply
    {
        private static final CheckStatusNack instance = new CheckStatusNack();

        private CheckStatusNack() { }

        @Override
        public MessageType type()
        {
            return MessageType.CHECK_STATUS_RSP;
        }

        static CheckStatusNack nack()
        {
            return instance;
        }

        @Override
        public boolean isOk()
        {
            return false;
        }

        @Override
        public String toString()
        {
            return "CheckStatusNack";
        }
    }

    @Override
    public String toString()
    {
        return "CheckStatus{" +
               "txnId:" + txnId +
               '}';
    }

    @Override
    public MessageType type()
    {
        return MessageType.CHECK_STATUS_REQ;
    }
}
