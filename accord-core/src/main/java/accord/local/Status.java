package accord.local;

import java.util.List;
import java.util.function.Function;

import accord.messages.BeginRecovery.RecoverOk;
import accord.primitives.Ballot;

public enum Status
{
    NotWitnessed(0),
    PreAccepted(10),
    Accepted(20),
    AcceptedInvalidate(20),
    Committed(30),
    ReadyToExecute(30),
    Executed(50),
    Applied(60),
    Invalidated(70);

    final int logicalOrdinal;

    Status(int logicalOrdinal)
    {
        this.logicalOrdinal = logicalOrdinal;
    }

    // equivalent to compareTo except Accepted and AcceptedInvalidate sort equal
    public int logicalCompareTo(Status that)
    {
        return this.logicalOrdinal - that.logicalOrdinal;
    }

    public static Status max(Status a, Status b)
    {
        return a.compareTo(b) >= 0 ? a : b;
    }

    public boolean hasBeen(Status equalOrGreaterThan)
    {
        return compareTo(equalOrGreaterThan) >= 0;
    }

    public static <T> T maxAcceptedOrLater(List<T> list, Function<T, Status> getStatus, Function<T, Ballot> getAccepted)
    {
        Status maxStatus = null;
        Ballot maxAccepted = null;
        T max = null;
        for (T item : list)
        {
            Status status = getStatus.apply(item);
            if (!status.hasBeen(Accepted))
                continue;

            Ballot accepted = getAccepted.apply(item);
            boolean update =    max == null
                             || maxStatus.compareTo(status) < 0
                             || (status == Accepted && maxAccepted.compareTo(accepted) < 0);
            if (update)
            {
                max = item;
                maxStatus = status;
                maxAccepted = getAccepted.apply(item);
            }
        }
        return max;
    }
}
