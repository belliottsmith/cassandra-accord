package accord.local;

public enum Status
{
    NotWitnessed(0),
    PreAccepted(10),
    Accepted(20),
    AcceptedInvalidate(20),
    PartiallyCommitted(30),
    Committed(40),
    ReadyToExecute(50),
    PartiallyExecuted(60),
    Executed(70),
    Applied(80),
    Invalidated(90);

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
}
