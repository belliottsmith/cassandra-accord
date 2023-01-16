package accord.primitives;

public class SyncPoint
{
    public final Timestamp at;
    public final Deps waitFor;

    public SyncPoint(Timestamp at, Deps waitFor)
    {
        this.at = at;
        this.waitFor = waitFor;
    }
}
