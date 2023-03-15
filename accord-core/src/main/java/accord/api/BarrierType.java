package accord.api;

public enum BarrierType
{
    // Only wait until the barrier is achieved locally, and possibly don't trigger the barrier remotely
    local(false, true),
    // Wait until the barrier has been achieved at a quorum globally
    global_sync(true, false),
    // Trigger the global barrier, but only block on creation of the barrier and local application
    global_async(true, true);

    public final boolean global;
    public final boolean async;

    BarrierType(boolean global, boolean async)
    {
        this.global = global;
        this.async = async;
    }
}
