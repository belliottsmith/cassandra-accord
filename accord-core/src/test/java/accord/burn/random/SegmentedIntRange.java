package accord.burn.random;

import java.util.Objects;

public class SegmentedIntRange implements RandomInt
{
    private final RandomInt small, large;
    private final Decision largeDecision;

    public SegmentedIntRange(RandomInt small, RandomInt large, Decision largeDecision) {
        this.small = Objects.requireNonNull(small);
        this.large = Objects.requireNonNull(large);
        this.largeDecision = Objects.requireNonNull(largeDecision);
    }

    @Override
    public int getInt()
    {
        if (largeDecision.get()) return large.getInt();
        return small.getInt();
    }
}
