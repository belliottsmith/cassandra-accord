package accord.burn.random;

import java.util.Objects;
import java.util.Random;

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
    public int getInt(Random randomSource)
    {
        if (largeDecision.get(randomSource)) return large.getInt(randomSource);
        return small.getInt(randomSource);
    }
}
