package accord.burn.random;

import java.util.Objects;
import java.util.Random;

public class SegmentedIntRange implements RandomInt
{
    private final RandomInt small, large;
    private final Decision chooseLargeChance;

    public SegmentedIntRange(RandomInt small, RandomInt large, Decision chooseLargeChance)
    {
        this.small = Objects.requireNonNull(small);
        this.large = Objects.requireNonNull(large);
        this.chooseLargeChance = Objects.requireNonNull(chooseLargeChance);
    }

    @Override
    public int getInt(Random randomSource)
    {
        if (chooseLargeChance.get(randomSource)) return large.getInt(randomSource);
        return small.getInt(randomSource);
    }
}
