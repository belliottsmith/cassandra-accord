package accord.burn.random;

import java.util.Random;

public class IntRange implements RandomInt
{
    private final int min, upperBound;

    public IntRange(int min, int max)
    {
        if (min >= max) throw new IllegalArgumentException(String.format("Min (%s) should be less than max (%d).", min, max));
        this.min = min;
        this.upperBound = max - min + 1;
    }

    @Override
    public int getInt(Random randomSource)
    {
        return min + randomSource.nextInt(upperBound);
    }
}
