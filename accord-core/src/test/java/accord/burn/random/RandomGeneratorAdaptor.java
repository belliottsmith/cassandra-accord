package accord.burn.random;

import org.apache.commons.math3.random.AbstractRandomGenerator;

import java.util.Objects;
import java.util.Random;

public class RandomGeneratorAdaptor extends AbstractRandomGenerator
{
    private final Random random;

    public RandomGeneratorAdaptor(Random random)
    {
        this.random = Objects.requireNonNull(random);
    }

    @Override
    public void setSeed(long seed)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public double nextDouble()
    {
        return random.nextDouble();
    }
}
