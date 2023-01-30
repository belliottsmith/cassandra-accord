package accord.burn.random;

import java.util.Random;

public interface Decision
{
    boolean get(Random randomSource);

    public static class FixedChance implements Decision
    {
        private final float chance;

        public FixedChance(float chance)
        {
            this.chance = chance;
        }

        @Override
        public boolean get(Random randomSource)
        {
            return randomSource.nextFloat() < chance;
        }
    }
}
