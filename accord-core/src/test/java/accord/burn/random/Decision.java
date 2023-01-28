package accord.burn.random;

import java.util.Random;

public interface Decision
{
    boolean get();

    public static class FixedChance implements Decision
    {
        private final Random random;
        private final float chance;

        public FixedChance(Random random, float chance)
        {
            this.random = random;
            this.chance = chance;
        }

        @Override
        public boolean get()
        {
            return random.nextFloat() < chance;
        }
    }
}
