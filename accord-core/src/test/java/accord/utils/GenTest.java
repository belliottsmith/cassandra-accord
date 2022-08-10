package accord.utils;

import org.junit.jupiter.api.Test;

import static accord.utils.Property.qt;

public class GenTest {
    @Test
    public void randomNextInt()
    {
        qt().forAll(Gens.random()).check(r -> {
            int value = r.nextInt(1, 10);
            if (value < 1)
                throw new AssertionError(value + " is less than 1");
            if (value > 10)
                throw new AssertionError(value + " is >= " + 10);
        });
    }
}
