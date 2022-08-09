package accord.utils;

import accord.utils.Gen.Random;

import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class Property
{
    public static abstract class Common<T extends Common<T>>
    {
        protected long seed = ThreadLocalRandom.current().nextLong();
        protected int examples = 100;

        protected Common() {
        }

        protected Common(Common<?> other) {
            this.seed = other.seed;
            this.examples = other.examples;
        }

        public T withSeed(long seed)
        {
            this.seed = seed;
            return (T) this;
        }

        public T withExamples(int examples)
        {
            if (examples <= 0)
                throw new IllegalArgumentException("Examples must be positive");
            this.examples = examples;
            return (T) this;
        }

        protected String propertyMessage()
        {
            return "Seed=" + seed + "\nExamples=" + examples;
        }
    }

    public static class ForBuilder extends Common<ForBuilder>
    {
        public <T> SingleBuilder<T> forAll(Gen<T> gen)
        {
            return new SingleBuilder<>(gen, this);
        }

        public <A, B> DoubleBuilder<A, B> forAll(Gen<A> a, Gen<B> b)
        {
            return new DoubleBuilder<>(a, b, this);
        }
    }

    public static class SingleBuilder<T> extends Common<SingleBuilder<T>>
    {
        private final Gen<T> gen;

        private SingleBuilder(Gen<T> gen, Common<?> other) {
            super(other);
            this.gen = Objects.requireNonNull(gen);
        }

        public void check(Consumer<T> fn)
        {
            Random random = new Random(seed);
            for (int i = 0; i < examples; i++)
            {
                try
                {
                    fn.accept(gen.next(random));
                }
                catch (Throwable t)
                {
                    throw new PropertyError(propertyMessage(), t);
                }
            }
        }
    }

    public static class DoubleBuilder<A, B> extends Common<DoubleBuilder<A, B>>
    {
        private final Gen<A> a;
        private final Gen<B> b;

        private DoubleBuilder(Gen<A> a, Gen<B> b, Common<?> other) {
            super(other);
            this.a = Objects.requireNonNull(a);
            this.b = Objects.requireNonNull(b);
        }

        public void check(BiConsumer<A, B> fn)
        {
            Random random = new Random(seed);
            for (int i = 0; i < examples; i++)
            {
                try
                {
                    fn.accept(a.next(random), b.next(random));
                }
                catch (Throwable t)
                {
                    throw new PropertyError(propertyMessage(), t);
                }
            }
        }
    }

    public static class PropertyError extends AssertionError
    {
        public PropertyError(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static ForBuilder qt()
    {
        return new ForBuilder();
    }
}
