package accord.primitives;

import accord.api.RoutingKey;

import javax.annotation.Nullable;

/**
 * Something that can be found in the cluster, and MAYBE found on disk (if Seekable)
 */
public interface Routable
{
    enum Domain
    {
        Key, Range;
        private static final Domain[] VALUES = Domain.values();

        public boolean isKey()
        {
            return this == Key;
        }

        public boolean isRange()
        {
            return this == Range;
        }

        public static Routable.Domain ofOrdinal(int ordinal)
        {
            return VALUES[ordinal];
        }
    }

    Domain domain();
    Unseekable toUnseekable();

    /**
     * Deterministically select a key that intersects this Routable and the provided Ranges
     */
    RoutingKey someIntersectingRoutingKey(@Nullable Ranges ranges);
}
