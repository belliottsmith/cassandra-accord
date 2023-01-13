package accord.primitives;

import accord.utils.Invariants;

public class PartialDeps extends Deps
{
    public static final PartialDeps NONE = new PartialDeps(Ranges.EMPTY, KeyDeps.NONE, RangeDeps.NONE);

    public static Builder builder(Ranges covering)
    {
        return new Builder(covering);
    }

    public static class Builder extends AbstractBuilder<PartialDeps>
    {
        final Ranges covering;
        public Builder(Ranges covering)
        {
            this.covering = covering;
        }

        @Override
        public PartialDeps build()
        {
            return new PartialDeps(covering, keyBuilder.build(), rangeBuilder == null ? RangeDeps.NONE : rangeBuilder.build());
        }
    }

    public final Ranges covering;

    public PartialDeps(Ranges covering, KeyDeps keyDeps, RangeDeps rangeDeps)
    {
        super(keyDeps, rangeDeps);
        this.covering = covering;
        Invariants.checkState(covering.containsAll(keyDeps.keys));
        Invariants.checkState(rangeDeps.isCoveredBy(covering));
    }

    public boolean covers(Unseekables<?, ?> keysOrRanges)
    {
        return covering.containsAll(keysOrRanges);
    }

    public PartialDeps with(PartialDeps that)
    {
        Invariants.checkArgument((this.rangeDeps == null) == (that.rangeDeps == null));
        return new PartialDeps(that.covering.with(this.covering),
                this.keyDeps.with(that.keyDeps),
                this.rangeDeps == null ? null : this.rangeDeps.with(that.rangeDeps));
    }

    public Deps reconstitute(FullRoute<?> route)
    {
        if (!covers(route))
            throw new IllegalArgumentException();
        return new Deps(keyDeps, rangeDeps);
    }

    // covering might cover a wider set of ranges, some of which may have no involved keys
    public PartialDeps reconstitutePartial(Ranges covering)
    {
        if (!covers(covering))
            throw new IllegalArgumentException();

        if (covers(covering))
            return this;

        return new PartialDeps(covering, keyDeps, rangeDeps);
    }

    @Override
    public boolean equals(Object that)
    {
        return this == that || (that instanceof PartialDeps && equals((PartialDeps) that));
    }

    @Override
    public boolean equals(Deps that)
    {
        return that instanceof PartialDeps && equals((PartialDeps) that);
    }

    public boolean equals(PartialDeps that)
    {
        return this.covering.equals(that.covering) && super.equals(that);
    }

    @Override
    public String toString()
    {
        return covering + ":" + super.toString();
    }
}
