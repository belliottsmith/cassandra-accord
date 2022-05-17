package accord.primitives;

import accord.api.Query;
import accord.api.Read;
import accord.api.Update;

public class PartialTxn extends Txn
{
    public final KeyRanges covering;
    public final Kind kind; // TODO: we do not need to take a write-edge dependency on every key

    public PartialTxn(KeyRanges covering, Kind kind, Keys keys, Read read, Query query)
    {
        super(keys, read, query);
        this.covering = covering;
        this.kind = kind;
    }

    public PartialTxn(KeyRanges covering, Kind kind, Keys keys, Read read, Query query, Update update)
    {
        super(keys, read, query, update);
        this.covering = covering;
        this.kind = kind;
    }

    public boolean covers(KeyRanges ranges)
    {
        return covering.contains(ranges);
    }

    // TODO: merge efficient merge when more than one input
    public PartialTxn with(PartialTxn add)
    {
        if (!add.kind.equals(kind))
            throw new IllegalArgumentException();

        KeyRanges covering = this.covering.union(add.covering);
        Keys keys = this.keys.union(add.keys);
        Read read = this.read.merge(add.read);
        Query query = this.query == null ? add.query : this.query;
        Update update = this.update == null ? null : this.update.merge(add.update);
        if (keys == this.keys)
        {
            if (covering == this.covering && read == this.read && query == this.query && update == this.update)
                return this;
        }
        else if (keys == add.keys)
        {
            if (covering == add.covering && read == add.read && query == add.query && update == add.update)
                return add;
        }
        return new PartialTxn(covering, kind, keys, read, query, update);
    }
}
