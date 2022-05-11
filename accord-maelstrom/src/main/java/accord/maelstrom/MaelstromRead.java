package accord.maelstrom;

import accord.api.*;
import accord.primitives.Keys;
import accord.primitives.Timestamp;

public class MaelstromRead implements Read
{
    final Keys keys;

    public MaelstromRead(Keys keys)
    {
        this.keys = keys;
    }

    @Override
    public Keys keys()
    {
        return keys;
    }

    @Override
    public Data read(Key key, Timestamp executeAt, DataStore store)
    {
        MaelstromStore s = (MaelstromStore)store;
        MaelstromData result = new MaelstromData();
        result.put(key, s.get(key));
        return result;
    }
}
