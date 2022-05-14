package accord.impl.list;

import java.util.Map;

import accord.api.Read;
import accord.api.Update;
import accord.local.Node.Id;
import accord.api.Data;
import accord.api.Key;
import accord.api.Query;
import accord.api.Result;

public class ListQuery implements Query
{
    final Id client;
    final long requestId;

    public ListQuery(Id client, long requestId)
    {
        this.client = client;
        this.requestId = requestId;
    }

    @Override
    public Result compute(Data data, Read untypedRead, Update update)
    {
        ListRead read = (ListRead) untypedRead;
        int[][] values = new int[read.readKeys.size()][];
        for (Map.Entry<Key, int[]> e : ((ListData)data).entrySet())
            values[read.readKeys.indexOf(e.getKey())] = e.getValue();
        return new ListResult(client, requestId, read.keys, values, (ListUpdate) update);
    }
}
