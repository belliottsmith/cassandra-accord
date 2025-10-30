/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package accord.primitives;

/**
 * Defines an inequality point in the processing of the distributed transaction log, which is to say that
 * this is able to say that the point has passed, or that it has not yet passed, but it is unable to
 * guarantee that it is processed at the precise moment given by {@code at}. This is because we do not
 * expect the whole cluster to process these, and we do not want transaction processing to be held up,
 * so while these are processed much like a transaction, they are invisible to real transactions which
 * may proceed before this is witnessed by the node processing it.
 */
public class MinimalSyncPoint
{
    public static class SerializationSupport
    {
        public static MinimalSyncPoint construct(TxnId syncId, Timestamp executeAt, RangeRoute route)
        {
            return new MinimalSyncPoint(syncId, executeAt, route);
        }
    }

    public final TxnId syncId;
    public final Timestamp executeAt;
    public final RangeRoute route;

    public MinimalSyncPoint(TxnId syncId, Timestamp executeAt, RangeRoute route)
    {
        this.syncId = syncId;
        this.executeAt = executeAt;
        this.route = route;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MinimalSyncPoint syncPoint = (MinimalSyncPoint) o;
        return syncId.equals(syncPoint.syncId) && route.equals(syncPoint.route);
    }

    @Override
    public int hashCode()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString()
    {
        return "SyncPoint{" +
               "syncId=" + syncId +
               ", scope=" + route +
               '}';
    }
}
