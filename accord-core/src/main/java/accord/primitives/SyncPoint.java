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
public class SyncPoint<U extends Unseekable>
{
    public static class SerializationSupport
    {
        public static SyncPoint construct(TxnId syncId, Timestamp executeAt, Deps waitFor, FullRoute<?> route)
        {
            return new SyncPoint(syncId, executeAt, waitFor, route);
        }
    }

    public final TxnId syncId;
    public final Timestamp executeAt;
    public final Deps waitFor;
    public final FullRoute<U> route;

    public SyncPoint(TxnId syncId, Timestamp executeAt, Deps waitFor, FullRoute<U> route)
    {
        this.syncId = syncId;
        this.executeAt = executeAt;
        this.waitFor = waitFor;
        this.route = route;
    }

    public FullRoute<U> route()
    {
        return route;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SyncPoint<?> syncPoint = (SyncPoint<?>) o;
        return syncId.equals(syncPoint.syncId) && waitFor.equals(syncPoint.waitFor) && route.equals(syncPoint.route);
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
               ", waitFor=" + waitFor +
               '}';
    }
}
