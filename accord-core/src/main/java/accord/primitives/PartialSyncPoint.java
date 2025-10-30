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
public class PartialSyncPoint extends MinimalSyncPoint
{
    public static class SerializationSupport
    {
        public static PartialSyncPoint construct(TxnId syncId, Timestamp executeAt, RangeRoute route, FullRangeRoute fullRoute, Deps waitFor)
        {
            return new PartialSyncPoint(syncId, executeAt, route, fullRoute, waitFor);
        }
    }

    public final FullRangeRoute fullRoute;
    public final Deps waitFor;

    public PartialSyncPoint(TxnId syncId, Timestamp executeAt, RangeRoute route, FullRangeRoute fullRoute, Deps waitFor)
    {
        super(syncId, executeAt, route);
        this.fullRoute = fullRoute;
        this.waitFor = waitFor;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!super.equals(o)) return false;
        PartialSyncPoint syncPoint = (PartialSyncPoint) o;
        return waitFor.equals(syncPoint.waitFor);
    }

    public PartialSyncPoint without(AbstractRanges ranges)
    {
        RangeRoute route = this.route.without((Unseekables<?>) ranges);
        return new PartialSyncPoint(syncId, executeAt, route, fullRoute, waitFor);
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "{" +
               "syncId=" + syncId +
               ", scope=" + route +
               ", waitFor=" + waitFor +
               '}';
    }
}
