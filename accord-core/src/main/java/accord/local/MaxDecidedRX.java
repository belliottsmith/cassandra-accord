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
package accord.local;

import accord.api.RoutingKey;
import accord.primitives.Routables;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.utils.BTreeReducingRangeMap;

public class MaxDecidedRX extends BTreeReducingRangeMap<TxnId>
{
    public static final MaxDecidedRX EMPTY = new MaxDecidedRX();

    private MaxDecidedRX()
    {
        super();
    }

    private MaxDecidedRX(boolean inclusiveEnds, Object[] tree)
    {
        super(inclusiveEnds, tree);
    }

    TxnId get(Routables<?> keysOrRanges)
    {
        return foldl(keysOrRanges, TxnId::max, TxnId.NONE);
    }

    public MaxDecidedRX update(Unseekables<?> keysOrRanges, TxnId maxId)
    {
        // note: we use mergeMax to ensure we take the maximum epoch and hlc independently from any conflict
        //  this is particularly essential for propagating unique HLCs, so that bootstrap recipients don't
        //  begin serving reads too early
        return update(this, keysOrRanges, maxId, TxnId::max, MaxDecidedRX::new, Builder::new);
    }

    private static class Builder extends AbstractBoundariesBuilder<RoutingKey, TxnId, MaxDecidedRX>
    {
        protected Builder(boolean inclusiveEnds, int capacity)
        {
            super(inclusiveEnds, capacity);
        }

        @Override
        protected MaxDecidedRX buildInternal(Object[] tree)
        {
            return new MaxDecidedRX(inclusiveEnds, tree);
        }
    }
}
