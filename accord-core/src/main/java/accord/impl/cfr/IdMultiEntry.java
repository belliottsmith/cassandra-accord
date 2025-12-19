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

package accord.impl.cfr;

import accord.primitives.Ranges;
import accord.primitives.TxnId;

public class IdMultiEntry extends IdEntry
{
    final Ranges ranges;

    public IdMultiEntry(TxnId txnId, Ranges ranges)
    {
        super(txnId);
        this.ranges = ranges;
    }

    @Override
    IdEntry copy()
    {
        IdMultiEntry copy = new IdMultiEntry(this, ranges);
        copy.encoded = encoded;
        return copy;
    }

    @Override
    Ranges ranges()
    {
        return ranges;
    }
}
