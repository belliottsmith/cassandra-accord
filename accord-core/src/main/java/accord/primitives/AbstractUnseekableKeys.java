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

import accord.api.RoutingKey;

import java.util.Arrays;

// TODO: do we need this class?
public abstract class AbstractUnseekableKeys<KS extends Unseekables<RoutingKey, ?>> extends AbstractKeys<RoutingKey, KS> implements Iterable<RoutingKey>, Unseekables<RoutingKey, KS>
{
    AbstractUnseekableKeys(RoutingKey[] keys)
    {
        super(keys);
    }

    @Override
    public final int indexOf(RoutingKey key)
    {
        return Arrays.binarySearch(keys, key);
    }

    @Override
    public Unseekables<RoutingKey, ?> subtract(Ranges ranges)
    {
        RoutingKey[] output = subtract(ranges, RoutingKey[]::new);
        return output == keys ? this : new RoutingKeys(output);
    }
}
