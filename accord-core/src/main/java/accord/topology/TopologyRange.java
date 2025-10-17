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

package accord.topology;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Consumer;

import accord.utils.Invariants;

import static java.util.stream.Collectors.joining;

public class TopologyRange
{
    public final long min;
    public final long current;
    public final long firstNonEmpty;
    public final List<Topology> topologies;

    public TopologyRange(long min, long current, long firstNonEmpty, List<Topology> topologies)
    {
        this.min = min;
        this.current = current;
        this.topologies = topologies;
        this.firstNonEmpty = firstNonEmpty;
    }

    public void forEach(Consumer<Topology> forEach, long minEpoch, int count)
    {
        if (minEpoch == 0) // Bootstrap
            minEpoch = this.min;

        long emptyUpTo = firstNonEmpty == -1 ? current : firstNonEmpty - 1;
        // Report empty epochs
        for (long epoch = minEpoch; epoch <= emptyUpTo && count > 0; epoch++, count--)
            forEach.accept(new Topology(epoch));

        // Report known non-empty epochs
        for (int i = 0; i < topologies.size() && count > 0; i++, count--)
        {
            Topology topology = topologies.get(i);
            forEach.accept(topology);
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || getClass() != o.getClass()) return false;
        TopologyRange that = (TopologyRange) o;
        return min == that.min && current == that.current && firstNonEmpty == that.firstNonEmpty && Objects.equals(topologies, that.topologies);
    }

    public boolean hasEpoch(long epoch)
    {
        return epoch >= min && epoch <= current;
    }

    public Topology get(long epoch)
    {
        if (!hasEpoch(epoch))
            throw new NoSuchElementException();

        if (firstNonEmpty < 0 || epoch < firstNonEmpty)
            return new Topology(epoch);

        Invariants.require(!topologies.isEmpty());
        int index = (int) (topologies.get(0).epoch() - epoch);
        Invariants.require(index >= 0);
        return topologies.get(index);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(min, current, firstNonEmpty, topologies);
    }

    @Override
    public String toString()
    {
        return String.format("TopologyRange{min=%d, current=%d, firstNonEmpty=%d, topologies=[%s]}",
                             min,
                             current,
                             firstNonEmpty,
                             topologies.stream().map(t -> Long.toString(t.epoch())).collect(joining(",")));
    }
}
