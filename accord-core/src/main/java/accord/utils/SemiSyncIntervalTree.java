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

package accord.utils;

import java.util.List;

import accord.utils.btree.IntervalBTree;

public abstract class SemiSyncIntervalTree<E> extends SemiSyncCollection<Object[], E, Object[]>
{
    final IntervalBTree.IntervalComparators<?> comparators;

    protected SemiSyncIntervalTree(IntervalBTree.IntervalComparators<?> comparators)
    {
        super(IntervalBTree.empty());
        this.comparators = comparators;
    }

    protected abstract Object[] tree(E edit);

    protected void pushEdit(Object group, E add, E remove)
    {
        pushEdit(new Edit<>(group, add, remove));
    }

    protected Object[] merge(List<E> es)
    {
        if (es.isEmpty())
            return IntervalBTree.empty();
        Object[] v = tree(es.get(0));
        for (int i = 1; i < es.size() ; ++i)
            v = IntervalBTree.update(v, tree(es.get(i)), comparators);
        return v;
    }

    protected Object[] applyOne(Object[] value, E add, E remove)
    {
        return applyMultiple(value, add == null ? null : tree(add), remove == null ? null : tree(remove));
    }

    protected Object[] applyMultiple(Object[] value, Object[] add, Object[] remove)
    {
        Object[] result = value;
        if (remove != null)
            result = IntervalBTree.subtract(value, remove, comparators);
        if (add != null)
            return IntervalBTree.update(result, add, comparators);
        return result;
    }
}
