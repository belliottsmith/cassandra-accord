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

package accord.utils.btree;

import java.util.Comparator;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * {@link BTree#update} may swap its inputs, flipping the update function so that its result is unchanged
 */
public class BTreeUpdateFunctionTest
{
    private static Object[] tree(int from, int to)
    {
        Integer[] values = new Integer[to - from];
        for (int i = from ; i < to ; ++i)
            values[i - from] = i;
        return BTree.build(BulkIterator.of(values), values.length, UpdateFunction.noOp());
    }

    @Test
    void rejectUpdatesRejectsWhenFlipped()
    {
        // both leaves, insert larger than the tree we update
        assertThrows(IllegalStateException.class, () -> BTree.update(tree(5, 6), tree(0, 10), Comparator.<Integer>naturalOrder(), UpdateFunction.rejectUpdates()));
        // insert much larger than the tree we update
        assertThrows(IllegalStateException.class, () -> BTree.update(tree(5, 6), tree(0, 1000), Comparator.<Integer>naturalOrder(), UpdateFunction.rejectUpdates()));
    }

    @Test
    void flippedNoOpRetainsExisting()
    {
        Integer existing = 1000, inserted = 1000;
        Object[] small = BTree.singleton(existing);
        for (int size : new int[] { 10, 2000 })
        {
            Object[] large = BTree.update(tree(0, size), BTree.singleton(inserted), Comparator.<Integer>naturalOrder(), UpdateFunction.noOpReplace());
            Object[] noOp = BTree.update(small, large, Comparator.<Integer>naturalOrder(), UpdateFunction.noOp());
            assertSame(existing, BTree.find(noOp, Comparator.<Integer>naturalOrder(), 1000));
            Object[] replace = BTree.update(small, large, Comparator.<Integer>naturalOrder(), UpdateFunction.noOpReplace());
            assertSame(inserted, BTree.find(replace, Comparator.<Integer>naturalOrder(), 1000));
            assertEquals(BTree.size(large), BTree.size(noOp));
        }
    }
}
