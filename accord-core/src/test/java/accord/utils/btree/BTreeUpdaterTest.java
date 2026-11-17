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
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

public class BTreeUpdaterTest
{
    private static Object[] tree(int size)
    {
        Integer[] values = new Integer[size];
        for (int i = 0 ; i < size ; ++i)
            values[i] = i;
        return BTree.build(BulkIterator.of(values), values.length, UpdateFunction.noOp());
    }

    /**
     * An update that changes nothing returns the tree it updated, at every height
     */
    @Test
    void unchangedTreeIsReused()
    {
        for (int size : new int[] { 100, 2000, 70000 })
        {
            Object[] tree = tree(size);
            Object[] insert = BTree.singleton(size / 2);
            Object[] updated = BTree.update(tree, insert, Comparator.<Integer>naturalOrder(), UpdateFunction.noOp());
            assertSame(tree, updated, "size " + size);

            // but a change still produces a new root
            Object[] changed = BTree.update(tree, BTree.singleton(size), Comparator.<Integer>naturalOrder(), UpdateFunction.noOp());
            assertNotSame(tree, changed);
            assertEquals(size + 1, BTree.size(changed));
        }
    }
}
