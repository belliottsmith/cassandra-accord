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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * A pooled {@link BTree.FastBuilder} closed without being built (e.g. abandoned on an exception) must not corrupt the
 * next tree built with it on the same thread.
 */
public class BTreeFastBuilderResetTest
{
    /**
     * A transformer abandoned once it has propagated an unchanged child, but no key after it, retains nothing
     */
    @Test
    void abandonedTransformerReleasesRightChild() throws Exception
    {
        Integer[] values = new Integer[1000];
        for (int i = 0 ; i < values.length ; ++i)
            values[i] = i;
        Object[] tree = BTree.build(BulkIterator.of(values), values.length, UpdateFunction.noOp());
        // the first leaf is unchanged, so propagated whole to its parent, before we abort on the key that follows it
        Integer abortAt = (Integer) tree[0];
        assertThrows(UnsupportedOperationException.class, () -> BTree.transformAndFilter(tree, (Integer v) -> {
            if (v.equals(abortAt)) throw new UnsupportedOperationException();
            return v;
        }));

        Field poolField = Class.forName(BTree.class.getName() + "$Transformer").getDeclaredField("POOL");
        poolField.setAccessible(true);
        TinyThreadLocalPool.TinyPool<Object> pool = ((TinyThreadLocalPool<Object>) poolField.get(null)).get();
        BTree.LeafOrBranchBuilder builder = (BTree.LeafOrBranchBuilder) pool.poll();
        try
        {
            for (BTree.LeafOrBranchBuilder level = builder ; level != null ; level = level.parent)
            {
                assertNull(level.sourceNode);
                assertNull(level.savedNextKey);
                for (Object[] buffer : new Object[][] { level.buffer, level.savedBuffer })
                {
                    for (int i = 0 ; buffer != null && i < buffer.length ; ++i)
                        assertNull(buffer[i], "height " + level.height + " retains " + i);
                }
            }
        }
        finally
        {
            pool.offer(builder);
        }
    }

    @Test
    void abandonedBuilderIsReset()
    {
        for (int abandoned : new int[] { 1, 5, BTree.MAX_KEYS, BTree.MAX_KEYS + 1, 100 })
        {
            try (BTree.FastBuilder<Integer> builder = BTree.fastBuilder())
            {
                for (int i = 0 ; i < abandoned ; ++i)
                    builder.add(i);
            }

            for (int size : new int[] { 1, 18, BTree.MAX_KEYS, 100 })
            {
                Object[] tree;
                try (BTree.FastBuilder<Integer> builder = BTree.fastBuilder())
                {
                    for (int i = 0 ; i < size ; ++i)
                        builder.add(i);
                    tree = builder.build();
                }

                List<Integer> contents = new ArrayList<>();
                for (Integer i : BTree.<Integer>iterable(tree))
                    contents.add(i);
                List<Integer> expected = new ArrayList<>();
                for (int i = 0 ; i < size ; ++i)
                    expected.add(i);
                assertEquals(expected, contents, "after abandoning a builder of " + abandoned);
            }
        }
    }
}
