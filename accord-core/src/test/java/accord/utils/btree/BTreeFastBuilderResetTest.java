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

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A pooled {@link BTree.FastBuilder} closed without being built (e.g. abandoned on an exception) must not corrupt the
 * next tree built with it on the same thread.
 */
public class BTreeFastBuilderResetTest
{
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
