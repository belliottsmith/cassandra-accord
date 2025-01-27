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

import javax.annotation.Nonnull;

import accord.utils.SortedList.MergeCursor;

public class MergeFewDisjointSortedListsCursor<T extends Comparable<? super T>, L extends SortedList<? extends T>> implements MergeCursor<T, L>
{
    // Holds and is comparable by the head item of an iterator it owns
    public static final class Candidate<T extends Comparable<? super T>, L extends SortedList<? extends T>> implements Comparable<Candidate<T, L>>, MergeCursor.Entry<L>
    {
        private final L list;
        private int itemIdx;
        private T item;

        public Candidate(@Nonnull L list)
        {
            Invariants.require(!list.isEmpty());
            this.list = list;
            this.item = list.get(0);
        }

        /** @return this if our iterator had an item, and it is now available, otherwise null */
        private Candidate<T, L> advance()
        {
            if (++itemIdx >= list.size())
                return null;

            item = list.get(itemIdx);
            return this;
        }

        private static final int FOUND = 1;
        private static final int ADVANCED = 2;

        private int find(Comparable<? super T> find)
        {
            int result = 0;
            int i = list.findNext(itemIdx, find);
            if (i >= 0) result = FOUND;
            if (i < 0) i = -1 - i;
            if (i > itemIdx)
            {
                result |= ADVANCED;
                if ((itemIdx = i) < list.size())
                    item = list.get(i);
                else
                    item = null;
            }
            return result;
        }

        public L list()
        {
            return list;
        }

        public int index()
        {
            return itemIdx;
        }

        @Override
        public String toString()
        {
            return list.toString();
        }

        public int compareTo(Candidate<T, L> that)
        {
            return this.item.compareTo(that.item);
        }
    }

    Candidate<T, L>[] heap;
    int size = 0;
    public MergeFewDisjointSortedListsCursor(int capacity)
    {
        heap = new Candidate[capacity];
    }

    public void add(L list)
    {
        heap[size++] = new Candidate<>(list);
    }

    @Override
    public boolean hasCur()
    {
        return size > 0;
    }

    @Override
    public T cur()
    {
        return heap[0].item;
    }

    public MergeCursor.Entry<L> curEntry()
    {
        return heap[0];
    }

    public void advance()
    {
        Candidate<T, L> sink = heap[0].advance();
        if (sink == null) replaceHead();
        else siftDown(heap[0], 0);
    }

    public void init()
    {
        for (int i = size - 2 ; i >= 0 ; --i)
            siftDown(heap[i], i);
    }

    @Override
    public boolean find(Comparable<? super T> find)
    {
        if (size == 0)
            return false;

        Candidate<T, L> found = null;
        while (true)
        {
            Candidate<T, L> head = heap[0];
            if (head == found)
                return true;

            int result = head.find(find);
            if (0 != (result & Candidate.FOUND))
            {
                Invariants.require(found == null, "%s and %s both contained %s", head, found, find);
                found = head;
            }

            if (0 == (result & Candidate.ADVANCED))
                return found != null;

            if (head.item == null)
            {
                Invariants.require(head != found);
                replaceHead();
                if (size == 0)
                    return false;
            }
            else
            {
                siftDown(head, 0);
            }
        }
    }

    void replaceHead()
    {
        for (int i = 1 ; i < size ; ++i)
            heap[i - 1] = heap[i];
        heap[--size] = null;
    }

    private void siftDown(Candidate<T, L> node, int i)
    {
        int j = i;
        while (++j < size)
        {
            if (node.compareTo(heap[j]) < 0)
                break;
        }

        if (--j == i)
            return;

        for (; i < j ; ++i)
            heap[i] = heap[i + 1];
        heap[i] = node;
    }
}