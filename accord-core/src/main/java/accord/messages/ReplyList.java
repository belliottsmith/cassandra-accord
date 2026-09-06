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

package accord.messages;

import java.util.ArrayList;
import java.util.function.BiConsumer;

import accord.utils.Reduce;
import accord.utils.async.AsyncResults;
import accord.utils.async.CancellableAsyncResult;

public interface ReplyList<R extends CancellableAsyncResult<R> & ReplyList<R>>
{
    int size();
    CancellableAsyncResult<R> get(int i);
    void cancelReplies();

    final class ReplyMultiList<R extends CancellableAsyncResult<R> & ReplyList<R>> extends ArrayList<CancellableAsyncResult<R>> implements ReplyList<R>
    {
        public ReplyMultiList() {}
        public ReplyMultiList(int initialCapacity) { super(initialCapacity); }

        @Override
        public void cancelReplies()
        {
            forEach(CancellableAsyncResult::cancel);
        }
    }

    // parameters must be EITHER a ReplyMultiList<R> OR a single R (that itself implements both ReplyList<R> and AsyncResult<R>)
    static <R extends CancellableAsyncResult<R> & ReplyList<R>> ReplyList<R> merge(ReplyList<R> r1, ReplyList<R> r2)
    {
        if (r1 == null || r2 == null)
        {
            if (r1 != null) r1.cancelReplies();
            if (r2 != null) r2.cancelReplies();
            return null;
        }

        ReplyMultiList<R> multi;
        if (r1 instanceof ReplyMultiList)
        {
            multi = (ReplyMultiList<R>) r1;
            if (r2 instanceof ReplyMultiList)
            {
                multi.addAll((ReplyMultiList<R>)r2);
                return multi;
            }
            multi.add((CancellableAsyncResult<R>) r2);
        }
        else if (r2 instanceof ReplyMultiList)
        {
            multi = (ReplyMultiList<R>) r2;
            multi.add((CancellableAsyncResult<R>) r1);
        }
        else
        {
            multi = new ReplyMultiList<>();
            multi.add((CancellableAsyncResult<R>) r1);
            multi.add((CancellableAsyncResult<R>) r2);
        }
        return multi;
    }

    static <R extends CancellableAsyncResult<R> & ReplyList<R>> void invoke(ReplyList<R> replies, Reduce<R, R> reduce, BiConsumer<? super R, Throwable> callback)
    {
        if (replies instanceof ReplyMultiList<?>)
        {
            AsyncResults.reduce((ReplyMultiList<R>)replies, reduce)
                        .invoke(callback);
        }
        else
        {
            ((R)replies).invoke(callback);
        }
    }
}
