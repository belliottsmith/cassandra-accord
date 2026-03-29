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

package accord.local;

import java.util.function.Function;
import javax.annotation.Nullable;

import accord.primitives.Participants;
import accord.primitives.TxnId;
import accord.utils.MapReduceConsume;
import accord.utils.async.AsyncChain;

public abstract class MapReduceConsumeCommandStores<P extends Participants<?>, O> extends MapReduceCommandStores<P, O> implements MapReduceConsume<SafeCommandStore, O>
{
    protected MapReduceConsumeCommandStores(P scope)
    {
        super(scope);
    }

    public MapReduceConsumeCommandStores<P, O> overrideWithSynchronousApply(Function<? super CommandStore, AsyncChain<O>> apply)
    {
        return new Delegate<>(this)
        {
            @Override
            protected AsyncChain<O> applyAsyncInternal(CommandStore commandStore)
            {
                return apply.apply(commandStore);
            }

            @Override
            protected O applyInternal(SafeCommandStore safeStore)
            {
                throw new IllegalStateException();
            }
        };
    }

    public static class Delegate<P extends Participants<?>, O> extends MapReduceConsumeCommandStores<P, O>
    {
        final MapReduceConsumeCommandStores<P, O> delegate;

        public Delegate(MapReduceConsumeCommandStores<P, O> delegate)
        {
            super(delegate.scope);
            this.delegate = delegate;
        }

        @Override
        protected O applyInternal(SafeCommandStore safeStore)
        {
            return delegate.applyInternal(safeStore);
        }

        @Nullable
        @Override
        public TxnId primaryTxnId()
        {
            return delegate.primaryTxnId();
        }

        @Override
        public String reason()
        {
            return delegate.reason();
        }

        @Override
        public void accept(O result, Throwable failure)
        {
            delegate.accept(result, failure);
        }

        @Override
        public O reduce(O o1, O o2)
        {
            return delegate.reduce(o1, o2);
        }
    }
}
