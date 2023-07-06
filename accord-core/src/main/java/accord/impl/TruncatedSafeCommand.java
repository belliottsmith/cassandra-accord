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

package accord.impl;

import java.util.Collection;
import java.util.Collections;

import accord.local.Command;
import accord.local.SafeCommand;
import accord.local.SaveStatus;
import accord.primitives.TxnId;

import static accord.api.ProgressLog.ProgressShard.Unsure;
import static accord.local.Listeners.Immutable.EMPTY;

public class TruncatedSafeCommand extends SafeCommand
{
    final Command truncated;

    public TruncatedSafeCommand(TxnId txnId)
    {
        super(txnId);
        this.truncated = new Command.Truncated(txnId, SaveStatus.Truncated, null, null, EMPTY);
    }

    @Override
    public Command current()
    {
        return truncated;
    }

    @Override
    public void invalidate()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean invalidated()
    {
        return false;
    }

    @Override
    public void addListener(Command.TransientListener listener)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeListener(Command.TransientListener listener)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<Command.TransientListener> transientListeners()
    {
        return Collections.emptyList();
    }

    @Override
    protected void set(Command command)
    {
        throw new UnsupportedOperationException();
    }
}
