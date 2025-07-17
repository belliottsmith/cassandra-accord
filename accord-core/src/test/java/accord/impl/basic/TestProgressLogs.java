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

package accord.impl.basic;

import javax.annotation.Nullable;

import accord.api.ProgressLog;
import accord.impl.progresslog.DefaultProgressLog;
import accord.local.CommandStore;
import accord.local.Node;
import accord.local.SafeCommandStore;

public class TestProgressLogs implements ProgressLog.Factory
{
    static class TestProgressLog extends DefaultProgressLog
    {
        protected TestProgressLog(Node node, CommandStore commandStore)
        {
            super(node, commandStore);
        }

        @Override
        public void accept(@Nullable SafeCommandStore safeStore)
        {
            Pending prev = Pending.Global.activeOrigin();
            Pending.Global.setNoActiveOrigin();
            try
            {
                super.accept(safeStore);
            }
            finally
            {
                if (prev != Pending.Global.NONE)
                {
                    Pending.Global.clearActiveOrigin();
                    Pending.Global.setActiveOrigin(prev);
                }
            }
        }
    }

    final Node node;
    public TestProgressLogs(Node node)
    {
        this.node = node;
    }

    @Override
    public DefaultProgressLog create(CommandStore commandStore)
    {
        return new TestProgressLog(node, commandStore);
    }
}
