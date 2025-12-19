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

package accord.impl.cfr;

import java.util.Map;

import accord.local.Command;
import accord.local.CommandSummaries;
import accord.local.CommandSummaries.Summary;
import accord.local.CommandSummaries.SummaryLoader;
import accord.primitives.Timestamp;
import accord.primitives.Unseekables;

import static accord.local.CommandSummaries.Relevance.MAYBE_SUPERSEDING;

public class LoadListener extends Listener
{
    final SummaryLoader loader;
    final Map<Timestamp, Summary> into;

    public LoadListener(SummaryLoader loader, Map<Timestamp, Summary> into)
    {
        this.loader = loader;
        this.into = into;
    }

    public SummaryLoader loader()
    {
        return loader;
    }

    public Map<Timestamp, Summary> into()
    {
        return into;
    }

    @Override
    protected Unseekables<?> participants()
    {
        return loader.participants();
    }

    @Override
    protected void accept(Command command)
    {
        CommandSummaries.Relevance relevance = loader.relevance(command);
        if (relevance.is(MAYBE_SUPERSEDING))
        {
            Summary summary = loader.get(relevance, command);
            if (summary != null)
                into.put(command.txnId(), summary);
        }
    }
}
