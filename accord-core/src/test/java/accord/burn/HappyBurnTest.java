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

package accord.burn;

import org.junit.jupiter.api.Test;

/**
 * A burn test of a healthy cluster: reliable delivery with stable latencies, and no partitions, restarts or topology
 * changes, across three DCs by default. Intended to model the common case, e.g. for evaluating optimisations whose
 * benefit is primarily latency or efficiency (such as backlog execution), rather than for finding faults.
 * Accepts the same arguments as {@link BurnTest}.
 */
public class HappyBurnTest extends BurnTestBase
{
    static void configure()
    {
        System.setProperty("accord.burn.happy", "true");
        if (System.getProperty("accord.burn.dcs") == null)
            System.setProperty("accord.burn.dcs", "3");
    }

    @Test
    public void testOne()
    {
        configure();
        run(System.nanoTime());
    }

    public static void main(String[] args)
    {
        configure();
        BurnTest.main(args);
    }
}
