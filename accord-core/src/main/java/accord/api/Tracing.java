/*
 * Licensed to the Apache Software ation (ASF) under one
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
package accord.api;

import java.util.Arrays;

import accord.local.CommandStore;

public interface Tracing
{
    void trace(CommandStore store, String message);

    default void trace(CommandStore store, String fmt, Object ... args)
    {
        String message;
        try { message = String.format(fmt, args); }
        catch (Throwable t) { message = "Could not format \"" + fmt + "\" with " + Arrays.toString(args) + " (" + t.getLocalizedMessage() + ")"; }
        trace(store, message);
    }

    static String format(Throwable failure)
    {
        StackTraceElement[] ste = failure.getStackTrace();
        return failure.getClass().getSimpleName() + ":" + failure.getLocalizedMessage()
               + (ste.length > 0 ? " (@" + ste[0].getClassName() + "." + ste[0].getMethodName() + ":" + ste[0].getLineNumber() + ")" : "");
    }
}
