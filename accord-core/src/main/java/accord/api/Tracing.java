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

import java.util.function.BiConsumer;

import javax.annotation.Nullable;

import accord.local.CommandStore;

public interface Tracing
{
    void trace(CommandStore store, String message);

    default void trace(CommandStore store, String fmt, Object ... args)
    {
        trace(store, safeFormat(fmt, args));
    }

    static String safeFormat(String fmt, Object ... args)
    {
        try
        {
            return String.format(fmt, args);
        }
        catch (Throwable t)
        {
            try
            {
                String thrown = format(t);
                StringBuilder argsStr = new StringBuilder();
                if (args == null) argsStr.append("null");
                else
                {
                    argsStr.append('[');
                    for (int i = 0 ; i < args.length ; i++)
                    {
                        if (i > 0) argsStr.append(',');
                        try { argsStr.append(args[i]); }
                        catch (Throwable t2) { argsStr.append("<Could not invoke toString(): ").append(format(t2)).append('>'); }
                    }
                    argsStr.append(']');
                }
                return "<Could not invoke String.format(\"" + fmt + "\", " + argsStr + "): " + thrown + '>';
            }
            catch (Throwable t2)
            {
                return "<Could not format string or failure info>";
            }
        }
    }

    static String format(Throwable failure)
    {
        StackTraceElement[] ste = failure.getStackTrace();
        return failure.getClass().getSimpleName() + ':' + failure.getLocalizedMessage()
               + (ste.length > 0 ? " (@" + ste[0].getClassName() + '.' + ste[0].getMethodName() + ':' + ste[0].getLineNumber() + ')' : "");
    }

    static <V> BiConsumer<V, Throwable> wrap(BiConsumer<V, Throwable> wrap, String context, @Nullable Tracing tracing)
    {
        if (tracing == null) return wrap;
        return (success, fail) -> {
            if (fail != null) tracing.trace(null, "Failure when %s: %s", context, format(fail));
            wrap.accept(success, fail);
        };
    }
}
