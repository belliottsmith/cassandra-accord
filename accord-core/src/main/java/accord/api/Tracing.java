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

import java.util.MissingFormatArgumentException;

import accord.local.CommandStore;

public interface Tracing
{
    void trace(CommandStore commandStore, String message);
    default void done() {}
    default Tracing send() { return null; }

    default void trace(CommandStore commandStore, String fmt, Object ... args)
    {
        trace(commandStore, safeFormat(fmt, args));
    }

    static String safeFormat(String fmt, Object ... args)
    {
        StringBuilder out = new StringBuilder();
        int prev = 0;
        int argIndex = 0;
        while (true)
        {
            int next = fmt.indexOf('%', prev);
            if (next < 0)
                break;

            out.append(fmt, prev, next);
            if (++next == fmt.length())
                throw new IllegalArgumentException("Invalid substitution declaration: % not followed by d, s or %");

            char ch = fmt.charAt(next);
            prev = next + 1;

            if (ch == '%')
            {
                out.append('%');
                continue;
            }

            if (ch != 's' && ch != 'd')
                throw new IllegalArgumentException("Invalid substitution declaration: % not followed by d, s or %");

            if (argIndex >= args.length)
                throw new MissingFormatArgumentException("At least " + (argIndex + 1) + " format specifiers, but only " + args.length + " provided");

            Object arg = args[argIndex++];
            if (arg == null)
            {
                out.append("null");
                continue;
            }

            try
            {
                if (arg instanceof Throwable)
                    arg = format((Throwable) arg);
                out.append(arg);
            }
            catch (Throwable t)
            {
                try
                {
                    out.append("<Could not invoke toString(): ").append(format(t)).append('>');
                }
                catch (Throwable t2)
                {
                    out.append("<Could not invoke toString() on argument ").append(argIndex).append('>');
                }
            }
        }

        if (prev == 0)
            return fmt;

        out.append(fmt, prev, fmt.length());
        return out.toString();
    }

    static String format(Throwable failure)
    {
        StackTraceElement[] ste = failure.getStackTrace();
        return failure.getClass().getSimpleName() + ':' + failure.getLocalizedMessage()
               + (ste.length > 0 ? " (@" + ste[0].getClassName() + '.' + ste[0].getMethodName() + ':' + ste[0].getLineNumber() + ')' : "");
    }
}
