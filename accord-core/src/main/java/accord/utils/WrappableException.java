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

public interface WrappableException<T extends Throwable & WrappableException<T>>
{
    /**
     * Create a new exception of the same type, keeping the original exception as a cause of the new exception,
     * so that we can track the current stack trace as well as the origin stack trace.
     */
    T wrap();

    static Throwable wrap(Throwable t)
    {
        if (t instanceof WrappableException<?>)
        {
            Throwable wrapped = ((WrappableException<?>)t).wrap();
            if (wrapped.getClass() != t.getClass())
            {
                IllegalStateException ise = new IllegalStateException("Wrapping should not change type");
                ise.addSuppressed(t);
                throw ise;
            }
            return wrapped;
        }
        else if (t instanceof AssertionError)
        {
            return new AssertionError(t);
        }
        else if (t instanceof OutOfMemoryError)
        {
            return t;
        }
        else if (t instanceof Error)
        {
            return new Error(t);
        }
        else
        {
            return new RuntimeException(t);
        }
    }
}
