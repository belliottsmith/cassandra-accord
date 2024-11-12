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

public class Base62
{
    private static final long OVERFLOWL = Long.MAX_VALUE / 62;
    public static String encode(long value)
    {
        if (value == 0) return "0";
        StringBuilder sb = new StringBuilder();
        while (value > 0)
        {
            int next = (int) (value % 62);
            char c;
            if (next < 10) c = (char) ('0' + next);
            else if (next < 36) c = (char) ('a' + next);
            else c = (char) ('A' + next);
            sb.append(c);
            value /= 62;
        }
        return sb.toString();
    }

    public static long decodeLong(String value)
    {
        long result = 0;
        for (int i = 0; i < value.length(); i++)
        {
            if (result >= OVERFLOWL)
                throw new IllegalArgumentException("base62 string is too large for long: " + value);
            result *= 62;
            char c = value.charAt(i);
            if (c >= '0' && c <= '9') result += c - '0';
            else if (c >= 'a' && c <= 'z') result += c - 'a' + 10;
            else if (c >= 'A' && c <= 'Z') result += c - 'A' + 36;
            else throw new IllegalArgumentException("Invalid base62 string: " + value);
        }
        return result;
    }
}
