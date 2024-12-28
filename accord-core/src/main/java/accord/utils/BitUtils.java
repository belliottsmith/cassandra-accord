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

import java.nio.ByteBuffer;

import net.nicoulaj.compilecommand.annotations.DontInline;

public class BitUtils
{
    public static int numberOfBitsToRepresent(long value)
    {
        return 64 - Long.numberOfLeadingZeros(value);
    }

    public static int numberOfBitsToRepresent(int value)
    {
        return 32 - Integer.numberOfLeadingZeros(value);
    }

    public static final class BitReader
    {
        private long bitBuffer;
        private int bitCount;

        public long read(int readCount, ByteBuffer in)
        {
            if (readCount == 0)
                return 0;

            if (readCount == 64 && bitCount == 0)
                return in.getLong();

            long result = bitBuffer >>> (64 - readCount);
            int remaining = bitCount - readCount;
            if (remaining >= 0)
            {
                bitBuffer <<= readCount;
                bitCount = remaining;
            }
            else if (in.remaining() >= 8)
            {
                readCount -= bitCount;
                bitBuffer = in.getLong();
                bitCount = 64 - readCount;
                result |= (bitBuffer >>> bitCount);
                bitBuffer <<= readCount;
            }
            else
            {
                readCount -= bitCount;
                while (readCount > 8)
                {
                    long next = in.get() & 0xff;
                    readCount -= 8;
                    result |= next << readCount;
                }
                long next = in.get() & 0xff;
                bitCount = 8 - readCount;
                result |= next >>> bitCount;
                bitBuffer = next << (64 - bitCount);
            }
            return result;
        }
    }

    public static long flushBits(long buffer, int bufferCount, long add, int addCount, ByteBuffer out)
    {
        Invariants.checkArgument(addCount == 64 || 0 == (add & (-1L << addCount)));
        int total = bufferCount + addCount;
        if (total < 64)
        {
            return buffer | (add << 64 - total);
        }
        else
        {
            buffer |= add >>> total - 64;
            out.putLong(buffer);
            return total == 64 ? 0 : (add << (128 - total));
        }
    }

    public static void writeLeastSignificantBytes(long register, int bytes, ByteBuffer out)
    {
        writeMostSignificantBytes(register << ((8 - bytes)*8), bytes, out);
    }

    // TODO (expected): this is probably endian-sensitive
    public static void writeMostSignificantBytes(long register, int bytes, ByteBuffer out)
    {
        int position = out.position();
        int limit = out.limit();
        if (limit - position < Long.BYTES)
        {
            writeMostSignificantBytesSlow(register, bytes, out);
        }
        else
        {
            out.putLong(position, register);
            out.position(position + bytes);
        }
    }

    @DontInline
    private static void writeMostSignificantBytesSlow(long register, int bytes, ByteBuffer out)
    {
        switch (bytes)
        {
            case 0:
                break;
            case 1:
                out.put((byte)(register >>> 56));
                break;
            case 2:
                out.putShort((short)(register >> 48));
                break;
            case 3:
                out.putShort((short)(register >> 48));
                out.put((byte)(register >> 40));
                break;
            case 4:
                out.putInt((int)(register >> 32));
                break;
            case 5:
                out.putInt((int)(register >> 32));
                out.put((byte)(register >> 24));
                break;
            case 6:
                out.putInt((int)(register >> 32));
                out.putShort((short)(register >> 16));
                break;
            case 7:
                out.putInt((int)(register >> 32));
                out.putShort((short)(register >> 16));
                out.put((byte)(register >> 8));
                break;
            case 8:
                out.putLong(register);
                break;
            default:
                throw new IllegalArgumentException();
        }
    }

    public static long readLeastSignificantBytes(int bytes, ByteBuffer in)
    {
        if (bytes == 0)
            return 0L;

        int position = in.position();
        int limit = in.limit();
        if (limit - position < Long.BYTES)
        {
            return readLeastSignificantBytesSlow(bytes, in);
        }
        else
        {
            long result = in.getLong(position);
            in.position(position + bytes);
            return result >>> (64 - 8*bytes);
        }
    }

    @DontInline
    private static long readLeastSignificantBytesSlow(int bytes, ByteBuffer out)
    {
        switch (bytes)
        {
            case 0: return 0;
            case 1: return out.get() & 0xffL;
            case 2: return out.getShort() & 0xffffL;
            case 3: return ((out.getShort() & 0xffffL) << 8) | (out.get() & 0xffL);
            case 4: return out.getInt() & 0xffffffffL;
            case 5: return ((out.getInt() & 0xffffffffL) << 8) | (out.get() & 0xffL);
            case 6: return ((out.getInt() & 0xffffffffL) << 16) | (out.getShort() & 0xffffL);
            case 7: return ((out.getInt() & 0xffffffffL) << 24) | ((out.getShort() & 0xffffL) << 8) | (out.get() & 0xffL);
            case 8: return out.getLong();
            default: throw new IllegalArgumentException();
        }
    }
}
