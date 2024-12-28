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
// Protocol Buffers - Google's data interchange format
// Copyright 2008 Google Inc.  All rights reserved.
// https://developers.google.com/protocol-buffers/
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//     * Neither the name of Google Inc. nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
package accord.utils;

import java.nio.ByteBuffer;

import static accord.utils.BitUtils.readLeastSignificantBytes;
import static accord.utils.BitUtils.writeMostSignificantBytes;

/**
 * Borrows idea from
 * https://developers.google.com/protocol-buffers/docs/encoding#varints
 */
public class VIntCoding
{
    /**
     * Throw when attempting to decode a vint and the output type
     * doesn't have enough space to fit the value that was decoded
     */
    public static class VIntOutOfRangeException extends RuntimeException
    {
        public final long value;

        private VIntOutOfRangeException(long value)
        {
            super(value + " is out of range for a 32-bit integer");
            this.value = value;
        }
    }

    public static long readUnsignedVInt(ByteBuffer in)
    {
        byte firstByte = in.get();
        if (firstByte >= 0)
            return firstByte;

        int extraBytes = VIntCoding.numberOfExtraBytesToRead(firstByte);
        long retval = readLeastSignificantBytes(extraBytes, in);
        // remove the non-value bits from the first byte
        firstByte &= VIntCoding.firstByteValueMask(extraBytes);
        // shift the first byte up to its correct position
        retval |= (long) firstByte << (extraBytes * 8);
        return retval;

    }

    /**
     * Computes size of an unsigned vint that starts at readerIndex of the provided ByteBuf.
     *
     * @return -1 if there are not enough bytes in the input to calculate the size; else, the vint unsigned value size in bytes.
     */
    public static int sizeOfUnsignedVInt(ByteBuffer input, int readerIndex)
    {
        return sizeOfUnsignedVInt(input, readerIndex, input.limit());
    }
    public static int sizeOfUnsignedVInt(ByteBuffer input, int readerIndex, int readerLimit)
    {
        if (readerIndex >= readerLimit)
            return -1;

        int firstByte = input.get(readerIndex);
        return 1 + ((firstByte >= 0) ? 0 : numberOfExtraBytesToRead(firstByte));
    }

    public static long readVInt(ByteBuffer input)
    {
        return decodeZigZag64(readUnsignedVInt(input));
    }

    /**
     * Read up to a 32-bit integer.
     *
     * This method assumes the original integer was written using {@link #writeUnsignedVInt32(int, ByteBuffer)}
     * or similar that doesn't zigzag encodes the vint.
     *
     * @throws VIntOutOfRangeException If the vint doesn't fit into a 32-bit integer
     */
    public static int readUnsignedVInt32(ByteBuffer input)
    {
        return checkedCast(readUnsignedVInt(input));
    }

    // & this with the first byte to give the value part for a given extraBytesToRead encoded in the byte
    public static int firstByteValueMask(int extraBytesToRead)
    {
        // by including the known 0bit in the mask, we can use this for encodeExtraBytesToRead
        return 0xff >> extraBytesToRead;
    }

    public static int encodeExtraBytesToRead(int extraBytesToRead)
    {
        // because we have an extra bit in the value mask, we just need to invert it
        return ~firstByteValueMask(extraBytesToRead);
    }

    public static int numberOfExtraBytesToRead(int firstByte)
    {
        // we count number of set upper bits; so if we simply invert all of the bits, we're golden
        // this is aided by the fact that we only work with negative numbers, so when upcast to an int all
        // of the new upper bits are also set, so by inverting we set all of them to zero
        return Integer.numberOfLeadingZeros(~firstByte) - 24;
    }

    public static void writeUnsignedVInt(long value, ByteBuffer output)
    {
        int size = VIntCoding.sizeOfUnsignedVInt(value);
        if (size == 1)
        {
            output.put((byte)value);
        }
        else if (size < 9)
        {
            int shift = (8 - size) << 3;
            int extraBytes = size - 1;
            long mask = (long)VIntCoding.encodeExtraBytesToRead(extraBytes) << 56;
            long register = (value << shift) | mask;
            writeMostSignificantBytes(register, size, output);
        }
        else
        {
            output.put((byte)0xff);
            output.putLong(value);
        }
    }

    public static void writeUnsignedVInt32(int value, ByteBuffer output)
    {
        writeUnsignedVInt((long)value, output);
    }

    public static void writeVInt(long value, ByteBuffer output)
    {
        writeUnsignedVInt(encodeZigZag64(value), output);
    }

    public static void writeVInt32(int value, ByteBuffer output)
    {
        writeVInt((long)value, output);
    }

    /**
     * Decode a ZigZag-encoded 64-bit value.  ZigZag encodes signed integers
     * into values that can be efficiently encoded with varint.  (Otherwise,
     * negative values must be sign-extended to 64 bits to be varint encoded,
     * thus always taking 10 bytes on the wire.)
     *
     * @param n An unsigned 64-bit integer, stored in a signed int because
     *          Java has no explicit unsigned support.
     * @return A signed 64-bit integer.
     */
    public static long decodeZigZag64(final long n)
    {
        return (n >>> 1) ^ -(n & 1);
    }

    /**
     * Encode a ZigZag-encoded 64-bit value.  ZigZag encodes signed integers
     * into values that can be efficiently encoded with varint.  (Otherwise,
     * negative values must be sign-extended to 64 bits to be varint encoded,
     * thus always taking 10 bytes on the wire.)
     *
     * @param n A signed 64-bit integer.
     * @return An unsigned 64-bit integer, stored in a signed int because
     *         Java has no explicit unsigned support.
     */
    public static long encodeZigZag64(final long n)
    {
        // Note:  the right-shift must be arithmetic
        return (n << 1) ^ (n >> 63);
    }

    /** Compute the number of bytes that would be needed to encode a varint. */
    public static int sizeOfVInt(final long param)
    {
        return sizeOfUnsignedVInt(encodeZigZag64(param));
    }

    /** Compute the number of bytes that would be needed to encode an unsigned varint. */
    public static int sizeOfUnsignedVInt(final long value)
    {
        int magnitude = Long.numberOfLeadingZeros(value | 1); // | with 1 to ensure magntiude <= 63, so (63 - 1) / 7 <= 8
        // the formula below is hand-picked to match the original 9 - ((magnitude - 1) / 7)
        return (639 - magnitude * 9) >> 6;
    }

    public static int checkedCast(long value)
    {
        int result = (int)value;
        if ((long)result != value)
            throw new VIntOutOfRangeException(value);
        return result;
    }
}
