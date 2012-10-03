/**
 * Original iOS version by Jens Alfke
 * Java port by Marty Schoch
 * Ported to C# by John Zablocki
 *
 * Copyright (c) 2012 Couchbase, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Security.Cryptography;

namespace TouchDB.Mono
{
    //TODO: This class shouldn't exist!
    public static class TDMisc
    {
        public static string TDCreateUUID()
        {
            return Guid.NewGuid().ToString().Replace("-", "");
        }

        public static string TDHexSha1Digest(byte[] input)
        {
            var sha1 = new SHA1Managed();
            var bytes = sha1.ComputeHash(input);

            return ConvertToHex(bytes);
        }

        public static string ConvertToHex(byte[] data)
        {
            //TODO: Refactor
            return BitConverter.ToString(data).Replace("-", "").ToLower();
        }

        public static int TDSequenceCompare(long a, long b)
        {
            var diff = a - b;
            return diff > 0 ? 1 : (diff < 0 ? -1 : 0);
        }
    }
}
