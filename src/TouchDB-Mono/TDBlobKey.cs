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
using System.Globalization;

namespace TouchDB.Mono
{
	/// <summary>
	/// Key identifying a data blob. This happens to be a SHA-1 digest.
	/// </summary>
	public class TDBlobKey
	{
		public TDBlobKey()
		{
		}

		public TDBlobKey(byte[] bytes)
		{
			Bytes = bytes;
		}

		public byte[] Bytes { get; set; }

		public static string ConvertToHex(byte[] data)
		{
			return BitConverter.ToString(data).Replace("-", "").ToLower();
		}

		public static byte[] ConvertFromHex(string source)
		{
			var len = source.Length;
			var data = new byte[len / 2];
			for (var i = 0; i < len; i += 2)
			{
				data[i / 2] = (byte)((Convert.ToInt32(source[i].ToString(), 16) << 4)
									 + Convert.ToInt32(source[i + 1].ToString(), 16));
			}
			return data;
		}

		public override bool Equals(object obj)
		{
			if (! (obj is TDBlobKey))
				return false;

			return Bytes.Equals((obj as TDBlobKey).Bytes);
		}

		public override int GetHashCode()
		{
			return Bytes.GetHashCode();
		}

		public override string ToString()
		{
			return ConvertToHex(Bytes);
		}
	}
}
