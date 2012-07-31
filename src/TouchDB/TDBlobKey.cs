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

namespace TouchDB
{
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
			var buf = new StringBuilder();
			for (int i = 0; i < data.Length; i++)
			{
				var halfbyte = (data[i] >> 4) & 0x0F;
				int twoHalfs = 0;
				do
				{
					if ((0 <= halfbyte) && (halfbyte <= 9))
						buf.Append((char)('0' + halfbyte));
					else
						buf.Append((char)('a' + (halfbyte - 10)));
					halfbyte = data[i] & 0x0F;
				} while (twoHalfs++ < 1);
			}
			return buf.ToString();
		}

		public static byte[] ConvertFromHex(string s)
		{
			var len = s.Length;
			var data = new byte[len / 2];
			for (int i = 0; i < len; i += 2)
			{
				data[i / 2] = (byte)((Convert.ToInt32(s[i].ToString(), 16) << 4) + 
										Convert.ToInt32(s[i + 1].ToString(), 16));
			}
			return data;
		}

		public override bool Equals(object obj)
		{
			if(!(obj is TDBlobKey)) {
				return false;
			}

			return Array.Equals(Bytes, ((TDBlobKey)obj).Bytes);			
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
