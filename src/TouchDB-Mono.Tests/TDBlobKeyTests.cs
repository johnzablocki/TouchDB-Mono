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
using NUnit.Framework;

namespace TouchDB.Mono.Tests
{
	[TestFixture]
	public class TDBlobKeyTests
	{
		[Test]
		public void When_Converting_Byte_Array_To_Hex_Output_Matches_Java_Implementation()
		{
			var input = new byte[] { 87, 79, 87, 46, 46, 46 };
			var expected = "574f572e2e2e";
			var result = TDBlobKey.ConvertToHex(input);

			Assert.That(result, Is.StringMatching(expected));
		}

		[Test]
		public void When_Converting_Hex_String_To_Byte_Array_Output_Matches_Java_Implementation()
		{
			var expected = new byte[] { 87, 79, 87, 46, 46, 46 };
			var input = "574f572e2e2e";
			var result = TDBlobKey.ConvertFromHex(input);

			Assert.That(result, Is.EquivalentTo(expected));
		}

		[Test]
		public void When_Comparing_Two_Equal_TDBlobKey_Instances_Equals_Is_True()
		{
			var arr = new byte[] { 87, 79, 87, 46, 46, 46 };
			var key = new TDBlobKey(arr);
			var equals = key.Equals(new TDBlobKey(arr));

			Assert.That(equals, Is.True);
		}

		[Test]
		public void When_Comparing_Two_Unequal_TDBlobKey_Instances_Equals_Is_False()
		{
			var arr = new byte[] { 87, 79, 87, 46, 46, 46 };
			var arr2 = new byte[] { 87, 79, 87, 46, 46, 45 };
			var key = new TDBlobKey(arr);
			var equals = key.Equals(new TDBlobKey(arr2));

			Assert.That(equals, Is.False);
		}

		[Test]
		public void When_Calling_To_String_Output_Matches_Convert_To_Hex()
		{
			var input = new byte[] { 87, 79, 87, 46, 46, 46 };
			var result = TDBlobKey.ConvertToHex(input);

			Assert.That(result, Is.StringMatching(new TDBlobKey(input).ToString()));
		}

	}
}
