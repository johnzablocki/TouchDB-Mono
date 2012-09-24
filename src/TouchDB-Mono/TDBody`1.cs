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

namespace TouchDB.Mono
{
	/// <summary>
	/// A request/response/document body, stored as either JSON or a Map<String,Object>
	/// </summary>
	/// <typeparam name="T">Type of the stored object</typeparam>
	public class TDBody<T>
	{
		private bool _error = false;

		public byte[] Json { get; set; }
		private object Object { get; set; }

		public TDBody(byte[] json)
		{
			Json = json;
		}

		public TDBody(Dictionary<string, object> properties)
		{
			Object = properties;
		}

		public TDBody(IList<T> array)
		{
			Object = array;
		}

		public bool IsValidJson
		{
			get
			{
				// Yes, this is just like asObject except it doesn't warn.
				if (Json == null && !_error)
				{
					
					try 
					{
						//TODO: implement JSON validation similar to Java 
						//Java -> TDServer.getObjectMapper().writeValueAsBytes(object);
					} 
					catch
					{
						_error = true;
					}
				}
				return (Object != null);
			}
		}
		
		public static TDBody<T> BodyWithProperties(Dictionary<string, object> properties)
		{
			var result = new TDBody<T>(properties);
			return result;
		}

		public static TDBody<T> BodyWithJson(byte[] json)
		{
			var result = new TDBody<T>(json);
			return result;
		}




	}
}
