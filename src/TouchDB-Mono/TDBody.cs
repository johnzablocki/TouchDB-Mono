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
	public class TDBody : TDBody<object>
	{
		public TDBody(byte[] json) : base(json) { }

		public TDBody(IDictionary<string, object> properties) : base(properties) { }
		
		public TDBody(IList<object> array) : base(array) { }
        
	}
}
