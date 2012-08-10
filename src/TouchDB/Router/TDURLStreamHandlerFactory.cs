/**
 * Original iOS version by  Jens Alfke
 * Ported Android version by Marty Schoch
 * C# Port by John Zablocki
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
using Couchbase.TouchDB.Router;
using Sharpen;

namespace Couchbase.TouchDB.Router
{
	public class TDURLStreamHandlerFactory : URLStreamHandlerFactory
	{
		public static readonly string SCHEME = "touchdb";

		public virtual URLStreamHandler CreateURLStreamHandler(string protocol)
		{
			if (SCHEME.Equals(protocol))
			{
				return new TDURLHandler();
			}
			return null;
		}

		public static void RegisterSelfIgnoreError()
		{
			try
			{
				Uri.SetURLStreamHandlerFactory(new TDURLStreamHandlerFactory());
			}
			catch (Error)
			{
			}
		}
		//usually you should never catch an Error
		//but I can't see how to avoid this
	}
}
