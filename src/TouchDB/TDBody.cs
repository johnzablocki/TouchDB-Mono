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
using System.Collections;
using System.Collections.Generic;
using Android.Util;
using Couchbase.TouchDB;
using Org.Codehaus.Jackson.Map;
using Sharpen;

namespace Couchbase.TouchDB
{
	/// <summary>A request/response/document body, stored as either JSON or a Map<String,Object>
	/// 	</summary>
	public class TDBody
	{
		private byte[] json;

		private object @object;

		private bool error = false;

		public TDBody(byte[] json)
		{
			this.json = json;
		}

		public TDBody(IDictionary<string, object> properties)
		{
			this.@object = properties;
		}

		public TDBody(IList<object> array)
		{
			this.@object = array;
		}

		public static Couchbase.TouchDB.TDBody BodyWithProperties(IDictionary<string, object
			> properties)
		{
			Couchbase.TouchDB.TDBody result = new Couchbase.TouchDB.TDBody(properties);
			return result;
		}

		public static Couchbase.TouchDB.TDBody BodyWithJSON(byte[] json)
		{
			Couchbase.TouchDB.TDBody result = new Couchbase.TouchDB.TDBody(json);
			return result;
		}

		public virtual bool IsValidJSON()
		{
			// Yes, this is just like asObject except it doesn't warn.
			if (json == null && !error)
			{
				try
				{
					json = TDServer.GetObjectMapper().WriteValueAsBytes(@object);
				}
				catch (Exception)
				{
					error = true;
				}
			}
			return (@object != null);
		}

		public virtual byte[] GetJson()
		{
			if (json == null && !error)
			{
				try
				{
					json = TDServer.GetObjectMapper().WriteValueAsBytes(@object);
				}
				catch (Exception)
				{
					Log.W(TDDatabase.TAG, "TDBody: couldn't convert JSON");
					error = true;
				}
			}
			return json;
		}

		public virtual byte[] GetPrettyJson()
		{
			object properties = GetObject();
			if (properties != null)
			{
				ObjectWriter writer = TDServer.GetObjectMapper().WriterWithDefaultPrettyPrinter();
				try
				{
					json = writer.WriteValueAsBytes(properties);
				}
				catch (Exception)
				{
					error = true;
				}
			}
			return GetJson();
		}

		public virtual string GetJSONString()
		{
			return Sharpen.Runtime.GetStringForBytes(GetJson());
		}

		public virtual object GetObject()
		{
			if (@object == null && !error)
			{
				try
				{
					if (json != null)
					{
						@object = TDServer.GetObjectMapper().ReadValue<IDictionary>(json);
					}
				}
				catch (Exception e)
				{
					Log.W(TDDatabase.TAG, "TDBody: couldn't parse JSON: " + Sharpen.Runtime.GetStringForBytes
						(json), e);
					error = true;
				}
			}
			return @object;
		}

		public virtual IDictionary<string, object> GetProperties()
		{
			object @object = GetObject();
			if (@object is IDictionary)
			{
				return (IDictionary<string, object>)@object;
			}
			return null;
		}

		public virtual object GetPropertyForKey(string key)
		{
			IDictionary<string, object> theProperties = GetProperties();
			return theProperties.Get(key);
		}

		public virtual bool IsError()
		{
			return error;
		}
	}
}
