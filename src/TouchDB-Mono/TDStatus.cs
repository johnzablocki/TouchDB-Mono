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
	/// Same interpretation as HTTP status codes, esp. 200, 201, 404, 409, 500.
	/// </summary>
	public class TDStatus
	{
		public const int UNKNOWN = -1;
		public const int OK = 200;
		public const int CREATED = 201;
		public const int NOT_MODIFIED = 304;
		public const int BAD_REQUEST = 400;
		public const int FORBIDDEN = 403;
		public const int NOT_FOUND = 404;
		public const int METHOD_NOT_ALLOWED = 405;
		public const int NOT_ACCEPTABLE = 406;
		public const int CONFLICT = 409;
		public const int PRECONDITION_FAILED = 412;
		public const int BAD_JSON = 493;
		public const int INTERNAL_SERVER_ERROR = 500;
		public const int DB_ERROR = 590;

		public TDStatus(int code)
		{
			Code = code;
		}

		public int Code { get; set; }

		public bool IsSuccessful
		{
			get { return Code > 0 && Code < 400; }
		}

		public override string ToString()
		{
			return "Status: " + Code;
		}
	}
}
