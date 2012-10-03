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
    /// Query options for view
    /// </summary>
	public class TDQueryOptions
	{
        public object StartKey { get; set; }

        public object EndKey { get; set; }

        public IList<object> Keys { get; set; }

        public int Skip { get; set; }

        public int Limit { get; set; }

        public int GroupLevel { get; set; }

        //TODO: revisit EnumSet equiv
        public IList<TDContentOptions> ContentOptions { get; set; }

        public bool Descending { get; set; }

        public bool IncludeDocs { get; set; }

        public bool UpdateSequence { get; set; }

        public bool InclusiveEnd { get; set; }

        public bool Reduce { get; set; }

        public bool Group { get; set; }
	}
}
