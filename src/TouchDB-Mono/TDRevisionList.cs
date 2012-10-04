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
    /// An ordered list of TDRevisions
    /// </summary>
    public class TDRevisionList : List<TDRevision>
    {
        public TDRevisionList() : base()
        {

        }

        //Allow converting to TDRevisionList from List<TDRevision>
        public TDRevisionList(List<TDRevision> list) : base(list)
        {

        }

        public TDRevision FindByRevAndDocId(string docId, string revId)
        {
            return this.Where(r => r.DocId.Equals(docId) && r.RevId.Equals(revId)).FirstOrDefault();
        }

        public IList<string> AllDocIds
        {
            get { return this.Select(r => r.DocId).ToList(); }
        }

        public IList<string> AllRevIds
        {
            get { return this.Select(r => r.RevId).ToList(); }
        }

        public void SortBySequence()
        {            
            Sort((a, b) => a.Sequence > b.Sequence  ? 1 : 0);
        }

        public void Limit(int limit)
        {
            if (Count > limit)
            {
                RemoveRange(limit, Count-limit);
            }
        }
    }
}