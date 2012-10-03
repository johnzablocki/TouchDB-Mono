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
    /// Stores information about a revision -- its docID, revID, and whether it's deleted.
    /// 
    /// It can also store the sequence number and document contents (they can be added after creation).
    /// </summary>
	public class TDRevision
	{
        public string DocId { get; set; }

        public string RevId { get; set; }

        public bool IsDeleted { get; set; }

        public TDBody Body { get; set; }

        public long Sequence { get; set; }

        public TDRevision(string docId, string revId, bool deleted)
        {
            DocId = docId;
            RevId = revId;
            IsDeleted = deleted;
        }

        public TDRevision(TDBody body) 
            : this(body.GetPropertyForKey("_id") as string, 
                   body.GetPropertyForKey("_rev") as string,
                   body.GetPropertyForKey("_deleted") != null)  
        {
            Body = body;
        }

        public TDRevision(IDictionary<string, object> properties) : this(new TDBody(properties))
        {
        }

        public IDictionary<string, object> Properties
        {
            get { return Body == null ? null : Body.Properties;  }
            set { Body = new TDBody(value); }
        }

        public byte[] Json
        {
            get { return Body == null ? null : Body.Json; }
            set { Body = new TDBody(value); }
        }

        public override bool Equals(object obj)
        {
            var result = false;
            if (obj is TDRevision)
            {
                var other = (TDRevision)obj;
                if (DocId.Equals(other.DocId) && RevId.Equals(other.RevId))
                {
                    result = true;
                }
            }

            return result;
        }

        public override int GetHashCode()
        {
            return DocId.GetHashCode() ^ RevId.GetHashCode();
        }

        public TDRevision CopyWithDocId(string docId, string revId)
        {
            if (string.IsNullOrEmpty(docId)) throw new ArgumentNullException("docId");
            if (string.IsNullOrEmpty(revId)) throw new ArgumentNullException("revId");

            var result = new TDRevision(docId, revId, IsDeleted);
            var properties = Properties ?? new Dictionary<string, object>();

            properties["_docId"] = docId;
            properties["_revId"] = revId;
            result.Properties = properties;

            return result;
            
        }

        public override string ToString()
        {
            return "{" + DocId + " #" + RevId + (IsDeleted ? "DEL" : "") + "}";
        }

        public static int GenerateFromRevId(string revId)
        {
            var generation = 0;
            var dashPos = revId.IndexOf("-");
            if (dashPos > 0)
            {
                generation = Convert.ToInt32(revId.Substring(0, dashPos));
            }

            return generation;
        }
	}
}