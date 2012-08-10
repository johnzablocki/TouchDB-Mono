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

using System.Collections.Generic;
using Couchbase.TouchDB;
using Sharpen;

namespace Couchbase.TouchDB
{
	/// <summary>
	/// Stores information about a revision -- its docID, revID, and whether it's
	/// deleted.
	/// </summary>
	/// <remarks>
	/// Stores information about a revision -- its docID, revID, and whether it's
	/// deleted.
	/// It can also store the sequence number and document contents (they can be
	/// added after creation).
	/// </remarks>
	public class TDRevision
	{
		private string docId;

		private string revId;

		private bool deleted;

		private TDBody body;

		private long sequence;

		public TDRevision(string docId, string revId, bool deleted)
		{
			this.docId = docId;
			this.revId = revId;
			this.deleted = deleted;
		}

		public TDRevision(TDBody body) : this((string)body.GetPropertyForKey("_id"), (string
			)body.GetPropertyForKey("_rev"), (((bool)body.GetPropertyForKey("_deleted") != null
			) && ((bool)body.GetPropertyForKey("_deleted") == true)))
		{
			this.body = body;
		}

		public TDRevision(IDictionary<string, object> properties) : this(new TDBody(properties
			))
		{
		}

		public virtual IDictionary<string, object> GetProperties()
		{
			IDictionary<string, object> result = null;
			if (body != null)
			{
				result = body.GetProperties();
			}
			return result;
		}

		public virtual void SetProperties(IDictionary<string, object> properties)
		{
			this.body = new TDBody(properties);
		}

		public virtual byte[] GetJson()
		{
			byte[] result = null;
			if (body != null)
			{
				result = body.GetJson();
			}
			return result;
		}

		public virtual void SetJson(byte[] json)
		{
			this.body = new TDBody(json);
		}

		public override bool Equals(object o)
		{
			bool result = false;
			if (o is Couchbase.TouchDB.TDRevision)
			{
				Couchbase.TouchDB.TDRevision other = (Couchbase.TouchDB.TDRevision)o;
				if (docId.Equals(other.docId) && revId.Equals(other.revId))
				{
					result = true;
				}
			}
			return result;
		}

		public override int GetHashCode()
		{
			return docId.GetHashCode() ^ revId.GetHashCode();
		}

		public virtual string GetDocId()
		{
			return docId;
		}

		public virtual void SetDocId(string docId)
		{
			this.docId = docId;
		}

		public virtual string GetRevId()
		{
			return revId;
		}

		public virtual void SetRevId(string revId)
		{
			this.revId = revId;
		}

		public virtual bool IsDeleted()
		{
			return deleted;
		}

		public virtual void SetDeleted(bool deleted)
		{
			this.deleted = deleted;
		}

		public virtual TDBody GetBody()
		{
			return body;
		}

		public virtual void SetBody(TDBody body)
		{
			this.body = body;
		}

		public virtual Couchbase.TouchDB.TDRevision CopyWithDocID(string docId, string revId
			)
		{
			//assert ((docId != null) && (revId != null));
			//assert ((this.docId == null) || (this.docId.equals(docId)));
			Couchbase.TouchDB.TDRevision result = new Couchbase.TouchDB.TDRevision(docId, revId
				, deleted);
			IDictionary<string, object> properties = GetProperties();
			if (properties == null)
			{
				properties = new Dictionary<string, object>();
			}
			properties.Put("_id", docId);
			properties.Put("_rev", revId);
			result.SetProperties(properties);
			return result;
		}

		public virtual void SetSequence(long sequence)
		{
			this.sequence = sequence;
		}

		public virtual long GetSequence()
		{
			return sequence;
		}

		public override string ToString()
		{
			return "{" + this.docId + " #" + this.revId + (deleted ? "DEL" : string.Empty) + 
				"}";
		}

		/// <summary>Generation number: 1 for a new document, 2 for the 2nd revision, ...</summary>
		/// <remarks>
		/// Generation number: 1 for a new document, 2 for the 2nd revision, ...
		/// Extracted from the numeric prefix of the revID.
		/// </remarks>
		public virtual int GetGeneration()
		{
			return GenerationFromRevID(revId);
		}

		public static int GenerationFromRevID(string revID)
		{
			int generation = 0;
			int dashPos = revID.IndexOf("-");
			if (dashPos > 0)
			{
				generation = System.Convert.ToInt32(Sharpen.Runtime.Substring(revID, 0, dashPos));
			}
			return generation;
		}
	}
}
