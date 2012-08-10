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
	/// <summary>An ordered list of TDRevisions</summary>
	[System.Serializable]
	public class TDRevisionList : AList<TDRevision>
	{
		public TDRevisionList() : base()
		{
		}

		/// <summary>Allow converting to TDRevisionList from List<TDRevision></summary>
		/// <param name="list"></param>
		public TDRevisionList(IList<TDRevision> list) : base(list)
		{
		}

		public virtual TDRevision RevWithDocIdAndRevId(string docId, string revId)
		{
			Iterator<TDRevision> iterator = Iterator();
			while (iterator.HasNext())
			{
				TDRevision rev = iterator.Next();
				if (docId.Equals(rev.GetDocId()) && revId.Equals(rev.GetRevId()))
				{
					return rev;
				}
			}
			return null;
		}

		public virtual IList<string> GetAllDocIds()
		{
			IList<string> result = new AList<string>();
			Iterator<TDRevision> iterator = Iterator();
			while (iterator.HasNext())
			{
				TDRevision rev = iterator.Next();
				result.AddItem(rev.GetDocId());
			}
			return result;
		}

		public virtual IList<string> GetAllRevIds()
		{
			IList<string> result = new AList<string>();
			Iterator<TDRevision> iterator = Iterator();
			while (iterator.HasNext())
			{
				TDRevision rev = iterator.Next();
				result.AddItem(rev.GetRevId());
			}
			return result;
		}

		public virtual void SortBySequence()
		{
			this.Sort(new _IComparer_80());
		}

		private sealed class _IComparer_80 : IComparer<TDRevision>
		{
			public _IComparer_80()
			{
			}

			public int Compare(TDRevision rev1, TDRevision rev2)
			{
				return TDMisc.TDSequenceCompare(rev1.GetSequence(), rev2.GetSequence());
			}
		}

		public virtual void Limit(int limit)
		{
			if (Count > limit)
			{
				RemoveRange(limit, Count);
			}
		}
	}
}
