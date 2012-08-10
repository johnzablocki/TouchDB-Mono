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
using System.Collections.Generic;
using System.Text;
using Android.Database;
using Android.Util;
using Couchbase.TouchDB;
using Couchbase.TouchDB.Replicator;
using Couchbase.TouchDB.Replicator.Changetracker;
using Couchbase.TouchDB.Support;
using Org.Apache.Http.Client;
using Sharpen;

namespace Couchbase.TouchDB.Replicator
{
	public class TDPuller : TDReplicator, TDChangeTrackerClient
	{
		private const int MAX_OPEN_HTTP_CONNECTIONS = 16;

		protected internal TDBatcher<IList<object>> downloadsToInsert;

		protected internal IList<TDRevision> revsToPull;

		protected internal long nextFakeSequence;

		protected internal long maxInsertedFakeSequence;

		protected internal TDChangeTracker changeTracker;

		protected internal int httpConnectionCount;

		public TDPuller(TDDatabase db, Uri remote, bool continuous) : this(db, remote, continuous
			, null)
		{
		}

		public TDPuller(TDDatabase db, Uri remote, bool continuous, HttpClientFactory clientFactory
			) : base(db, remote, continuous, clientFactory)
		{
		}

		public override void BeginReplicating()
		{
			if (downloadsToInsert == null)
			{
				downloadsToInsert = new TDBatcher<IList<object>>(db.GetHandler(), 200, 1000, new 
					_TDBatchProcessor_55(this));
			}
			nextFakeSequence = maxInsertedFakeSequence = 0;
			Log.W(TDDatabase.TAG, this + " starting ChangeTracker with since=" + lastSequence
				);
			changeTracker = new TDChangeTracker(remote, continuous ? TDChangeTracker.TDChangeTrackerMode
				.LongPoll : TDChangeTracker.TDChangeTrackerMode.OneShot, lastSequence, this);
			if (filterName != null)
			{
				changeTracker.SetFilterName(filterName);
				if (filterParams != null)
				{
					changeTracker.SetFilterParams(filterParams);
				}
			}
			changeTracker.Start();
			AsyncTaskStarted();
		}

		private sealed class _TDBatchProcessor_55 : TDBatchProcessor<IList<object>>
		{
			public _TDBatchProcessor_55(TDPuller _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public void Process(IList<IList<object>> inbox)
			{
				this._enclosing.InsertRevisions(inbox);
			}

			private readonly TDPuller _enclosing;
		}

		public override void Stop()
		{
			if (!running)
			{
				return;
			}
			changeTracker.SetClient(null);
			// stop it from calling my changeTrackerStopped()
			changeTracker.Stop();
			changeTracker = null;
			lock (this)
			{
				revsToPull = null;
			}
			base.Stop();
			downloadsToInsert.Flush();
		}

		public override void Stopped()
		{
			downloadsToInsert.Flush();
			downloadsToInsert.Close();
			base.Stopped();
		}

		// Got a _changes feed entry from the TDChangeTracker.
		public virtual void ChangeTrackerReceivedChange(IDictionary<string, object> change
			)
		{
			string lastSequence = change.Get("seq").ToString();
			string docID = (string)change.Get("id");
			if (docID == null)
			{
				return;
			}
			if (!TDDatabase.IsValidDocumentId(docID))
			{
				Log.W(TDDatabase.TAG, string.Format("%s: Received invalid doc ID from _changes: %s"
					, this, change));
				return;
			}
			bool deleted = (change.ContainsKey("deleted") && ((bool)change.Get("deleted")).Equals
				(true));
			IList<IDictionary<string, object>> changes = (IList<IDictionary<string, object>>)
				change.Get("changes");
			foreach (IDictionary<string, object> changeDict in changes)
			{
				string revID = (string)changeDict.Get("rev");
				if (revID == null)
				{
					continue;
				}
				TDPulledRevision rev = new TDPulledRevision(docID, revID, deleted);
				rev.SetRemoteSequenceID(lastSequence);
				rev.SetSequence(++nextFakeSequence);
				AddToInbox(rev);
			}
			SetChangesTotal(GetChangesTotal() + changes.Count);
		}

		public virtual void ChangeTrackerStopped(TDChangeTracker tracker)
		{
			Log.W(TDDatabase.TAG, this + ": ChangeTracker stopped");
			//FIXME tracker doesnt have error right now
			//        if(error == null && tracker.getError() != null) {
			//            error = tracker.getError();
			//        }
			changeTracker = null;
			if (batcher != null)
			{
				batcher.Flush();
			}
			AsyncTaskFinished(1);
		}

		public virtual HttpClient GetHttpClient()
		{
			HttpClient httpClient = this.clientFacotry.GetHttpClient();
			return httpClient;
		}

		/// <summary>Process a bunch of remote revisions from the _changes feed at once</summary>
		public override void ProcessInbox(TDRevisionList inbox)
		{
			// Ask the local database which of the revs are not known to it:
			//Log.w(TDDatabase.TAG, String.format("%s: Looking up %s", this, inbox));
			string lastInboxSequence = ((TDPulledRevision)inbox[inbox.Count - 1]).GetRemoteSequenceID
				();
			int total = GetChangesTotal() - inbox.Count;
			if (!db.FindMissingRevisions(inbox))
			{
				Log.W(TDDatabase.TAG, string.Format("%s failed to look up local revs", this));
				inbox = null;
			}
			//introducing this to java version since inbox may now be null everywhere
			int inboxCount = 0;
			if (inbox != null)
			{
				inboxCount = inbox.Count;
			}
			if (GetChangesTotal() != total + inboxCount)
			{
				SetChangesTotal(total + inboxCount);
			}
			if (inboxCount == 0)
			{
				// Nothing to do. Just bump the lastSequence.
				Log.W(TDDatabase.TAG, string.Format("%s no new remote revisions to fetch", this));
				SetLastSequence(lastInboxSequence);
				return;
			}
			Log.V(TDDatabase.TAG, this + " fetching " + inboxCount + " remote revisions...");
			//Log.v(TDDatabase.TAG, String.format("%s fetching remote revisions %s", this, inbox));
			// Dump the revs into the queue of revs to pull from the remote db:
			if (revsToPull == null)
			{
				revsToPull = new AList<TDRevision>(200);
			}
			Sharpen.Collections.AddAll(revsToPull, inbox);
			PullRemoteRevisions();
			//TEST
			//adding wait here to prevent revsToPull from getting too large
			while (revsToPull != null && revsToPull.Count > 1000)
			{
				PullRemoteRevisions();
				try
				{
					Sharpen.Thread.Sleep(500);
				}
				catch (Exception)
				{
				}
			}
		}

		//wake up
		/// <summary>
		/// Start up some HTTP GETs, within our limit on the maximum simultaneous number
		/// Needs to be synchronized because multiple RemoteRequest theads call this upon completion
		/// to keep the process moving, need to synchronize check for size with removal
		/// </summary>
		public virtual void PullRemoteRevisions()
		{
			lock (this)
			{
				while (httpConnectionCount < MAX_OPEN_HTTP_CONNECTIONS && revsToPull != null && revsToPull
					.Count > 0)
				{
					PullRemoteRevision(revsToPull[0]);
					revsToPull.Remove(0);
				}
			}
		}

		/// <summary>Fetches the contents of a revision from the remote db, including its parent revision ID.
		/// 	</summary>
		/// <remarks>
		/// Fetches the contents of a revision from the remote db, including its parent revision ID.
		/// The contents are stored into rev.properties.
		/// </remarks>
		public virtual void PullRemoteRevision(TDRevision rev)
		{
			AsyncTaskStarted();
			++httpConnectionCount;
			// Construct a query. We want the revision history, and the bodies of attachments that have
			// been added since the latest revisions we have locally.
			// See: http://wiki.apache.org/couchdb/HTTP_Document_API#Getting_Attachments_With_a_Document
			StringBuilder path = new StringBuilder("/" + URLEncoder.Encode(rev.GetDocId()) + 
				"?rev=" + URLEncoder.Encode(rev.GetRevId()) + "&revs=true&attachments=true");
			IList<string> knownRevs = KnownCurrentRevIDs(rev);
			if (knownRevs == null)
			{
				//this means something is wrong, possibly the replicator has shut down
				AsyncTaskFinished(1);
				--httpConnectionCount;
				return;
			}
			if (knownRevs.Count > 0)
			{
				path.Append("&atts_since=");
				path.Append(JoinQuotedEscaped(knownRevs));
			}
			//create a final version of this variable for the log statement inside
			//FIXME find a way to avoid this
			string pathInside = path.ToString();
			SendAsyncRequest("GET", pathInside, null, new _TDRemoteRequestCompletionBlock_245
				(this, rev, pathInside));
		}

		private sealed class _TDRemoteRequestCompletionBlock_245 : TDRemoteRequestCompletionBlock
		{
			public _TDRemoteRequestCompletionBlock_245(TDPuller _enclosing, TDRevision rev, string
				 pathInside)
			{
				this._enclosing = _enclosing;
				this.rev = rev;
				this.pathInside = pathInside;
			}

			public void OnCompletion(object result, Exception e)
			{
				// OK, now we've got the response revision:
				if (result != null)
				{
					IDictionary<string, object> properties = (IDictionary<string, object>)result;
					IList<string> history = TDDatabase.ParseCouchDBRevisionHistory(properties);
					if (history != null)
					{
						rev.SetProperties(properties);
						// Add to batcher ... eventually it will be fed to -insertRevisions:.
						IList<object> toInsert = new AList<object>();
						toInsert.AddItem(rev);
						toInsert.AddItem(history);
						this._enclosing.downloadsToInsert.QueueObject(toInsert);
						this._enclosing.AsyncTaskStarted();
					}
					else
					{
						Log.W(TDDatabase.TAG, this + ": Missing revision history in response from " + pathInside
							);
						this._enclosing.SetChangesProcessed(this._enclosing.GetChangesProcessed() + 1);
					}
				}
				else
				{
					if (e != null)
					{
						this._enclosing.error = e;
					}
					this._enclosing.SetChangesProcessed(this._enclosing.GetChangesProcessed() + 1);
				}
				// Note that we've finished this task; then start another one if there
				// are still revisions waiting to be pulled:
				this._enclosing.AsyncTaskFinished(1);
				--this._enclosing.httpConnectionCount;
				this._enclosing.PullRemoteRevisions();
			}

			private readonly TDPuller _enclosing;

			private readonly TDRevision rev;

			private readonly string pathInside;
		}

		/// <summary>This will be called when _revsToInsert fills up:</summary>
		public virtual void InsertRevisions(IList<IList<object>> revs)
		{
			Log.I(TDDatabase.TAG, this + " inserting " + revs.Count + " revisions...");
			//Log.v(TDDatabase.TAG, String.format("%s inserting %s", this, revs));
			revs.Sort(new _IComparer_291());
			bool allGood = true;
			TDPulledRevision lastGoodRev = null;
			if (db == null)
			{
				return;
			}
			db.BeginTransaction();
			bool success = false;
			try
			{
				foreach (IList<object> revAndHistory in revs)
				{
					TDPulledRevision rev = (TDPulledRevision)revAndHistory[0];
					IList<string> history = (IList<string>)revAndHistory[1];
					// Insert the revision:
					TDStatus status = db.ForceInsert(rev, history, remote);
					if (!status.IsSuccessful())
					{
						if (status.GetCode() == TDStatus.FORBIDDEN)
						{
							Log.I(TDDatabase.TAG, this + ": Remote rev failed validation: " + rev);
						}
						else
						{
							Log.W(TDDatabase.TAG, this + " failed to write " + rev + ": status=" + status.GetCode
								());
							error = new HttpResponseException(status.GetCode(), null);
							allGood = false;
						}
					}
					// stop advancing lastGoodRev
					if (allGood)
					{
						lastGoodRev = rev;
					}
				}
				// Now update lastSequence from the latest consecutively inserted revision:
				long lastGoodFakeSequence = lastGoodRev.GetSequence();
				if (lastGoodFakeSequence > maxInsertedFakeSequence)
				{
					maxInsertedFakeSequence = lastGoodFakeSequence;
					SetLastSequence(lastGoodRev.GetRemoteSequenceID());
				}
				Log.W(TDDatabase.TAG, this + " finished inserting " + revs.Count + " revisions");
				success = true;
			}
			catch (SQLException e)
			{
				Log.W(TDDatabase.TAG, this + ": Exception inserting revisions", e);
			}
			finally
			{
				db.EndTransaction(success);
				AsyncTaskFinished(revs.Count);
			}
			SetChangesProcessed(GetChangesProcessed() + revs.Count);
		}

		private sealed class _IComparer_291 : IComparer<IList<object>>
		{
			public _IComparer_291()
			{
			}

			public int Compare(IList<object> list1, IList<object> list2)
			{
				TDRevision reva = (TDRevision)list1[0];
				TDRevision revb = (TDRevision)list2[0];
				return TDMisc.TDSequenceCompare(reva.GetSequence(), revb.GetSequence());
			}
		}

		internal virtual IList<string> KnownCurrentRevIDs(TDRevision rev)
		{
			if (db != null)
			{
				return db.GetAllRevisionsOfDocumentID(rev.GetDocId(), true).GetAllRevIds();
			}
			return null;
		}

		public virtual string JoinQuotedEscaped(IList<string> strings)
		{
			if (strings.Count == 0)
			{
				return "[]";
			}
			byte[] json = null;
			try
			{
				json = TDServer.GetObjectMapper().WriteValueAsBytes(strings);
			}
			catch (Exception e)
			{
				Log.W(TDDatabase.TAG, "Unable to serialize json", e);
			}
			return URLEncoder.Encode(Sharpen.Runtime.GetStringForBytes(json));
		}
	}

	/// <summary>A revision received from a remote server during a pull.</summary>
	/// <remarks>A revision received from a remote server during a pull. Tracks the opaque remote sequence ID.
	/// 	</remarks>
	internal class TDPulledRevision : TDRevision
	{
		public TDPulledRevision(TDBody body) : base(body)
		{
		}

		public TDPulledRevision(string docId, string revId, bool deleted) : base(docId, revId
			, deleted)
		{
		}

		public TDPulledRevision(IDictionary<string, object> properties) : base(properties
			)
		{
		}

		protected internal string remoteSequenceID;

		public virtual string GetRemoteSequenceID()
		{
			return remoteSequenceID;
		}

		public virtual void SetRemoteSequenceID(string remoteSequenceID)
		{
			this.remoteSequenceID = remoteSequenceID;
		}
	}
}
