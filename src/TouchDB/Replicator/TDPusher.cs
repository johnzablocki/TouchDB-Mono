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
using Android.Util;
using Couchbase.TouchDB;
using Couchbase.TouchDB.Replicator;
using Couchbase.TouchDB.Support;
using Org.Apache.Http.Client;
using Sharpen;

namespace Couchbase.TouchDB.Replicator
{
	public class TDPusher : TDReplicator, Observer
	{
		private bool createTarget;

		private bool observing;

		private TDFilterBlock filter;

		public TDPusher(TDDatabase db, Uri remote, bool continuous) : this(db, remote, continuous
			, null)
		{
		}

		public TDPusher(TDDatabase db, Uri remote, bool continuous, HttpClientFactory clientFactory
			) : base(db, remote, continuous, clientFactory)
		{
			createTarget = false;
			observing = false;
		}

		public virtual void SetCreateTarget(bool createTarget)
		{
			this.createTarget = createTarget;
		}

		public virtual void SetFilter(TDFilterBlock filter)
		{
			this.filter = filter;
		}

		public override bool IsPush()
		{
			return true;
		}

		public override void MaybeCreateRemoteDB()
		{
			if (!createTarget)
			{
				return;
			}
			Log.V(TDDatabase.TAG, "Remote db might not exist; creating it...");
			SendAsyncRequest("PUT", string.Empty, null, new _TDRemoteRequestCompletionBlock_59
				(this));
		}

		private sealed class _TDRemoteRequestCompletionBlock_59 : TDRemoteRequestCompletionBlock
		{
			public _TDRemoteRequestCompletionBlock_59(TDPusher _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public void OnCompletion(object result, Exception e)
			{
				if (e != null && e is HttpResponseException && ((HttpResponseException)e).GetStatusCode
					() != 412)
				{
					Log.E(TDDatabase.TAG, "Failed to create remote db", e);
					this._enclosing.error = e;
					this._enclosing.Stop();
				}
				else
				{
					Log.V(TDDatabase.TAG, "Created remote db");
					this._enclosing.createTarget = false;
					this._enclosing.BeginReplicating();
				}
			}

			private readonly TDPusher _enclosing;
		}

		public override void BeginReplicating()
		{
			// If we're still waiting to create the remote db, do nothing now. (This method will be
			// re-invoked after that request finishes; see maybeCreateRemoteDB() above.)
			if (createTarget)
			{
				return;
			}
			if (filterName != null)
			{
				filter = db.GetFilterNamed(filterName);
			}
			if (filterName != null && filter == null)
			{
				Log.W(TDDatabase.TAG, string.Format("%s: No TDFilterBlock registered for filter '%s'; ignoring"
					, this, filterName));
			}
			// Process existing changes since the last push:
			long lastSequenceLong = 0;
			if (lastSequence != null)
			{
				lastSequenceLong = long.Parse(lastSequence);
			}
			TDRevisionList changes = db.ChangesSince(lastSequenceLong, null, filter);
			if (changes.Count > 0)
			{
				ProcessInbox(changes);
			}
			// Now listen for future changes (in continuous mode):
			if (continuous)
			{
				observing = true;
				db.AddObserver(this);
				AsyncTaskStarted();
			}
		}

		// prevents stopped() from being called when other tasks finish
		public override void Stop()
		{
			StopObserving();
			base.Stop();
		}

		private void StopObserving()
		{
			if (observing)
			{
				observing = false;
				db.DeleteObserver(this);
				AsyncTaskFinished(1);
			}
		}

		public virtual void Update(Observable observable, object data)
		{
			//make sure this came from where we expected
			if (observable == db)
			{
				IDictionary<string, object> change = (IDictionary<string, object>)data;
				// Skip revisions that originally came from the database I'm syncing to:
				Uri source = (Uri)change.Get("source");
				if (source != null && source.Equals(remote.ToExternalForm()))
				{
					return;
				}
				TDRevision rev = (TDRevision)change.Get("rev");
				if (rev != null && ((filter == null) || filter.Filter(rev)))
				{
					AddToInbox(rev);
				}
			}
		}

		public override void ProcessInbox(TDRevisionList inbox)
		{
			long lastInboxSequence = inbox[inbox.Count - 1].GetSequence();
			// Generate a set of doc/rev IDs in the JSON format that _revs_diff wants:
			IDictionary<string, IList<string>> diffs = new Dictionary<string, IList<string>>(
				);
			foreach (TDRevision rev in inbox)
			{
				string docID = rev.GetDocId();
				IList<string> revs = diffs.Get(docID);
				if (revs == null)
				{
					revs = new AList<string>();
					diffs.Put(docID, revs);
				}
				revs.AddItem(rev.GetRevId());
			}
			// Call _revs_diff on the target db:
			AsyncTaskStarted();
			SendAsyncRequest("POST", "/_revs_diff", diffs, new _TDRemoteRequestCompletionBlock_159
				(this, inbox, lastInboxSequence));
		}

		private sealed class _TDRemoteRequestCompletionBlock_159 : TDRemoteRequestCompletionBlock
		{
			public _TDRemoteRequestCompletionBlock_159(TDPusher _enclosing, TDRevisionList inbox
				, long lastInboxSequence)
			{
				this._enclosing = _enclosing;
				this.inbox = inbox;
				this.lastInboxSequence = lastInboxSequence;
			}

			public void OnCompletion(object response, Exception e)
			{
				IDictionary<string, object> results = (IDictionary<string, object>)response;
				if (e != null)
				{
					this._enclosing.error = e;
					this._enclosing.Stop();
				}
				else
				{
					if (results.Count != 0)
					{
						// Go through the list of local changes again, selecting the ones the destination server
						// said were missing and mapping them to a JSON dictionary in the form _bulk_docs wants:
						IList<object> docsToSend = new AList<object>();
						foreach (TDRevision rev in inbox)
						{
							IDictionary<string, object> properties = null;
							IDictionary<string, object> resultDoc = (IDictionary<string, object>)results.Get(
								rev.GetDocId());
							if (resultDoc != null)
							{
								IList<string> revs = (IList<string>)resultDoc.Get("missing");
								if (revs != null && revs.Contains(rev.GetRevId()))
								{
									//remote server needs this revision
									// Get the revision's properties
									if (rev.IsDeleted())
									{
										properties = new Dictionary<string, object>();
										properties.Put("_id", rev.GetDocId());
										properties.Put("_rev", rev.GetRevId());
										properties.Put("_deleted", true);
									}
									else
									{
										// OPT: Shouldn't include all attachment bodies, just ones that have changed
										// OPT: Should send docs with many or big attachments as multipart/related
										TDStatus status = this._enclosing.db.LoadRevisionBody(rev, EnumSet.Of(TDDatabase.TDContentOptions
											.TDIncludeAttachments));
										if (!status.IsSuccessful())
										{
											Log.W(TDDatabase.TAG, string.Format("%s: Couldn't get local contents of %s", this
												, rev));
										}
										else
										{
											properties = new Dictionary<string, object>(rev.GetProperties());
										}
									}
									if (properties != null)
									{
										// Add the _revisions list:
										properties.Put("_revisions", this._enclosing.db.GetRevisionHistoryDict(rev));
										//now add it to the docs to send
										docsToSend.AddItem(properties);
									}
								}
							}
						}
						// Post the revisions to the destination. "new_edits":false means that the server should
						// use the given _rev IDs instead of making up new ones.
						int numDocsToSend = docsToSend.Count;
						IDictionary<string, object> bulkDocsBody = new Dictionary<string, object>();
						bulkDocsBody.Put("docs", docsToSend);
						bulkDocsBody.Put("new_edits", false);
						Log.I(TDDatabase.TAG, string.Format("%s: Sending %d revisions", this, numDocsToSend
							));
						Log.V(TDDatabase.TAG, string.Format("%s: Sending %s", this, inbox));
						this._enclosing.SetChangesTotal(this._enclosing.GetChangesTotal() + numDocsToSend
							);
						this._enclosing.AsyncTaskStarted();
						this._enclosing.SendAsyncRequest("POST", "/_bulk_docs", bulkDocsBody, new _TDRemoteRequestCompletionBlock_214
							(this, inbox, lastInboxSequence, numDocsToSend));
					}
					else
					{
						// If none of the revisions are new to the remote, just bump the lastSequence:
						this._enclosing.SetLastSequence(string.Format("%d", lastInboxSequence));
					}
				}
				this._enclosing.AsyncTaskFinished(1);
			}

			private sealed class _TDRemoteRequestCompletionBlock_214 : TDRemoteRequestCompletionBlock
			{
				public _TDRemoteRequestCompletionBlock_214(_TDRemoteRequestCompletionBlock_159 _enclosing
					, TDRevisionList inbox, long lastInboxSequence, int numDocsToSend)
				{
					this._enclosing = _enclosing;
					this.inbox = inbox;
					this.lastInboxSequence = lastInboxSequence;
					this.numDocsToSend = numDocsToSend;
				}

				public void OnCompletion(object result, Exception e)
				{
					if (e != null)
					{
						this._enclosing._enclosing.error = e;
					}
					else
					{
						Log.V(TDDatabase.TAG, string.Format("%s: Sent %s", this, inbox));
						this._enclosing._enclosing.SetLastSequence(string.Format("%d", lastInboxSequence)
							);
					}
					this._enclosing._enclosing.SetChangesProcessed(this._enclosing._enclosing.GetChangesProcessed
						() + numDocsToSend);
					this._enclosing._enclosing.AsyncTaskFinished(1);
				}

				private readonly _TDRemoteRequestCompletionBlock_159 _enclosing;

				private readonly TDRevisionList inbox;

				private readonly long lastInboxSequence;

				private readonly int numDocsToSend;
			}

			private readonly TDPusher _enclosing;

			private readonly TDRevisionList inbox;

			private readonly long lastInboxSequence;
		}
	}
}
