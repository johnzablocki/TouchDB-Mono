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
using Android.OS;
using Android.Util;
using Couchbase.TouchDB;
using Couchbase.TouchDB.Support;
using Org.Apache.Http.Client;
using Org.Apache.Http.Impl.Client;
using Sharpen;

namespace Couchbase.TouchDB.Replicator
{
	public abstract class TDReplicator : Observable
	{
		private static int lastSessionID = 0;

		protected internal Handler handler;

		protected internal TDDatabase db;

		protected internal Uri remote;

		protected internal bool continuous;

		protected internal string lastSequence;

		protected internal bool lastSequenceChanged;

		protected internal IDictionary<string, object> remoteCheckpoint;

		protected internal bool savingCheckpoint;

		protected internal bool overdueForSave;

		protected internal bool running;

		protected internal bool active;

		protected internal Exception error;

		protected internal string sessionID;

		protected internal TDBatcher<TDRevision> batcher;

		protected internal int asyncTaskCount;

		private int changesProcessed;

		private int changesTotal;

		protected internal readonly HttpClientFactory clientFacotry;

		protected internal string filterName;

		protected internal IDictionary<string, object> filterParams;

		protected internal const int PROCESSOR_DELAY = 500;

		protected internal const int INBOX_CAPACITY = 100;

		public TDReplicator(TDDatabase db, Uri remote, bool continuous) : this(db, remote
			, continuous, null)
		{
		}

		public TDReplicator(TDDatabase db, Uri remote, bool continuous, HttpClientFactory
			 clientFacotry)
		{
			this.db = db;
			this.remote = remote;
			this.continuous = continuous;
			this.handler = db.GetHandler();
			batcher = new TDBatcher<TDRevision>(db.GetHandler(), INBOX_CAPACITY, PROCESSOR_DELAY
				, new _TDBatchProcessor_67(this));
			this.clientFacotry = clientFacotry != null ? clientFacotry : new _HttpClientFactory_77
				();
		}

		private sealed class _TDBatchProcessor_67 : TDBatchProcessor<TDRevision>
		{
			public _TDBatchProcessor_67(TDReplicator _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public void Process(IList<TDRevision> inbox)
			{
				Log.V(TDDatabase.TAG, "*** " + this.ToString() + ": BEGIN processInbox (" + inbox
					.Count + " sequences)");
				this._enclosing.ProcessInbox(new TDRevisionList(inbox));
				Log.V(TDDatabase.TAG, "*** " + this.ToString() + ": END processInbox (lastSequence="
					 + this._enclosing.lastSequence);
				this._enclosing.active = false;
			}

			private readonly TDReplicator _enclosing;
		}

		private sealed class _HttpClientFactory_77 : HttpClientFactory
		{
			public _HttpClientFactory_77()
			{
			}

			public HttpClient GetHttpClient()
			{
				return new DefaultHttpClient();
			}
		}

		public virtual void SetFilterName(string filterName)
		{
			this.filterName = filterName;
		}

		public virtual void SetFilterParams(IDictionary<string, object> filterParams)
		{
			this.filterParams = filterParams;
		}

		public virtual bool IsRunning()
		{
			return running;
		}

		public virtual Uri GetRemote()
		{
			return remote;
		}

		public virtual void DatabaseClosing()
		{
			SaveLastSequence();
			Stop();
			db = null;
		}

		public override string ToString()
		{
			string name = GetType().Name + "[" + (remote != null ? remote.ToExternalForm() : 
				string.Empty) + "]";
			return name;
		}

		public virtual bool IsPush()
		{
			return false;
		}

		public virtual string GetLastSequence()
		{
			return lastSequence;
		}

		public virtual void SetLastSequence(string lastSequenceIn)
		{
			if (!lastSequenceIn.Equals(lastSequence))
			{
				Log.V(TDDatabase.TAG, ToString() + ": Setting lastSequence to " + lastSequenceIn 
					+ " from( " + lastSequence + ")");
				lastSequence = lastSequenceIn;
				if (!lastSequenceChanged)
				{
					lastSequenceChanged = true;
					handler.PostDelayed(new _Runnable_126(this), 2 * 1000);
				}
			}
		}

		private sealed class _Runnable_126 : Runnable
		{
			public _Runnable_126(TDReplicator _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public void Run()
			{
				this._enclosing.SaveLastSequence();
			}

			private readonly TDReplicator _enclosing;
		}

		public virtual int GetChangesProcessed()
		{
			return changesProcessed;
		}

		public virtual void SetChangesProcessed(int processed)
		{
			this.changesProcessed = processed;
			SetChanged();
			NotifyObservers();
		}

		public virtual int GetChangesTotal()
		{
			return changesTotal;
		}

		public virtual void SetChangesTotal(int total)
		{
			this.changesTotal = total;
			SetChanged();
			NotifyObservers();
		}

		public virtual string GetSessionID()
		{
			return sessionID;
		}

		public virtual void Start()
		{
			if (running)
			{
				return;
			}
			this.sessionID = string.Format("repl%03d", ++lastSessionID);
			Log.V(TDDatabase.TAG, ToString() + " STARTING ...");
			running = true;
			lastSequence = null;
			FetchRemoteCheckpointDoc();
		}

		public abstract void BeginReplicating();

		public virtual void Stop()
		{
			if (!running)
			{
				return;
			}
			Log.V(TDDatabase.TAG, ToString() + " STOPPING...");
			batcher.Flush();
			continuous = false;
			if (asyncTaskCount == 0)
			{
				Stopped();
			}
		}

		public virtual void Stopped()
		{
			Log.V(TDDatabase.TAG, ToString() + " STOPPED");
			running = false;
			this.changesProcessed = this.changesTotal = 0;
			SaveLastSequence();
			batcher = null;
			db = null;
		}

		public virtual void AsyncTaskStarted()
		{
			lock (this)
			{
				++asyncTaskCount;
			}
		}

		public virtual void AsyncTaskFinished(int numTasks)
		{
			lock (this)
			{
				this.asyncTaskCount -= numTasks;
				if (asyncTaskCount == 0)
				{
					Stopped();
				}
			}
		}

		public virtual void AddToInbox(TDRevision rev)
		{
			if (batcher.Count() == 0)
			{
				active = true;
			}
			batcher.QueueObject(rev);
		}

		//Log.v(TDDatabase.TAG, String.format("%s: Received #%d %s", toString(), rev.getSequence(), rev.toString()));
		public virtual void ProcessInbox(TDRevisionList inbox)
		{
		}

		public virtual void SendAsyncRequest(string method, string relativePath, object body
			, TDRemoteRequestCompletionBlock onCompletion)
		{
			//Log.v(TDDatabase.TAG, String.format("%s: %s .%s", toString(), method, relativePath));
			string urlStr = remote.ToExternalForm() + relativePath;
			try
			{
				Uri url = new Uri(urlStr);
				TDRemoteRequest request = new TDRemoteRequest(db.GetHandler(), clientFacotry, method
					, url, body, onCompletion);
				request.Start();
			}
			catch (UriFormatException e)
			{
				Log.E(TDDatabase.TAG, "Malformed URL for async request", e);
			}
		}

		/// <summary>CHECKPOINT STORAGE:</summary>
		public virtual void MaybeCreateRemoteDB()
		{
		}

		// TDPusher overrides this to implement the .createTarget option
		/// <summary>This is the _local document ID stored on the remote server to keep track of state.
		/// 	</summary>
		/// <remarks>
		/// This is the _local document ID stored on the remote server to keep track of state.
		/// Its ID is based on the local database ID (the private one, to make the result unguessable)
		/// and the remote database's URL.
		/// </remarks>
		public virtual string RemoteCheckpointDocID()
		{
			if (db == null)
			{
				return null;
			}
			string input = db.PrivateUUID() + "\n" + remote.ToExternalForm() + "\n" + (IsPush
				() ? "1" : "0");
			return TDMisc.TDHexSHA1Digest(Sharpen.Runtime.GetBytesForString(input));
		}

		public virtual void FetchRemoteCheckpointDoc()
		{
			lastSequenceChanged = false;
			string localLastSequence = db.LastSequenceWithRemoteURL(remote, IsPush());
			if (localLastSequence == null)
			{
				MaybeCreateRemoteDB();
				BeginReplicating();
				return;
			}
			AsyncTaskStarted();
			SendAsyncRequest("GET", "/_local/" + RemoteCheckpointDocID(), null, new _TDRemoteRequestCompletionBlock_262
				(this, localLastSequence));
		}

		private sealed class _TDRemoteRequestCompletionBlock_262 : TDRemoteRequestCompletionBlock
		{
			public _TDRemoteRequestCompletionBlock_262(TDReplicator _enclosing, string localLastSequence
				)
			{
				this._enclosing = _enclosing;
				this.localLastSequence = localLastSequence;
			}

			public void OnCompletion(object result, Exception e)
			{
				if (e != null && e is HttpResponseException && ((HttpResponseException)e).GetStatusCode
					() != 404)
				{
					this._enclosing.error = e;
				}
				else
				{
					if (e is HttpResponseException && ((HttpResponseException)e).GetStatusCode() == 404)
					{
						this._enclosing.MaybeCreateRemoteDB();
					}
					IDictionary<string, object> response = (IDictionary<string, object>)result;
					this._enclosing.remoteCheckpoint = response;
					string remoteLastSequence = null;
					if (response != null)
					{
						remoteLastSequence = (string)response.Get("lastSequence");
					}
					if (remoteLastSequence != null && remoteLastSequence.Equals(localLastSequence))
					{
						this._enclosing.lastSequence = localLastSequence;
						Log.V(TDDatabase.TAG, this + ": Replicating from lastSequence=" + this._enclosing
							.lastSequence);
					}
					else
					{
						Log.V(TDDatabase.TAG, this + ": lastSequence mismatch: I had " + localLastSequence
							 + ", remote had " + remoteLastSequence);
					}
					this._enclosing.BeginReplicating();
				}
				this._enclosing.AsyncTaskFinished(1);
			}

			private readonly TDReplicator _enclosing;

			private readonly string localLastSequence;
		}

		public virtual void SaveLastSequence()
		{
			if (!lastSequenceChanged)
			{
				return;
			}
			if (savingCheckpoint)
			{
				// If a save is already in progress, don't do anything. (The completion block will trigger
				// another save after the first one finishes.)
				overdueForSave = true;
				return;
			}
			lastSequenceChanged = false;
			overdueForSave = false;
			Log.V(TDDatabase.TAG, this + " checkpointing sequence=" + lastSequence);
			IDictionary<string, object> body = new Dictionary<string, object>();
			if (remoteCheckpoint != null)
			{
				body.PutAll(remoteCheckpoint);
			}
			body.Put("lastSequence", lastSequence);
			string remoteCheckpointDocID = RemoteCheckpointDocID();
			if (remoteCheckpointDocID == null)
			{
				return;
			}
			savingCheckpoint = true;
			SendAsyncRequest("PUT", "/_local/" + remoteCheckpointDocID, body, new _TDRemoteRequestCompletionBlock_318
				(this, body));
			// TODO: If error is 401 or 403, and this is a pull, remember that remote is read-only and don't attempt to read its checkpoint next time.
			db.SetLastSequence(lastSequence, remote, IsPush());
		}

		private sealed class _TDRemoteRequestCompletionBlock_318 : TDRemoteRequestCompletionBlock
		{
			public _TDRemoteRequestCompletionBlock_318(TDReplicator _enclosing, IDictionary<string
				, object> body)
			{
				this._enclosing = _enclosing;
				this.body = body;
			}

			public void OnCompletion(object result, Exception e)
			{
				this._enclosing.savingCheckpoint = false;
				if (e != null)
				{
					Log.V(TDDatabase.TAG, this + ": Unable to save remote checkpoint", e);
				}
				else
				{
					IDictionary<string, object> response = (IDictionary<string, object>)result;
					body.Put("_rev", response.Get("rev"));
					this._enclosing.remoteCheckpoint = body;
				}
				if (this._enclosing.overdueForSave)
				{
					this._enclosing.SaveLastSequence();
				}
			}

			private readonly TDReplicator _enclosing;

			private readonly IDictionary<string, object> body;
		}
	}
}
