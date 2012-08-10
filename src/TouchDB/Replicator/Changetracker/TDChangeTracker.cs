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
using System.IO;
using Android.Util;
using Couchbase.TouchDB;
using Couchbase.TouchDB.Replicator.Changetracker;
using Org.Apache.Http;
using Org.Apache.Http.Auth;
using Org.Apache.Http.Client;
using Org.Apache.Http.Client.Methods;
using Org.Apache.Http.Client.Protocol;
using Org.Apache.Http.Impl.Auth;
using Org.Apache.Http.Impl.Client;
using Org.Apache.Http.Protocol;
using Sharpen;

namespace Couchbase.TouchDB.Replicator.Changetracker
{
	/// <summary>
	/// Reads the continuous-mode _changes feed of a database, and sends the
	/// individual change entries to its client's changeTrackerReceivedChange()
	/// </summary>
	public class TDChangeTracker : Runnable
	{
		private Uri databaseURL;

		private TDChangeTrackerClient client;

		private TDChangeTracker.TDChangeTrackerMode mode;

		private object lastSequenceID;

		private Sharpen.Thread thread;

		private bool running = false;

		private HttpUriRequest request;

		private string filterName;

		private IDictionary<string, object> filterParams;

		private Exception error;

		public enum TDChangeTrackerMode
		{
			OneShot,
			LongPoll,
			Continuous
		}

		public TDChangeTracker(Uri databaseURL, TDChangeTracker.TDChangeTrackerMode mode, 
			object lastSequenceID, TDChangeTrackerClient client)
		{
			this.databaseURL = databaseURL;
			this.mode = mode;
			this.lastSequenceID = lastSequenceID;
			this.client = client;
		}

		public virtual void SetFilterName(string filterName)
		{
			this.filterName = filterName;
		}

		public virtual void SetFilterParams(IDictionary<string, object> filterParams)
		{
			this.filterParams = filterParams;
		}

		public virtual void SetClient(TDChangeTrackerClient client)
		{
			this.client = client;
		}

		public virtual string GetDatabaseName()
		{
			string result = null;
			if (databaseURL != null)
			{
				result = databaseURL.AbsolutePath;
				if (result != null)
				{
					int pathLastSlashPos = result.LastIndexOf('/');
					if (pathLastSlashPos > 0)
					{
						result = Sharpen.Runtime.Substring(result, pathLastSlashPos);
					}
				}
			}
			return result;
		}

		public virtual string GetChangesFeedPath()
		{
			string path = "_changes?feed=";
			switch (mode)
			{
				case TDChangeTracker.TDChangeTrackerMode.OneShot:
				{
					path += "normal";
					break;
				}

				case TDChangeTracker.TDChangeTrackerMode.LongPoll:
				{
					path += "longpoll&limit=50";
					break;
				}

				case TDChangeTracker.TDChangeTrackerMode.Continuous:
				{
					path += "continuous";
					break;
				}
			}
			path += "&heartbeat=300000";
			if (lastSequenceID != null)
			{
				path += "&since=" + URLEncoder.Encode(lastSequenceID.ToString());
			}
			if (filterName != null)
			{
				path += "&filter=" + URLEncoder.Encode(filterName);
				if (filterParams != null)
				{
					foreach (string filterParamKey in filterParams.Keys)
					{
						path += "&" + URLEncoder.Encode(filterParamKey) + "=" + URLEncoder.Encode(filterParams
							.Get(filterParamKey).ToString());
					}
				}
			}
			return path;
		}

		public virtual Uri GetChangesFeedURL()
		{
			string dbURLString = databaseURL.ToExternalForm();
			if (!dbURLString.EndsWith("/"))
			{
				dbURLString += "/";
			}
			dbURLString += GetChangesFeedPath();
			Uri result = null;
			try
			{
				result = new Uri(dbURLString);
			}
			catch (UriFormatException e)
			{
				Log.E(TDDatabase.TAG, "Changes feed ULR is malformed", e);
			}
			return result;
		}

		public virtual void Run()
		{
			running = true;
			HttpClient httpClient = client.GetHttpClient();
			while (running)
			{
				Uri url = GetChangesFeedURL();
				request = new HttpGet(url.ToString());
				// if the URL contains user info AND if this a DefaultHttpClient
				// then preemptively set the auth credentials
				if (url.GetUserInfo() != null)
				{
					if (url.GetUserInfo().Contains(":"))
					{
						string[] userInfoSplit = url.GetUserInfo().Split(":");
						Credentials creds = new UsernamePasswordCredentials(userInfoSplit[0], userInfoSplit
							[1]);
						if (httpClient is DefaultHttpClient)
						{
							DefaultHttpClient dhc = (DefaultHttpClient)httpClient;
							HttpRequestInterceptor preemptiveAuth = new _HttpRequestInterceptor_161(creds);
							dhc.AddRequestInterceptor(preemptiveAuth, 0);
						}
					}
					else
					{
						Log.W(TDDatabase.TAG, "Unable to parse user info, not setting credentials");
					}
				}
				try
				{
					Log.V(TDDatabase.TAG, "Making request to " + GetChangesFeedURL().ToString());
					HttpResponse response = httpClient.Execute(request);
					StatusLine status = response.GetStatusLine();
					if (status.GetStatusCode() >= 300)
					{
						Log.E(TDDatabase.TAG, "Change tracker got error " + Sharpen.Extensions.ToString(status
							.GetStatusCode()));
						Stop();
					}
					HttpEntity entity = response.GetEntity();
					if (entity != null)
					{
						try
						{
							InputStream input = entity.GetContent();
							if (mode != TDChangeTracker.TDChangeTrackerMode.Continuous)
							{
								IDictionary<string, object> fullBody = TDServer.GetObjectMapper().ReadValue<IDictionary
									>(input);
								bool responseOK = ReceivedPollResponse(fullBody);
								if (mode == TDChangeTracker.TDChangeTrackerMode.LongPoll && responseOK)
								{
									Log.V(TDDatabase.TAG, "Starting new longpoll");
									continue;
								}
								else
								{
									Log.W(TDDatabase.TAG, "Change tracker calling stop");
									Stop();
								}
							}
							else
							{
								BufferedReader reader = new BufferedReader(new InputStreamReader(input));
								string line = null;
								while ((line = reader.ReadLine()) != null)
								{
									ReceivedChunk(line);
								}
							}
						}
						finally
						{
							try
							{
								entity.ConsumeContent();
							}
							catch (IOException)
							{
							}
						}
					}
				}
				catch (ClientProtocolException e)
				{
					Log.E(TDDatabase.TAG, "ClientProtocolException in change tracker", e);
				}
				catch (IOException e)
				{
					if (running)
					{
						//we get an exception when we're shutting down and have to
						//close the socket underneath our read, ignore that
						Log.E(TDDatabase.TAG, "IOException in change tracker", e);
					}
				}
			}
			Log.V(TDDatabase.TAG, "Change tracker run loop exiting");
		}

		private sealed class _HttpRequestInterceptor_161 : HttpRequestInterceptor
		{
			public _HttpRequestInterceptor_161(Credentials creds)
			{
				this.creds = creds;
			}

			/// <exception cref="Org.Apache.Http.HttpException"></exception>
			/// <exception cref="System.IO.IOException"></exception>
			public void Process(HttpRequest request, HttpContext context)
			{
				AuthState authState = (AuthState)context.GetAttribute(ClientContext.TARGET_AUTH_STATE
					);
				CredentialsProvider credsProvider = (CredentialsProvider)context.GetAttribute(ClientContext
					.CREDS_PROVIDER);
				HttpHost targetHost = (HttpHost)context.GetAttribute(ExecutionContext.HTTP_TARGET_HOST
					);
				if (authState.GetAuthScheme() == null)
				{
					AuthScope authScope = new AuthScope(targetHost.GetHostName(), targetHost.GetPort(
						));
					authState.SetAuthScheme(new BasicScheme());
					authState.SetCredentials(creds);
				}
			}

			private readonly Credentials creds;
		}

		public virtual bool ReceivedChunk(string line)
		{
			if (line.Length > 1)
			{
				try
				{
					IDictionary<string, object> change = (IDictionary)TDServer.GetObjectMapper().ReadValue
						<IDictionary>(line);
					if (!ReceivedChange(change))
					{
						Log.W(TDDatabase.TAG, string.Format("Received unparseable change line from server: %s"
							, line));
						return false;
					}
				}
				catch (Exception e)
				{
					Log.W(TDDatabase.TAG, "Exception parsing JSON in change tracker", e);
					return false;
				}
			}
			return true;
		}

		public virtual bool ReceivedChange(IDictionary<string, object> change)
		{
			object seq = change.Get("seq");
			if (seq == null)
			{
				return false;
			}
			//pass the change to the client on the thread that created this change tracker
			if (client != null)
			{
				client.ChangeTrackerReceivedChange(change);
			}
			lastSequenceID = seq;
			return true;
		}

		public virtual bool ReceivedPollResponse(IDictionary<string, object> response)
		{
			IList<IDictionary<string, object>> changes = (IList)response.Get("results");
			if (changes == null)
			{
				return false;
			}
			foreach (IDictionary<string, object> change in changes)
			{
				if (!ReceivedChange(change))
				{
					return false;
				}
			}
			return true;
		}

		public virtual void SetUpstreamError(string message)
		{
			Log.W(TDDatabase.TAG, string.Format("Server error: %s", message));
			this.error = new Exception(message);
		}

		public virtual bool Start()
		{
			this.error = null;
			thread = new Sharpen.Thread(this, "ChangeTracker-" + databaseURL.ToExternalForm()
				);
			thread.Start();
			return true;
		}

		public virtual void Stop()
		{
			Log.D(TDDatabase.TAG, "changed tracker asked to stop");
			running = false;
			thread.Interrupt();
			if (request != null)
			{
				request.Abort();
			}
			Stopped();
		}

		public virtual void Stopped()
		{
			Log.D(TDDatabase.TAG, "change tracker in stopped");
			if (client != null)
			{
				Log.D(TDDatabase.TAG, "posting stopped");
				client.ChangeTrackerStopped(this);
			}
			client = null;
			Log.D(TDDatabase.TAG, "change tracker client should be null now");
		}

		public virtual bool IsRunning()
		{
			return running;
		}
	}
}
