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
using System.IO;
using Android.OS;
using Android.Util;
using Couchbase.TouchDB;
using Couchbase.TouchDB.Support;
using Org.Apache.Http;
using Org.Apache.Http.Auth;
using Org.Apache.Http.Client;
using Org.Apache.Http.Client.Methods;
using Org.Apache.Http.Client.Protocol;
using Org.Apache.Http.Conn;
using Org.Apache.Http.Entity;
using Org.Apache.Http.Impl.Auth;
using Org.Apache.Http.Impl.Client;
using Org.Apache.Http.Protocol;
using Sharpen;

namespace Couchbase.TouchDB.Support
{
	public class TDRemoteRequest : Runnable
	{
		private Handler handler;

		private Sharpen.Thread thread;

		private readonly HttpClientFactory clientFactory;

		private string method;

		private Uri url;

		private object body;

		private TDRemoteRequestCompletionBlock onCompletion;

		public TDRemoteRequest(Handler handler, HttpClientFactory clientFactory, string method
			, Uri url, object body, TDRemoteRequestCompletionBlock onCompletion)
		{
			this.clientFactory = clientFactory;
			this.method = method;
			this.url = url;
			this.body = body;
			this.onCompletion = onCompletion;
			this.handler = handler;
		}

		public virtual void Start()
		{
			thread = new Sharpen.Thread(this, "RemoteRequest-" + url.ToExternalForm());
			thread.Start();
		}

		public virtual void Run()
		{
			HttpClient httpClient = clientFactory.GetHttpClient();
			ClientConnectionManager manager = httpClient.GetConnectionManager();
			HttpUriRequest request = null;
			if (Sharpen.Runtime.EqualsIgnoreCase(method, "GET"))
			{
				request = new HttpGet(url.ToExternalForm());
			}
			else
			{
				if (Sharpen.Runtime.EqualsIgnoreCase(method, "PUT"))
				{
					request = new HttpPut(url.ToExternalForm());
				}
				else
				{
					if (Sharpen.Runtime.EqualsIgnoreCase(method, "POST"))
					{
						request = new HttpPost(url.ToExternalForm());
					}
				}
			}
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
						HttpRequestInterceptor preemptiveAuth = new _HttpRequestInterceptor_88(creds);
						dhc.AddRequestInterceptor(preemptiveAuth, 0);
					}
				}
				else
				{
					Log.W(TDDatabase.TAG, "Unable to parse user info, not setting credentials");
				}
			}
			request.AddHeader("Accept", "application/json");
			//set body if appropriate
			if (body != null && request is HttpEntityEnclosingRequestBase)
			{
				byte[] bodyBytes = null;
				try
				{
					bodyBytes = TDServer.GetObjectMapper().WriteValueAsBytes(body);
				}
				catch (Exception e)
				{
					Log.E(TDDatabase.TAG, "Error serializing body of request", e);
				}
				ByteArrayEntity entity = new ByteArrayEntity(bodyBytes);
				entity.SetContentType("application/json");
				((HttpEntityEnclosingRequestBase)request).SetEntity(entity);
			}
			object fullBody = null;
			Exception error = null;
			try
			{
				HttpResponse response = httpClient.Execute(request);
				StatusLine status = response.GetStatusLine();
				if (status.GetStatusCode() >= 300)
				{
					Log.E(TDDatabase.TAG, "Got error " + Sharpen.Extensions.ToString(status.GetStatusCode
						()));
					Log.E(TDDatabase.TAG, "Request was for: " + request.ToString());
					Log.E(TDDatabase.TAG, "Status reason: " + status.GetReasonPhrase());
					error = new HttpResponseException(status.GetStatusCode(), status.GetReasonPhrase(
						));
				}
				else
				{
					HttpEntity temp = response.GetEntity();
					if (temp != null)
					{
						try
						{
							InputStream stream = temp.GetContent();
							fullBody = TDServer.GetObjectMapper().ReadValue<object>(stream);
						}
						finally
						{
							try
							{
								temp.ConsumeContent();
							}
							catch (IOException)
							{
							}
						}
					}
				}
			}
			catch (ClientProtocolException e)
			{
				error = e;
			}
			catch (IOException e)
			{
				error = e;
			}
			RespondWithResult(fullBody, error);
		}

		private sealed class _HttpRequestInterceptor_88 : HttpRequestInterceptor
		{
			public _HttpRequestInterceptor_88(Credentials creds)
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

		public virtual void RespondWithResult(object result, Exception error)
		{
			if (handler != null)
			{
				handler.Post(new _Runnable_161(this, result, error));
			}
		}

		private sealed class _Runnable_161 : Runnable
		{
			public _Runnable_161(TDRemoteRequest _enclosing, object result, Exception error)
			{
				this._enclosing = _enclosing;
				this.result = result;
				this.error = error;
				this.copy = this._enclosing.onCompletion;
			}

			internal TDRemoteRequestCompletionBlock copy;

			public void Run()
			{
				try
				{
					this._enclosing.onCompletion.OnCompletion(result, error);
				}
				catch (Exception e)
				{
					// don't let this crash the thread
					Log.E(TDDatabase.TAG, "TDRemoteRequestCompletionBlock throw Exception", e);
				}
			}

			private readonly TDRemoteRequest _enclosing;

			private readonly object result;

			private readonly Exception error;
		}
	}
}
