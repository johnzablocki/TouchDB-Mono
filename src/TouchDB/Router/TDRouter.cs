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
using System.Reflection;
using System.Text;
using Android.Util;
using Couchbase.TouchDB;
using Couchbase.TouchDB.Replicator;
using Couchbase.TouchDB.Router;
using Sharpen;

namespace Couchbase.TouchDB.Router
{
	public class TDRouter : Observer
	{
		private TDServer server;

		private TDDatabase db;

		private TDURLConnection connection;

		private IDictionary<string, string> queries;

		private bool changesIncludesDocs = false;

		private TDRouterCallbackBlock callbackBlock;

		private bool responseSent = false;

		private bool waiting = false;

		private TDFilterBlock changesFilter;

		private bool longpoll = false;

		public static string GetVersionString()
		{
			return TouchDBVersion.TouchDBVersionNumber;
		}

		public TDRouter(TDServer server, TDURLConnection connection)
		{
			this.server = server;
			this.connection = connection;
		}

		public virtual void SetCallbackBlock(TDRouterCallbackBlock callbackBlock)
		{
			this.callbackBlock = callbackBlock;
		}

		public virtual IDictionary<string, string> GetQueries()
		{
			if (queries == null)
			{
				string queryString = connection.GetURL().GetQuery();
				if (queryString != null && queryString.Length > 0)
				{
					queries = new Dictionary<string, string>();
					foreach (string component in queryString.Split("&"))
					{
						int location = component.IndexOf('=');
						if (location > 0)
						{
							string key = Sharpen.Runtime.Substring(component, 0, location);
							string value = Sharpen.Runtime.Substring(component, location + 1);
							queries.Put(key, value);
						}
					}
				}
			}
			return queries;
		}

		public virtual string GetQuery(string param)
		{
			IDictionary<string, string> queries = GetQueries();
			if (queries != null)
			{
				string value = queries.Get(param);
				if (value != null)
				{
					return URLDecoder.Decode(value);
				}
			}
			return null;
		}

		public virtual bool GetBooleanQuery(string param)
		{
			string value = GetQuery(param);
			return (value != null) && !"false".Equals(value) && !"0".Equals(value);
		}

		public virtual int GetIntQuery(string param, int defaultValue)
		{
			int result = defaultValue;
			string value = GetQuery(param);
			if (value != null)
			{
				try
				{
					result = System.Convert.ToInt32(value);
				}
				catch (FormatException)
				{
				}
			}
			//ignore, will return default value
			return result;
		}

		public virtual object GetJSONQuery(string param)
		{
			string value = GetQuery(param);
			if (value == null)
			{
				return null;
			}
			object result = null;
			try
			{
				result = TDServer.GetObjectMapper().ReadValue<object>(value);
			}
			catch (Exception e)
			{
				Log.W("Unable to parse JSON Query", e);
			}
			return result;
		}

		public virtual bool CacheWithEtag(string etag)
		{
			string eTag = string.Format("\"%s\"", etag);
			connection.GetResHeader().Add("Etag", eTag);
			string requestIfNoneMatch = connection.GetRequestProperty("If-None-Match");
			return eTag.Equals(requestIfNoneMatch);
		}

		public virtual IDictionary<string, object> GetBodyAsDictionary()
		{
			try
			{
				InputStream contentStream = connection.GetRequestInputStream();
				IDictionary<string, object> bodyMap = TDServer.GetObjectMapper().ReadValue<IDictionary
					>(contentStream);
				return bodyMap;
			}
			catch (IOException)
			{
				return null;
			}
		}

		public virtual EnumSet<TDDatabase.TDContentOptions> GetContentOptions()
		{
			EnumSet<TDDatabase.TDContentOptions> result = EnumSet.NoneOf<TDDatabase.TDContentOptions
				>();
			if (GetBooleanQuery("attachments"))
			{
				result.AddItem(TDDatabase.TDContentOptions.TDIncludeAttachments);
			}
			if (GetBooleanQuery("local_seq"))
			{
				result.AddItem(TDDatabase.TDContentOptions.TDIncludeLocalSeq);
			}
			if (GetBooleanQuery("conflicts"))
			{
				result.AddItem(TDDatabase.TDContentOptions.TDIncludeConflicts);
			}
			if (GetBooleanQuery("revs"))
			{
				result.AddItem(TDDatabase.TDContentOptions.TDIncludeRevs);
			}
			if (GetBooleanQuery("revs_info"))
			{
				result.AddItem(TDDatabase.TDContentOptions.TDIncludeRevsInfo);
			}
			return result;
		}

		public virtual bool GetQueryOptions(TDQueryOptions options)
		{
			// http://wiki.apache.org/couchdb/HTTP_view_API#Querying_Options
			options.SetSkip(GetIntQuery("skip", options.GetSkip()));
			options.SetLimit(GetIntQuery("limit", options.GetLimit()));
			options.SetGroupLevel(GetIntQuery("group_level", options.GetGroupLevel()));
			options.SetDescending(GetBooleanQuery("descending"));
			options.SetIncludeDocs(GetBooleanQuery("include_docs"));
			options.SetUpdateSeq(GetBooleanQuery("update_seq"));
			if (GetQuery("inclusive_end") != null)
			{
				options.SetInclusiveEnd(GetBooleanQuery("inclusive_end"));
			}
			if (GetQuery("reduce") != null)
			{
				options.SetReduce(GetBooleanQuery("reduce"));
			}
			options.SetGroup(GetBooleanQuery("group"));
			options.SetContentOptions(GetContentOptions());
			options.SetStartKey(GetJSONQuery("startkey"));
			options.SetEndKey(GetJSONQuery("endkey"));
			object key = GetJSONQuery("key");
			if (key != null)
			{
				IList<object> keys = new AList<object>();
				keys.AddItem(key);
				options.SetKeys(keys);
			}
			return true;
		}

		public virtual string GetMultipartRequestType()
		{
			string accept = connection.GetRequestProperty("Accept");
			if (accept.StartsWith("multipart/"))
			{
				return accept;
			}
			return null;
		}

		public virtual TDStatus OpenDB()
		{
			if (db == null)
			{
				return new TDStatus(TDStatus.INTERNAL_SERVER_ERROR);
			}
			if (!db.Exists())
			{
				return new TDStatus(TDStatus.NOT_FOUND);
			}
			if (!db.Open())
			{
				return new TDStatus(TDStatus.INTERNAL_SERVER_ERROR);
			}
			return new TDStatus(TDStatus.OK);
		}

		public static IList<string> SplitPath(Uri url)
		{
			string pathString = url.AbsolutePath;
			if (pathString.StartsWith("/"))
			{
				pathString = Sharpen.Runtime.Substring(pathString, 1);
			}
			IList<string> result = new AList<string>();
			//we want empty string to return empty list
			if (pathString.Length == 0)
			{
				return result;
			}
			foreach (string component in pathString.Split("/"))
			{
				result.AddItem(URLDecoder.Decode(component));
			}
			return result;
		}

		public virtual void SendResponse()
		{
			if (!responseSent)
			{
				responseSent = true;
				if (callbackBlock != null)
				{
					callbackBlock.OnResponseReady();
				}
			}
		}

		public virtual void Start()
		{
			// Refer to: http://wiki.apache.org/couchdb/Complete_HTTP_API_Reference
			// We're going to map the request into a method call using reflection based on the method and path.
			// Accumulate the method name into the string 'message':
			string method = connection.GetRequestMethod();
			if ("HEAD".Equals(method))
			{
				method = "GET";
			}
			string message = string.Format("do_%s", method);
			// First interpret the components of the request:
			IList<string> path = SplitPath(connection.GetURL());
			if (path == null)
			{
				connection.SetResponseCode(TDStatus.BAD_REQUEST);
				try
				{
					connection.GetResponseOutputStream().Close();
				}
				catch (IOException)
				{
					Log.E(TDDatabase.TAG, "Error closing empty output stream");
				}
				SendResponse();
				return;
			}
			int pathLen = path.Count;
			if (pathLen > 0)
			{
				string dbName = path[0];
				if (dbName.StartsWith("_"))
				{
					message += dbName;
				}
				else
				{
					// special root path, like /_all_dbs
					message += "_Database";
					db = server.GetDatabaseNamed(dbName);
					if (db == null)
					{
						connection.SetResponseCode(TDStatus.BAD_REQUEST);
						try
						{
							connection.GetResponseOutputStream().Close();
						}
						catch (IOException)
						{
							Log.E(TDDatabase.TAG, "Error closing empty output stream");
						}
						SendResponse();
						return;
					}
				}
			}
			else
			{
				message += "Root";
			}
			string docID = null;
			if (db != null && pathLen > 1)
			{
				message = message.ReplaceFirst("_Database", "_Document");
				// Make sure database exists, then interpret doc name:
				TDStatus status = OpenDB();
				if (!status.IsSuccessful())
				{
					connection.SetResponseCode(status.GetCode());
					try
					{
						connection.GetResponseOutputStream().Close();
					}
					catch (IOException)
					{
						Log.E(TDDatabase.TAG, "Error closing empty output stream");
					}
					SendResponse();
					return;
				}
				string name = path[1];
				if (!name.StartsWith("_"))
				{
					// Regular document
					if (!TDDatabase.IsValidDocumentId(name))
					{
						connection.SetResponseCode(TDStatus.BAD_REQUEST);
						try
						{
							connection.GetResponseOutputStream().Close();
						}
						catch (IOException)
						{
							Log.E(TDDatabase.TAG, "Error closing empty output stream");
						}
						SendResponse();
						return;
					}
					docID = name;
				}
				else
				{
					if ("_design".Equals(name) || "_local".Equals(name))
					{
						// "_design/____" and "_local/____" are document names
						if (pathLen <= 2)
						{
							connection.SetResponseCode(TDStatus.NOT_FOUND);
							try
							{
								connection.GetResponseOutputStream().Close();
							}
							catch (IOException)
							{
								Log.E(TDDatabase.TAG, "Error closing empty output stream");
							}
							SendResponse();
							return;
						}
						docID = name + "/" + path[2];
						path.Set(1, docID);
						path.Remove(2);
						pathLen--;
					}
					else
					{
						if (name.StartsWith("_design") || name.StartsWith("_local"))
						{
							// This is also a document, just with a URL-encoded "/"
							docID = name;
						}
						else
						{
							// Special document name like "_all_docs":
							message += name;
							if (pathLen > 2)
							{
								IList<string> subList = path.SubList(2, pathLen - 1);
								StringBuilder sb = new StringBuilder();
								Iterator<string> iter = subList.Iterator();
								while (iter.HasNext())
								{
									sb.Append(iter.Next());
									if (iter.HasNext())
									{
										sb.Append("/");
									}
								}
								docID = sb.ToString();
							}
						}
					}
				}
			}
			string attachmentName = null;
			if (docID != null && pathLen > 2)
			{
				message = message.ReplaceFirst("_Document", "_Attachment");
				// Interpret attachment name:
				attachmentName = path[2];
				if (attachmentName.StartsWith("_") && docID.StartsWith("_design"))
				{
					// Design-doc attribute like _info or _view
					message = message.ReplaceFirst("_Attachment", "_DesignDocument");
					docID = Sharpen.Runtime.Substring(docID, 8);
					// strip the "_design/" prefix
					attachmentName = pathLen > 3 ? path[3] : null;
				}
				else
				{
					if (pathLen > 3)
					{
						IList<string> subList = path.SubList(2, pathLen);
						StringBuilder sb = new StringBuilder();
						Iterator<string> iter = subList.Iterator();
						while (iter.HasNext())
						{
							sb.Append(iter.Next());
							if (iter.HasNext())
							{
								//sb.append("%2F");
								sb.Append("/");
							}
						}
						attachmentName = sb.ToString();
					}
				}
			}
			//Log.d(TAG, "path: " + path + " message: " + message + " docID: " + docID + " attachmentName: " + attachmentName);
			// Send myself a message based on the components:
			TDStatus status_1 = new TDStatus(TDStatus.INTERNAL_SERVER_ERROR);
			try
			{
				MethodInfo m = this.GetType().GetMethod(message, typeof(TDDatabase), typeof(string
					), typeof(string));
				status_1 = (TDStatus)m.Invoke(this, db, docID, attachmentName);
			}
			catch (NoSuchMethodException)
			{
				try
				{
					MethodInfo m = this.GetType().GetMethod("do_UNKNOWN", typeof(TDDatabase), typeof(
						string), typeof(string));
					status_1 = (TDStatus)m.Invoke(this, db, docID, attachmentName);
				}
				catch (Exception)
				{
				}
			}
			catch (Exception e)
			{
				//default status is internal server error
				//default status is internal server error
				Log.E(TDDatabase.TAG, "Exception in TDRouter", e);
			}
			// Configure response headers:
			if (status_1.IsSuccessful() && connection.GetResponseBody() == null && connection
				.GetHeaderField("Content-Type") == null)
			{
				connection.SetResponseBody(new TDBody(Sharpen.Runtime.GetBytesForString("{\"ok\":true}"
					)));
			}
			if (connection.GetResponseBody() != null && connection.GetResponseBody().IsValidJSON
				())
			{
				connection.GetResHeader().Add("Content-Type", "application/json");
			}
			// Check for a mismatch between the Accept request header and the response type:
			string accept = connection.GetRequestProperty("Accept");
			if (accept != null && !"*/*".Equals(accept))
			{
				string responseType = connection.GetBaseContentType();
				if (responseType != null && accept.IndexOf(responseType) < 0)
				{
					Log.E(TDDatabase.TAG, string.Format("Error 406: Can't satisfy request Accept: %s"
						, accept));
					status_1 = new TDStatus(TDStatus.NOT_ACCEPTABLE);
				}
			}
			connection.GetResHeader().Add("Server", string.Format("TouchDB %s", GetVersionString
				()));
			// If response is ready (nonzero status), tell my client about it:
			if (status_1.GetCode() != 0)
			{
				connection.SetResponseCode(status_1.GetCode());
				if (connection.GetResponseBody() != null)
				{
					ByteArrayInputStream bais = new ByteArrayInputStream(connection.GetResponseBody()
						.GetJson());
					connection.SetResponseInputStream(bais);
				}
				else
				{
					try
					{
						connection.GetResponseOutputStream().Close();
					}
					catch (IOException)
					{
						Log.E(TDDatabase.TAG, "Error closing empty output stream");
					}
				}
				SendResponse();
			}
		}

		public virtual void Stop()
		{
			callbackBlock = null;
			if (db != null)
			{
				db.DeleteObserver(this);
			}
		}

		public virtual TDStatus Do_UNKNOWN(TDDatabase db, string docID, string attachmentName
			)
		{
			return new TDStatus(TDStatus.BAD_REQUEST);
		}

		public virtual void SetResponseLocation(Uri url)
		{
			string location = url.ToExternalForm();
			string query = url.GetQuery();
			if (query != null)
			{
				int startOfQuery = location.IndexOf(query);
				if (startOfQuery > 0)
				{
					location = Sharpen.Runtime.Substring(location, 0, startOfQuery);
				}
			}
			connection.GetResHeader().Add("Location", location);
		}

		/// <summary>SERVER REQUESTS:</summary>
		public virtual TDStatus Do_GETRoot(TDDatabase _db, string _docID, string _attachmentName
			)
		{
			IDictionary<string, object> info = new Dictionary<string, object>();
			info.Put("TouchDB", "Welcome");
			info.Put("couchdb", "Welcome");
			// for compatibility
			info.Put("version", GetVersionString());
			connection.SetResponseBody(new TDBody(info));
			return new TDStatus(TDStatus.OK);
		}

		public virtual TDStatus Do_GET_all_dbs(TDDatabase _db, string _docID, string _attachmentName
			)
		{
			IList<string> dbs = server.AllDatabaseNames();
			connection.SetResponseBody(new TDBody(dbs));
			return new TDStatus(TDStatus.OK);
		}

		public virtual TDStatus Do_GET_session(TDDatabase _db, string _docID, string _attachmentName
			)
		{
			// Send back an "Admin Party"-like response
			IDictionary<string, object> session = new Dictionary<string, object>();
			IDictionary<string, object> userCtx = new Dictionary<string, object>();
			string[] roles = new string[] { "_admin" };
			session.Put("ok", true);
			userCtx.Put("name", null);
			userCtx.Put("roles", roles);
			session.Put("userCtx", userCtx);
			connection.SetResponseBody(new TDBody(session));
			return new TDStatus(TDStatus.OK);
		}

		public virtual TDStatus Do_POST_replicate(TDDatabase _db, string _docID, string _attachmentName
			)
		{
			// Extract the parameters from the JSON request body:
			// http://wiki.apache.org/couchdb/Replication
			IDictionary<string, object> body = GetBodyAsDictionary();
			if (body == null)
			{
				return new TDStatus(TDStatus.BAD_REQUEST);
			}
			string source = (string)body.Get("source");
			string target = (string)body.Get("target");
			bool createTargetBoolean = (bool)body.Get("create_target");
			bool createTarget = (createTargetBoolean != null && createTargetBoolean);
			bool continuousBoolean = (bool)body.Get("continuous");
			bool continuous = (continuousBoolean != null && continuousBoolean);
			bool cancelBoolean = (bool)body.Get("cancel");
			bool cancel = (cancelBoolean != null && cancelBoolean);
			// Map the 'source' and 'target' JSON params to a local database and remote URL:
			if (source == null || target == null)
			{
				return new TDStatus(TDStatus.BAD_REQUEST);
			}
			bool push = false;
			TDDatabase db = server.GetExistingDatabaseNamed(source);
			string remoteStr = null;
			if (db != null)
			{
				remoteStr = target;
				push = true;
			}
			else
			{
				remoteStr = source;
				if (createTarget && !cancel)
				{
					db = server.GetDatabaseNamed(target);
					if (!db.Open())
					{
						return new TDStatus(TDStatus.INTERNAL_SERVER_ERROR);
					}
				}
				else
				{
					db = server.GetExistingDatabaseNamed(target);
				}
				if (db == null)
				{
					return new TDStatus(TDStatus.NOT_FOUND);
				}
			}
			Uri remote = null;
			try
			{
				remote = new Uri(remoteStr);
			}
			catch (UriFormatException)
			{
				return new TDStatus(TDStatus.BAD_REQUEST);
			}
			if (remote == null || !remote.Scheme.StartsWith("http"))
			{
				return new TDStatus(TDStatus.BAD_REQUEST);
			}
			if (!cancel)
			{
				// Start replication:
				TDReplicator repl = db.GetReplicator(remote, server.GetDefaultHttpClientFactory()
					, push, continuous);
				if (repl == null)
				{
					return new TDStatus(TDStatus.INTERNAL_SERVER_ERROR);
				}
				string filterName = (string)body.Get("filter");
				if (filterName != null)
				{
					repl.SetFilterName(filterName);
					IDictionary<string, object> filterParams = (IDictionary<string, object>)body.Get(
						"query_params");
					if (filterParams != null)
					{
						repl.SetFilterParams(filterParams);
					}
				}
				if (push)
				{
					((TDPusher)repl).SetCreateTarget(createTarget);
				}
				repl.Start();
				IDictionary<string, object> result = new Dictionary<string, object>();
				result.Put("session_id", repl.GetSessionID());
				connection.SetResponseBody(new TDBody(result));
			}
			else
			{
				// Cancel replication:
				TDReplicator repl = db.GetActiveReplicator(remote, push);
				if (repl == null)
				{
					return new TDStatus(TDStatus.NOT_FOUND);
				}
				repl.Stop();
			}
			return new TDStatus(TDStatus.OK);
		}

		public virtual TDStatus Do_GET_uuids(TDDatabase _db, string _docID, string _attachmentName
			)
		{
			int count = Math.Min(1000, GetIntQuery("count", 1));
			IList<string> uuids = new AList<string>(count);
			for (int i = 0; i < count; i++)
			{
				uuids.AddItem(TDDatabase.GenerateDocumentId());
			}
			IDictionary<string, object> result = new Dictionary<string, object>();
			result.Put("uuids", uuids);
			connection.SetResponseBody(new TDBody(result));
			return new TDStatus(TDStatus.OK);
		}

		public virtual TDStatus Do_GET_active_tasks(TDDatabase _db, string _docID, string
			 _attachmentName)
		{
			// http://wiki.apache.org/couchdb/HttpGetActiveTasks
			IList<IDictionary<string, object>> activities = new AList<IDictionary<string, object
				>>();
			foreach (TDDatabase db in server.AllOpenDatabases())
			{
				IList<TDReplicator> activeReplicators = db.GetActiveReplicators();
				if (activeReplicators != null)
				{
					foreach (TDReplicator replicator in activeReplicators)
					{
						string source = replicator.GetRemote().ToExternalForm();
						string target = db.GetName();
						if (replicator.IsPush())
						{
							string tmp = source;
							source = target;
							target = tmp;
						}
						int processed = replicator.GetChangesProcessed();
						int total = replicator.GetChangesTotal();
						string status = string.Format("Processed %d / %d changes", processed, total);
						int progress = (total > 0) ? Math.Round(100 * processed / (float)total) : 0;
						IDictionary<string, object> activity = new Dictionary<string, object>();
						activity.Put("type", "Replication");
						activity.Put("task", replicator.GetSessionID());
						activity.Put("source", source);
						activity.Put("target", target);
						activity.Put("status", status);
						activity.Put("progress", progress);
						activities.AddItem(activity);
					}
				}
			}
			connection.SetResponseBody(new TDBody(activities));
			return new TDStatus(TDStatus.OK);
		}

		/// <summary>DATABASE REQUESTS:</summary>
		public virtual TDStatus Do_GET_Database(TDDatabase _db, string _docID, string _attachmentName
			)
		{
			// http://wiki.apache.org/couchdb/HTTP_database_API#Database_Information
			TDStatus status = OpenDB();
			if (!status.IsSuccessful())
			{
				return status;
			}
			int num_docs = db.GetDocumentCount();
			long update_seq = db.GetLastSequence();
			IDictionary<string, object> result = new Dictionary<string, object>();
			result.Put("db_name", db.GetName());
			result.Put("db_uuid", db.PublicUUID());
			result.Put("doc_count", num_docs);
			result.Put("update_seq", update_seq);
			result.Put("disk_size", db.TotalDataSize());
			connection.SetResponseBody(new TDBody(result));
			return new TDStatus(TDStatus.OK);
		}

		public virtual TDStatus Do_PUT_Database(TDDatabase _db, string _docID, string _attachmentName
			)
		{
			if (db.Exists())
			{
				return new TDStatus(TDStatus.PRECONDITION_FAILED);
			}
			if (!db.Open())
			{
				return new TDStatus(TDStatus.INTERNAL_SERVER_ERROR);
			}
			SetResponseLocation(connection.GetURL());
			return new TDStatus(TDStatus.CREATED);
		}

		public virtual TDStatus Do_DELETE_Database(TDDatabase _db, string _docID, string 
			_attachmentName)
		{
			if (GetQuery("rev") != null)
			{
				return new TDStatus(TDStatus.BAD_REQUEST);
			}
			// CouchDB checks for this; probably meant to be a document deletion
			return server.DeleteDatabaseNamed(db.GetName()) ? new TDStatus(TDStatus.OK) : new 
				TDStatus(TDStatus.NOT_FOUND);
		}

		public virtual TDStatus Do_POST_Database(TDDatabase _db, string _docID, string _attachmentName
			)
		{
			TDStatus status = OpenDB();
			if (!status.IsSuccessful())
			{
				return status;
			}
			return Update(db, null, GetBodyAsDictionary(), false);
		}

		public virtual TDStatus Do_GET_Document_all_docs(TDDatabase _db, string _docID, string
			 _attachmentName)
		{
			TDQueryOptions options = new TDQueryOptions();
			if (!GetQueryOptions(options))
			{
				return new TDStatus(TDStatus.BAD_REQUEST);
			}
			IDictionary<string, object> result = db.GetAllDocs(options);
			if (result == null)
			{
				return new TDStatus(TDStatus.INTERNAL_SERVER_ERROR);
			}
			connection.SetResponseBody(new TDBody(result));
			return new TDStatus(TDStatus.OK);
		}

		public virtual TDStatus Do_POST_Document_all_docs(TDDatabase _db, string _docID, 
			string _attachmentName)
		{
			TDQueryOptions options = new TDQueryOptions();
			if (!GetQueryOptions(options))
			{
				return new TDStatus(TDStatus.BAD_REQUEST);
			}
			IDictionary<string, object> body = GetBodyAsDictionary();
			if (body == null)
			{
				return new TDStatus(TDStatus.BAD_REQUEST);
			}
			IDictionary<string, object> result = null;
			if (body.ContainsKey("keys") && body.Get("keys") is ArrayList)
			{
				AList<string> keys = (AList<string>)body.Get("keys");
				result = db.GetDocsWithIDs(keys, options);
			}
			else
			{
				result = db.GetAllDocs(options);
			}
			if (result == null)
			{
				return new TDStatus(TDStatus.INTERNAL_SERVER_ERROR);
			}
			connection.SetResponseBody(new TDBody(result));
			return new TDStatus(TDStatus.OK);
		}

		public virtual TDStatus Do_POST_Document_bulk_docs(TDDatabase _db, string _docID, 
			string _attachmentName)
		{
			IDictionary<string, object> bodyDict = GetBodyAsDictionary();
			if (bodyDict == null)
			{
				return new TDStatus(TDStatus.BAD_REQUEST);
			}
			IList<IDictionary<string, object>> docs = (IList<IDictionary<string, object>>)bodyDict
				.Get("docs");
			bool allObj = false;
			if (GetQuery("all_or_nothing") == null || (GetQuery("all_or_nothing") != null && 
				(System.Convert.ToBoolean(GetQuery("all_or_nothing")))))
			{
				allObj = true;
			}
			//   allowConflict If false, an error status 409 will be returned if the insertion would create a conflict, i.e. if the previous revision already has a child.
			bool allOrNothing = (allObj && allObj != false);
			bool noNewEdits = true;
			if (GetQuery("new_edits") == null || (GetQuery("new_edits") != null && (System.Convert.ToBoolean
				(GetQuery("new_edits")))))
			{
				noNewEdits = false;
			}
			bool ok = false;
			db.BeginTransaction();
			IList<IDictionary<string, object>> results = new AList<IDictionary<string, object
				>>();
			try
			{
				foreach (IDictionary<string, object> doc in docs)
				{
					string docID = (string)doc.Get("_id");
					TDRevision rev = null;
					TDStatus status = new TDStatus(TDStatus.BAD_REQUEST);
					TDBody docBody = new TDBody(doc);
					if (noNewEdits)
					{
						rev = new TDRevision(docBody);
						if (rev.GetRevId() == null || rev.GetDocId() == null || !rev.GetDocId().Equals(docID
							))
						{
							status = new TDStatus(TDStatus.BAD_REQUEST);
						}
						else
						{
							IList<string> history = TDDatabase.ParseCouchDBRevisionHistory(doc);
							status = db.ForceInsert(rev, history, null);
						}
					}
					else
					{
						TDStatus outStatus = new TDStatus();
						rev = Update(db, docID, docBody, false, allOrNothing, outStatus);
						status.SetCode(outStatus.GetCode());
					}
					IDictionary<string, object> result = null;
					if (status.IsSuccessful())
					{
						result = new Dictionary<string, object>();
						result.Put("ok", true);
						result.Put("id", docID);
						if (rev != null)
						{
							result.Put("rev", rev.GetRevId());
						}
					}
					else
					{
						if (allOrNothing)
						{
							return status;
						}
						else
						{
							// all_or_nothing backs out if there's any error
							if (status.GetCode() == TDStatus.FORBIDDEN)
							{
								result = new Dictionary<string, object>();
								result.Put("error", "validation failed");
								result.Put("id", docID);
							}
							else
							{
								if (status.GetCode() == TDStatus.CONFLICT)
								{
									result = new Dictionary<string, object>();
									result.Put("error", "conflict");
									result.Put("id", docID);
								}
								else
								{
									return status;
								}
							}
						}
					}
					// abort the whole thing if something goes badly wrong
					if (result != null)
					{
						results.AddItem(result);
					}
				}
				Log.W(TDDatabase.TAG, string.Format("%s finished inserting %d revisions in bulk", 
					this, docs.Count));
				ok = true;
			}
			catch (Exception e)
			{
				Log.W(TDDatabase.TAG, string.Format("%s: Exception inserting revisions in bulk", 
					this), e);
			}
			finally
			{
				db.EndTransaction(ok);
			}
			Log.D(TDDatabase.TAG, "results: " + results.ToString());
			connection.SetResponseBody(new TDBody(results));
			return new TDStatus(TDStatus.CREATED);
		}

		public virtual TDStatus Do_POST_Document_revs_diff(TDDatabase _db, string _docID, 
			string _attachmentName)
		{
			// http://wiki.apache.org/couchdb/HttpPostRevsDiff
			// Collect all of the input doc/revision IDs as TDRevisions:
			TDRevisionList revs = new TDRevisionList();
			IDictionary<string, object> body = GetBodyAsDictionary();
			if (body == null)
			{
				return new TDStatus(TDStatus.BAD_JSON);
			}
			foreach (string docID in body.Keys)
			{
				IList<string> revIDs = (IList<string>)body.Get(docID);
				foreach (string revID in revIDs)
				{
					TDRevision rev = new TDRevision(docID, revID, false);
					revs.AddItem(rev);
				}
			}
			// Look them up, removing the existing ones from revs:
			if (!db.FindMissingRevisions(revs))
			{
				return new TDStatus(TDStatus.DB_ERROR);
			}
			// Return the missing revs in a somewhat different format:
			IDictionary<string, object> diffs = new Dictionary<string, object>();
			foreach (TDRevision rev_1 in revs)
			{
				string docID_1 = rev_1.GetDocId();
				IList<string> missingRevs = null;
				IDictionary<string, object> idObj = (IDictionary<string, object>)diffs.Get(docID_1
					);
				if (idObj != null)
				{
					missingRevs = (IList<string>)idObj.Get("missing");
				}
				else
				{
					idObj = new Dictionary<string, object>();
				}
				if (missingRevs == null)
				{
					missingRevs = new AList<string>();
					idObj.Put("missing", missingRevs);
					diffs.Put(docID_1, idObj);
				}
				missingRevs.AddItem(rev_1.GetRevId());
			}
			// FIXME add support for possible_ancestors
			connection.SetResponseBody(new TDBody(diffs));
			return new TDStatus(TDStatus.OK);
		}

		public virtual TDStatus Do_POST_Document_compact(TDDatabase _db, string _docID, string
			 _attachmentName)
		{
			TDStatus status = _db.Compact();
			if (status.GetCode() < 300)
			{
				TDStatus outStatus = new TDStatus();
				outStatus.SetCode(202);
				// CouchDB returns 202 'cause it's an async operation
				return outStatus;
			}
			else
			{
				return status;
			}
		}

		public virtual TDStatus Do_POST_Document_ensure_full_commit(TDDatabase _db, string
			 _docID, string _attachmentName)
		{
			return new TDStatus(TDStatus.OK);
		}

		/// <summary>CHANGES:</summary>
		public virtual IDictionary<string, object> ChangesDictForRevision(TDRevision rev)
		{
			IDictionary<string, object> changesDict = new Dictionary<string, object>();
			changesDict.Put("rev", rev.GetRevId());
			IList<IDictionary<string, object>> changes = new AList<IDictionary<string, object
				>>();
			changes.AddItem(changesDict);
			IDictionary<string, object> result = new Dictionary<string, object>();
			result.Put("seq", rev.GetSequence());
			result.Put("id", rev.GetDocId());
			result.Put("changes", changes);
			if (rev.IsDeleted())
			{
				result.Put("deleted", true);
			}
			if (changesIncludesDocs)
			{
				result.Put("doc", rev.GetProperties());
			}
			return result;
		}

		public virtual IDictionary<string, object> ResponseBodyForChanges(IList<TDRevision
			> changes, long since)
		{
			IList<IDictionary<string, object>> results = new AList<IDictionary<string, object
				>>();
			foreach (TDRevision rev in changes)
			{
				IDictionary<string, object> changeDict = ChangesDictForRevision(rev);
				results.AddItem(changeDict);
			}
			if (changes.Count > 0)
			{
				since = changes[changes.Count - 1].GetSequence();
			}
			IDictionary<string, object> result = new Dictionary<string, object>();
			result.Put("results", results);
			result.Put("last_seq", since);
			return result;
		}

		public virtual IDictionary<string, object> ResponseBodyForChangesWithConflicts(IList
			<TDRevision> changes, long since)
		{
			// Assumes the changes are grouped by docID so that conflicts will be adjacent.
			IList<IDictionary<string, object>> entries = new AList<IDictionary<string, object
				>>();
			string lastDocID = null;
			IDictionary<string, object> lastEntry = null;
			foreach (TDRevision rev in changes)
			{
				string docID = rev.GetDocId();
				if (docID.Equals(lastDocID))
				{
					IDictionary<string, object> changesDict = new Dictionary<string, object>();
					changesDict.Put("rev", rev.GetRevId());
					IList<IDictionary<string, object>> inchanges = (IList<IDictionary<string, object>
						>)lastEntry.Get("changes");
					inchanges.AddItem(changesDict);
				}
				else
				{
					lastEntry = ChangesDictForRevision(rev);
					entries.AddItem(lastEntry);
					lastDocID = docID;
				}
			}
			// After collecting revisions, sort by sequence:
			entries.Sort(new _IComparer_910());
			long lastSeq = (long)entries[entries.Count - 1].Get("seq");
			if (lastSeq == null)
			{
				lastSeq = since;
			}
			IDictionary<string, object> result = new Dictionary<string, object>();
			result.Put("results", entries);
			result.Put("last_seq", lastSeq);
			return result;
		}

		private sealed class _IComparer_910 : IComparer<IDictionary<string, object>>
		{
			public _IComparer_910()
			{
			}

			public int Compare(IDictionary<string, object> e1, IDictionary<string, object> e2
				)
			{
				return TDMisc.TDSequenceCompare((long)e1.Get("seq"), (long)e2.Get("seq"));
			}
		}

		public virtual void SendContinuousChange(TDRevision rev)
		{
			IDictionary<string, object> changeDict = ChangesDictForRevision(rev);
			try
			{
				string jsonString = TDServer.GetObjectMapper().WriteValueAsString(changeDict);
				if (callbackBlock != null)
				{
					byte[] json = Sharpen.Runtime.GetBytesForString((jsonString + "\n"));
					OutputStream os = connection.GetResponseOutputStream();
					try
					{
						os.Write(json);
						os.Flush();
					}
					catch (Exception e)
					{
						Log.E(TDDatabase.TAG, "IOException writing to internal streams", e);
					}
				}
			}
			catch (Exception e)
			{
				Log.W("Unable to serialize change to JSON", e);
			}
		}

		public virtual void Update(Observable observable, object changeObject)
		{
			if (observable == db)
			{
				//make sure we're listening to the right events
				IDictionary<string, object> changeNotification = (IDictionary<string, object>)changeObject;
				TDRevision rev = (TDRevision)changeNotification.Get("rev");
				if (changesFilter != null && !changesFilter.Filter(rev))
				{
					return;
				}
				if (longpoll)
				{
					Log.W(TDDatabase.TAG, "TDRouter: Sending longpoll response");
					SendResponse();
					IList<TDRevision> revs = new AList<TDRevision>();
					revs.AddItem(rev);
					IDictionary<string, object> body = ResponseBodyForChanges(revs, 0);
					if (callbackBlock != null)
					{
						byte[] data = null;
						try
						{
							data = TDServer.GetObjectMapper().WriteValueAsBytes(body);
						}
						catch (Exception e)
						{
							Log.W(TDDatabase.TAG, "Error serializing JSON", e);
						}
						OutputStream os = connection.GetResponseOutputStream();
						try
						{
							os.Write(data);
							os.Close();
						}
						catch (IOException e)
						{
							Log.E(TDDatabase.TAG, "IOException writing to internal streams", e);
						}
					}
				}
				else
				{
					Log.W(TDDatabase.TAG, "TDRouter: Sending continous change chunk");
					SendContinuousChange(rev);
				}
			}
		}

		public virtual TDStatus Do_GET_Document_changes(TDDatabase _db, string docID, string
			 _attachmentName)
		{
			// http://wiki.apache.org/couchdb/HTTP_database_API#Changes
			TDChangesOptions options = new TDChangesOptions();
			changesIncludesDocs = GetBooleanQuery("include_docs");
			options.SetIncludeDocs(changesIncludesDocs);
			string style = GetQuery("style");
			if (style != null && style.Equals("all_docs"))
			{
				options.SetIncludeConflicts(true);
			}
			options.SetContentOptions(GetContentOptions());
			options.SetSortBySequence(!options.IsIncludeConflicts());
			options.SetLimit(GetIntQuery("limit", options.GetLimit()));
			int since = GetIntQuery("since", 0);
			string filterName = GetQuery("filter");
			if (filterName != null)
			{
				changesFilter = db.GetFilterNamed(filterName);
				if (changesFilter == null)
				{
					return new TDStatus(TDStatus.NOT_FOUND);
				}
			}
			TDRevisionList changes = db.ChangesSince(since, options, changesFilter);
			if (changes == null)
			{
				return new TDStatus(TDStatus.INTERNAL_SERVER_ERROR);
			}
			string feed = GetQuery("feed");
			longpoll = "longpoll".Equals(feed);
			bool continuous = !longpoll && "continuous".Equals(feed);
			if (continuous || (longpoll && changes.Count == 0))
			{
				connection.SetChunked(true);
				connection.SetResponseCode(TDStatus.OK);
				SendResponse();
				if (continuous)
				{
					foreach (TDRevision rev in changes)
					{
						SendContinuousChange(rev);
					}
				}
				db.AddObserver(this);
				// Don't close connection; more data to come
				return new TDStatus(0);
			}
			else
			{
				if (options.IsIncludeConflicts())
				{
					connection.SetResponseBody(new TDBody(ResponseBodyForChangesWithConflicts(changes
						, since)));
				}
				else
				{
					connection.SetResponseBody(new TDBody(ResponseBodyForChanges(changes, since)));
				}
				return new TDStatus(TDStatus.OK);
			}
		}

		/// <summary>DOCUMENT REQUESTS:</summary>
		public virtual string GetRevIDFromIfMatchHeader()
		{
			string ifMatch = connection.GetRequestProperty("If-Match");
			if (ifMatch == null)
			{
				return null;
			}
			// Value of If-Match is an ETag, so have to trim the quotes around it:
			if (ifMatch.Length > 2 && ifMatch.StartsWith("\"") && ifMatch.EndsWith("\""))
			{
				return Sharpen.Runtime.Substring(ifMatch, 1, ifMatch.Length - 2);
			}
			else
			{
				return null;
			}
		}

		public virtual string SetResponseEtag(TDRevision rev)
		{
			string eTag = string.Format("\"%s\"", rev.GetRevId());
			connection.GetResHeader().Add("Etag", eTag);
			return eTag;
		}

		public virtual TDStatus Do_GET_Document(TDDatabase _db, string docID, string _attachmentName
			)
		{
			// http://wiki.apache.org/couchdb/HTTP_Document_API#GET
			bool isLocalDoc = docID.StartsWith("_local");
			EnumSet<TDDatabase.TDContentOptions> options = GetContentOptions();
			string openRevsParam = GetQuery("open_revs");
			if (openRevsParam == null || isLocalDoc)
			{
				// Regular GET:
				string revID = GetQuery("rev");
				// often null
				TDRevision rev = null;
				if (isLocalDoc)
				{
					rev = db.GetLocalDocument(docID, revID);
				}
				else
				{
					rev = db.GetDocumentWithIDAndRev(docID, revID, options);
					// Handle ?atts_since query by stubbing out older attachments:
					//?atts_since parameter - value is a (URL-encoded) JSON array of one or more revision IDs.
					// The response will include the content of only those attachments that changed since the given revision(s).
					//(You can ask for this either in the default JSON or as multipart/related, as previously described.)
					IList<string> attsSince = (IList<string>)GetJSONQuery("atts_since");
					if (attsSince != null)
					{
						string ancestorId = db.FindCommonAncestorOf(rev, attsSince);
						if (ancestorId != null)
						{
							int generation = TDRevision.GenerationFromRevID(ancestorId);
							db.StubOutAttachmentsIn(rev, generation + 1);
						}
					}
				}
				if (rev == null)
				{
					return new TDStatus(TDStatus.NOT_FOUND);
				}
				if (CacheWithEtag(rev.GetRevId()))
				{
					return new TDStatus(TDStatus.NOT_MODIFIED);
				}
				// set ETag and check conditional GET
				connection.SetResponseBody(rev.GetBody());
			}
			else
			{
				IList<IDictionary<string, object>> result = null;
				if (openRevsParam.Equals("all"))
				{
					// Get all conflicting revisions:
					TDRevisionList allRevs = db.GetAllRevisionsOfDocumentID(docID, true);
					result = new AList<IDictionary<string, object>>(allRevs.Count);
					foreach (TDRevision rev in allRevs)
					{
						TDStatus status = db.LoadRevisionBody(rev, options);
						if (status.IsSuccessful())
						{
							IDictionary<string, object> dict = new Dictionary<string, object>();
							dict.Put("ok", rev.GetProperties());
							result.AddItem(dict);
						}
						else
						{
							if (status.GetCode() != TDStatus.INTERNAL_SERVER_ERROR)
							{
								IDictionary<string, object> dict = new Dictionary<string, object>();
								dict.Put("missing", rev.GetRevId());
								result.AddItem(dict);
							}
							else
							{
								return status;
							}
						}
					}
				}
				else
				{
					// internal error getting revision
					// ?open_revs=[...] returns an array of revisions of the document:
					IList<string> openRevs = (IList<string>)GetJSONQuery("open_revs");
					if (openRevs == null)
					{
						return new TDStatus(TDStatus.BAD_REQUEST);
					}
					result = new AList<IDictionary<string, object>>(openRevs.Count);
					foreach (string revID in openRevs)
					{
						TDRevision rev = db.GetDocumentWithIDAndRev(docID, revID, options);
						if (rev != null)
						{
							IDictionary<string, object> dict = new Dictionary<string, object>();
							dict.Put("ok", rev.GetProperties());
							result.AddItem(dict);
						}
						else
						{
							IDictionary<string, object> dict = new Dictionary<string, object>();
							dict.Put("missing", revID);
							result.AddItem(dict);
						}
					}
				}
				string acceptMultipart = GetMultipartRequestType();
				if (acceptMultipart != null)
				{
					//FIXME figure out support for multipart
					throw new NotSupportedException();
				}
				else
				{
					connection.SetResponseBody(new TDBody(result));
				}
			}
			return new TDStatus(TDStatus.OK);
		}

		public virtual TDStatus Do_GET_Attachment(TDDatabase _db, string docID, string _attachmentName
			)
		{
			// http://wiki.apache.org/couchdb/HTTP_Document_API#GET
			EnumSet<TDDatabase.TDContentOptions> options = GetContentOptions();
			options.AddItem(TDDatabase.TDContentOptions.TDNoBody);
			string revID = GetQuery("rev");
			// often null
			TDRevision rev = db.GetDocumentWithIDAndRev(docID, revID, options);
			if (rev == null)
			{
				return new TDStatus(TDStatus.NOT_FOUND);
			}
			if (CacheWithEtag(rev.GetRevId()))
			{
				return new TDStatus(TDStatus.NOT_MODIFIED);
			}
			// set ETag and check conditional GET
			string type = null;
			TDStatus status = new TDStatus();
			string acceptEncoding = connection.GetRequestProperty("Accept-Encoding");
			TDAttachment contents = db.GetAttachmentForSequence(rev.GetSequence(), _attachmentName
				, status);
			if (contents == null)
			{
				return new TDStatus(TDStatus.NOT_FOUND);
			}
			type = contents.GetContentType();
			if (type != null)
			{
				connection.GetResHeader().Add("Content-Type", type);
			}
			if (acceptEncoding != null && acceptEncoding.Equals("gzip"))
			{
				connection.GetResHeader().Add("Content-Encoding", acceptEncoding);
			}
			connection.SetResponseInputStream(contents.GetContentStream());
			return new TDStatus(TDStatus.OK);
		}

		/// <summary>NOTE this departs from the iOS version, returning revision, passing status back by reference
		/// 	</summary>
		public virtual TDRevision Update(TDDatabase _db, string docID, TDBody body, bool 
			deleting, bool allowConflict, TDStatus outStatus)
		{
			bool isLocalDoc = docID != null && docID.StartsWith(("_local"));
			string prevRevID = null;
			if (!deleting)
			{
				bool deletingBoolean = (bool)body.GetPropertyForKey("deleted");
				deleting = (deletingBoolean != null && deletingBoolean);
				if (docID == null)
				{
					if (isLocalDoc)
					{
						outStatus.SetCode(TDStatus.METHOD_NOT_ALLOWED);
						return null;
					}
					// POST's doc ID may come from the _id field of the JSON body, else generate a random one.
					docID = (string)body.GetPropertyForKey("_id");
					if (docID == null)
					{
						if (deleting)
						{
							outStatus.SetCode(TDStatus.BAD_REQUEST);
							return null;
						}
						docID = TDDatabase.GenerateDocumentId();
					}
				}
				// PUT's revision ID comes from the JSON body.
				prevRevID = (string)body.GetPropertyForKey("_rev");
			}
			else
			{
				// DELETE's revision ID comes from the ?rev= query param
				prevRevID = GetQuery("rev");
			}
			// A backup source of revision ID is an If-Match header:
			if (prevRevID == null)
			{
				prevRevID = GetRevIDFromIfMatchHeader();
			}
			TDRevision rev = new TDRevision(docID, null, deleting);
			rev.SetBody(body);
			TDRevision result = null;
			TDStatus tmpStatus = new TDStatus();
			if (isLocalDoc)
			{
				result = _db.PutLocalRevision(rev, prevRevID, tmpStatus);
			}
			else
			{
				result = _db.PutRevision(rev, prevRevID, allowConflict, tmpStatus);
			}
			outStatus.SetCode(tmpStatus.GetCode());
			return result;
		}

		public virtual TDStatus Update(TDDatabase _db, string docID, IDictionary<string, 
			object> bodyDict, bool deleting)
		{
			TDBody body = new TDBody(bodyDict);
			TDStatus status = new TDStatus();
			TDRevision rev = Update(_db, docID, body, deleting, false, status);
			if (status.IsSuccessful())
			{
				CacheWithEtag(rev.GetRevId());
				// set ETag
				if (!deleting)
				{
					Uri url = connection.GetURL();
					string urlString = url.ToExternalForm();
					if (docID != null)
					{
						urlString += "/" + rev.GetDocId();
						try
						{
							url = new Uri(urlString);
						}
						catch (UriFormatException e)
						{
							Log.W("Malformed URL", e);
						}
					}
					SetResponseLocation(url);
				}
				IDictionary<string, object> result = new Dictionary<string, object>();
				result.Put("ok", true);
				result.Put("id", rev.GetDocId());
				result.Put("rev", rev.GetRevId());
				connection.SetResponseBody(new TDBody(result));
			}
			return status;
		}

		public virtual TDStatus Do_PUT_Document(TDDatabase _db, string docID, string _attachmentName
			)
		{
			IDictionary<string, object> bodyDict = GetBodyAsDictionary();
			if (bodyDict == null)
			{
				return new TDStatus(TDStatus.BAD_REQUEST);
			}
			if (GetQuery("new_edits") == null || (GetQuery("new_edits") != null && (System.Convert.ToBoolean
				(GetQuery("new_edits")))))
			{
				// Regular PUT
				return Update(_db, docID, bodyDict, false);
			}
			else
			{
				// PUT with new_edits=false -- forcible insertion of existing revision:
				TDBody body = new TDBody(bodyDict);
				TDRevision rev = new TDRevision(body);
				if (rev.GetRevId() == null || rev.GetDocId() == null || !rev.GetDocId().Equals(docID
					))
				{
					return new TDStatus(TDStatus.BAD_REQUEST);
				}
				IList<string> history = TDDatabase.ParseCouchDBRevisionHistory(body.GetProperties
					());
				return db.ForceInsert(rev, history, null);
			}
		}

		public virtual TDStatus Do_DELETE_Document(TDDatabase _db, string docID, string _attachmentName
			)
		{
			return Update(_db, docID, null, true);
		}

		public virtual TDStatus UpdateAttachment(string attachment, string docID, InputStream
			 contentStream)
		{
			TDStatus status = new TDStatus();
			string revID = GetQuery("rev");
			if (revID == null)
			{
				revID = GetRevIDFromIfMatchHeader();
			}
			TDRevision rev = db.UpdateAttachment(attachment, contentStream, connection.GetRequestProperty
				("content-type"), docID, revID, status);
			if (status.IsSuccessful())
			{
				IDictionary<string, object> resultDict = new Dictionary<string, object>();
				resultDict.Put("ok", true);
				resultDict.Put("id", rev.GetDocId());
				resultDict.Put("rev", rev.GetRevId());
				connection.SetResponseBody(new TDBody(resultDict));
				CacheWithEtag(rev.GetRevId());
				if (contentStream != null)
				{
					SetResponseLocation(connection.GetURL());
				}
			}
			return status;
		}

		public virtual TDStatus Do_PUT_Attachment(TDDatabase _db, string docID, string _attachmentName
			)
		{
			return UpdateAttachment(_attachmentName, docID, connection.GetRequestInputStream(
				));
		}

		public virtual TDStatus Do_DELETE_Attachment(TDDatabase _db, string docID, string
			 _attachmentName)
		{
			return UpdateAttachment(_attachmentName, docID, null);
		}

		/// <summary>VIEW QUERIES:</summary>
		public virtual TDView CompileView(string viewName, IDictionary<string, object> viewProps
			)
		{
			string language = (string)viewProps.Get("language");
			if (language == null)
			{
				language = "javascript";
			}
			string mapSource = (string)viewProps.Get("map");
			if (mapSource == null)
			{
				return null;
			}
			TDViewMapBlock mapBlock = TDView.GetCompiler().CompileMapFunction(mapSource, language
				);
			if (mapBlock == null)
			{
				Log.W(TDDatabase.TAG, string.Format("View %s has unknown map function: %s", viewName
					, mapSource));
				return null;
			}
			string reduceSource = (string)viewProps.Get("reduce");
			TDViewReduceBlock reduceBlock = null;
			if (reduceSource != null)
			{
				reduceBlock = TDView.GetCompiler().CompileReduceFunction(reduceSource, language);
				if (reduceBlock == null)
				{
					Log.W(TDDatabase.TAG, string.Format("View %s has unknown reduce function: %s", viewName
						, reduceBlock));
					return null;
				}
			}
			TDView view = db.GetViewNamed(viewName);
			view.SetMapReduceBlocks(mapBlock, reduceBlock, "1");
			string collation = (string)viewProps.Get("collation");
			if ("raw".Equals(collation))
			{
				view.SetCollation(TDView.TDViewCollation.TDViewCollationRaw);
			}
			return view;
		}

		public virtual TDStatus QueryDesignDoc(string designDoc, string viewName, IList<object
			> keys)
		{
			string tdViewName = string.Format("%s/%s", designDoc, viewName);
			TDView view = db.GetExistingViewNamed(tdViewName);
			if (view == null || view.GetMapBlock() == null)
			{
				// No TouchDB view is defined, or it hasn't had a map block assigned;
				// see if there's a CouchDB view definition we can compile:
				TDRevision rev = db.GetDocumentWithIDAndRev(string.Format("_design/%s", designDoc
					), null, EnumSet.NoneOf<TDDatabase.TDContentOptions>());
				if (rev == null)
				{
					return new TDStatus(TDStatus.NOT_FOUND);
				}
				IDictionary<string, object> views = (IDictionary<string, object>)rev.GetProperties
					().Get("views");
				IDictionary<string, object> viewProps = (IDictionary<string, object>)views.Get(viewName
					);
				if (viewProps == null)
				{
					return new TDStatus(TDStatus.NOT_FOUND);
				}
				// If there is a CouchDB view, see if it can be compiled from source:
				view = CompileView(tdViewName, viewProps);
				if (view == null)
				{
					return new TDStatus(TDStatus.INTERNAL_SERVER_ERROR);
				}
			}
			TDQueryOptions options = new TDQueryOptions();
			//if the view contains a reduce block, it should default to reduce=true
			if (view.GetReduceBlock() != null)
			{
				options.SetReduce(true);
			}
			if (!GetQueryOptions(options))
			{
				return new TDStatus(TDStatus.BAD_REQUEST);
			}
			if (keys != null)
			{
				options.SetKeys(keys);
			}
			TDStatus status = view.UpdateIndex();
			if (!status.IsSuccessful())
			{
				return status;
			}
			long lastSequenceIndexed = view.GetLastSequenceIndexed();
			// Check for conditional GET and set response Etag header:
			if (keys == null)
			{
				long eTag = options.IsIncludeDocs() ? db.GetLastSequence() : lastSequenceIndexed;
				if (CacheWithEtag(string.Format("%d", eTag)))
				{
					return new TDStatus(TDStatus.NOT_MODIFIED);
				}
			}
			IList<IDictionary<string, object>> rows = view.QueryWithOptions(options, status);
			if (rows == null)
			{
				return status;
			}
			IDictionary<string, object> responseBody = new Dictionary<string, object>();
			responseBody.Put("rows", rows);
			responseBody.Put("total_rows", rows.Count);
			responseBody.Put("offset", options.GetSkip());
			if (options.IsUpdateSeq())
			{
				responseBody.Put("update_seq", lastSequenceIndexed);
			}
			connection.SetResponseBody(new TDBody(responseBody));
			return new TDStatus(TDStatus.OK);
		}

		public virtual TDStatus Do_GET_DesignDocument(TDDatabase _db, string designDocID, 
			string viewName)
		{
			return QueryDesignDoc(designDocID, viewName, null);
		}

		public virtual TDStatus Do_POST_DesignDocument(TDDatabase _db, string designDocID
			, string viewName)
		{
			IDictionary<string, object> bodyDict = GetBodyAsDictionary();
			if (bodyDict == null)
			{
				return new TDStatus(TDStatus.BAD_REQUEST);
			}
			IList<object> keys = (IList<object>)bodyDict.Get("keys");
			return QueryDesignDoc(designDocID, viewName, keys);
		}

		public override string ToString()
		{
			string url = "Unknown";
			if (connection != null && connection.GetURL() != null)
			{
				url = connection.GetURL().ToExternalForm();
			}
			return string.Format("TDRouter [%s]", url);
		}
	}
}
