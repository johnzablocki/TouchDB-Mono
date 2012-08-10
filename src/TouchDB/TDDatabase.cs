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
using Android.Content;
using Android.Database;
using Android.Database.Sqlite;
using Android.OS;
using Android.Util;
using Couchbase.TouchDB;
using Couchbase.TouchDB.Replicator;
using Couchbase.TouchDB.Support;
using Sharpen;

namespace Couchbase.TouchDB
{
	/// <summary>A TouchDB database.</summary>
	/// <remarks>A TouchDB database.</remarks>
	public class TDDatabase : Observable
	{
		private string path;

		private string name;

		private SQLiteDatabase database;

		private bool open = false;

		private int transactionLevel = 0;

		public static readonly string TAG = "TDDatabase";

		private IDictionary<string, TDView> views;

		private IDictionary<string, TDFilterBlock> filters;

		private IDictionary<string, TDValidationBlock> validations;

		private IList<TDReplicator> activeReplicators;

		private TDBlobStore attachments;

		private HandlerThread handlerThread;

		private Handler handler;

		/// <summary>Options for what metadata to include in document bodies</summary>
		public enum TDContentOptions
		{
			TDIncludeAttachments,
			TDIncludeConflicts,
			TDIncludeRevs,
			TDIncludeRevsInfo,
			TDIncludeLocalSeq,
			TDNoBody
		}

		private static readonly ICollection<string> KNOWN_SPECIAL_KEYS;

		static TDDatabase()
		{
			KNOWN_SPECIAL_KEYS = new HashSet<string>();
			KNOWN_SPECIAL_KEYS.AddItem("_id");
			KNOWN_SPECIAL_KEYS.AddItem("_rev");
			KNOWN_SPECIAL_KEYS.AddItem("_attachments");
			KNOWN_SPECIAL_KEYS.AddItem("_deleted");
			KNOWN_SPECIAL_KEYS.AddItem("_revisions");
			KNOWN_SPECIAL_KEYS.AddItem("_revs_info");
			KNOWN_SPECIAL_KEYS.AddItem("_conflicts");
			KNOWN_SPECIAL_KEYS.AddItem("_deleted_conflicts");
		}

		public static readonly string SCHEMA = string.Empty + "CREATE TABLE docs ( " + "        doc_id INTEGER PRIMARY KEY, "
			 + "        docid TEXT UNIQUE NOT NULL); " + "    CREATE INDEX docs_docid ON docs(docid); "
			 + "    CREATE TABLE revs ( " + "        sequence INTEGER PRIMARY KEY AUTOINCREMENT, "
			 + "        doc_id INTEGER NOT NULL REFERENCES docs(doc_id) ON DELETE CASCADE, "
			 + "        revid TEXT NOT NULL, " + "        parent INTEGER REFERENCES revs(sequence) ON DELETE SET NULL, "
			 + "        current BOOLEAN, " + "        deleted BOOLEAN DEFAULT 0, " + "        json BLOB); "
			 + "    CREATE INDEX revs_by_id ON revs(revid, doc_id); " + "    CREATE INDEX revs_current ON revs(doc_id, current); "
			 + "    CREATE INDEX revs_parent ON revs(parent); " + "    CREATE TABLE localdocs ( "
			 + "        docid TEXT UNIQUE NOT NULL, " + "        revid TEXT NOT NULL, " + "        json BLOB); "
			 + "    CREATE INDEX localdocs_by_docid ON localdocs(docid); " + "    CREATE TABLE views ( "
			 + "        view_id INTEGER PRIMARY KEY, " + "        name TEXT UNIQUE NOT NULL,"
			 + "        version TEXT, " + "        lastsequence INTEGER DEFAULT 0); " + "    CREATE INDEX views_by_name ON views(name); "
			 + "    CREATE TABLE maps ( " + "        view_id INTEGER NOT NULL REFERENCES views(view_id) ON DELETE CASCADE, "
			 + "        sequence INTEGER NOT NULL REFERENCES revs(sequence) ON DELETE CASCADE, "
			 + "        key TEXT NOT NULL COLLATE JSON, " + "        value TEXT); " + "    CREATE INDEX maps_keys on maps(view_id, key COLLATE JSON); "
			 + "    CREATE TABLE attachments ( " + "        sequence INTEGER NOT NULL REFERENCES revs(sequence) ON DELETE CASCADE, "
			 + "        filename TEXT NOT NULL, " + "        key BLOB NOT NULL, " + "        type TEXT, "
			 + "        length INTEGER NOT NULL, " + "        revpos INTEGER DEFAULT 0); " +
			 "    CREATE INDEX attachments_by_sequence on attachments(sequence, filename); "
			 + "    CREATE TABLE replicators ( " + "        remote TEXT NOT NULL, " + "        push BOOLEAN, "
			 + "        last_sequence TEXT, " + "        UNIQUE (remote, push)); " + "    PRAGMA user_version = 3";

		// at the end, update user_version
		public virtual string GetAttachmentStorePath()
		{
			string attachmentStorePath = path;
			int lastDotPosition = attachmentStorePath.LastIndexOf('.');
			if (lastDotPosition > 0)
			{
				attachmentStorePath = Sharpen.Runtime.Substring(attachmentStorePath, 0, lastDotPosition
					);
			}
			attachmentStorePath = attachmentStorePath + FilePath.separator + "attachments";
			return attachmentStorePath;
		}

		public static Couchbase.TouchDB.TDDatabase CreateEmptyDBAtPath(string path)
		{
			if (!FileDirUtils.RemoveItemIfExists(path))
			{
				return null;
			}
			Couchbase.TouchDB.TDDatabase result = new Couchbase.TouchDB.TDDatabase(path);
			FilePath af = new FilePath(result.GetAttachmentStorePath());
			//recursively delete attachments path
			if (!FileDirUtils.DeleteRecursive(af))
			{
				return null;
			}
			if (!result.Open())
			{
				return null;
			}
			return result;
		}

		public TDDatabase(string path)
		{
			////asert(path.startsWith("/")); //path must be absolute
			this.path = path;
			this.name = FileDirUtils.GetDatabaseNameFromPath(path);
			//start a handler thead to do work for this database
			handlerThread = new HandlerThread("HandlerThread for " + ToString());
			handlerThread.Start();
			//Get the looper from the handlerThread
			Looper looper = handlerThread.GetLooper();
			handler = new Handler(looper);
		}

		public override string ToString()
		{
			return this.GetType().FullName + "[" + path + "]";
		}

		public virtual bool Exists()
		{
			return new FilePath(path).Exists();
		}

		/// <summary>Replaces the database with a copy of another database.</summary>
		/// <remarks>
		/// Replaces the database with a copy of another database.
		/// This is primarily used to install a canned database on first launch of an app, in which case you should first check .exists to avoid replacing the database if it exists already. The canned database would have been copied into your app bundle at build time.
		/// </remarks>
		/// <param name="databasePath">Path of the database file that should replace this one.
		/// 	</param>
		/// <param name="attachmentsPath">Path of the associated attachments directory, or nil if there are no attachments.
		/// 	</param>
		/// <returns>true if the database was copied, IOException if an error occurs</returns>
		/// <exception cref="System.IO.IOException"></exception>
		public virtual bool ReplaceWithDatabase(string databasePath, string attachmentsPath
			)
		{
			string dstAttachmentsPath = this.GetAttachmentStorePath();
			FilePath sourceFile = new FilePath(databasePath);
			FilePath destFile = new FilePath(path);
			FileDirUtils.CopyFile(sourceFile, destFile);
			FilePath attachmentsFile = new FilePath(dstAttachmentsPath);
			FileDirUtils.DeleteRecursive(attachmentsFile);
			attachmentsFile.Mkdirs();
			if (attachmentsPath != null)
			{
				FileDirUtils.CopyFolder(new FilePath(attachmentsPath), attachmentsFile);
			}
			return true;
		}

		public virtual bool Initialize(string statements)
		{
			try
			{
				foreach (string statement in statements.Split(";"))
				{
					database.ExecSQL(statement);
				}
			}
			catch (SQLException)
			{
				Close();
				return false;
			}
			return true;
		}

		public virtual bool Open()
		{
			if (open)
			{
				return true;
			}
			try
			{
				database = SQLiteDatabase.OpenDatabase(path, null, SQLiteDatabase.CREATE_IF_NECESSARY
					);
				TDCollateJSON.RegisterCustomCollators(database);
			}
			catch (SQLiteException e)
			{
				Log.E(Couchbase.TouchDB.TDDatabase.TAG, "Error opening", e);
				return false;
			}
			// Stuff we need to initialize every time the database opens:
			if (!Initialize("PRAGMA foreign_keys = ON;"))
			{
				Log.E(Couchbase.TouchDB.TDDatabase.TAG, "Error turning on foreign keys");
				return false;
			}
			// Check the user_version number we last stored in the database:
			int dbVersion = database.GetVersion();
			// Incompatible version changes increment the hundreds' place:
			if (dbVersion >= 100)
			{
				Log.W(Couchbase.TouchDB.TDDatabase.TAG, "TDDatabase: Database version (" + dbVersion
					 + ") is newer than I know how to work with");
				database.Close();
				return false;
			}
			if (dbVersion < 1)
			{
				// First-time initialization:
				// (Note: Declaring revs.sequence as AUTOINCREMENT means the values will always be
				// monotonically increasing, never reused. See <http://www.sqlite.org/autoinc.html>)
				if (!Initialize(SCHEMA))
				{
					database.Close();
					return false;
				}
				dbVersion = 3;
			}
			if (dbVersion < 2)
			{
				// Version 2: added attachments.revpos
				string upgradeSql = "ALTER TABLE attachments ADD COLUMN revpos INTEGER DEFAULT 0; "
					 + "PRAGMA user_version = 2";
				if (!Initialize(upgradeSql))
				{
					database.Close();
					return false;
				}
				dbVersion = 2;
			}
			if (dbVersion < 3)
			{
				string upgradeSql = "CREATE TABLE localdocs ( " + "docid TEXT UNIQUE NOT NULL, " 
					+ "revid TEXT NOT NULL, " + "json BLOB); " + "CREATE INDEX localdocs_by_docid ON localdocs(docid); "
					 + "PRAGMA user_version = 3";
				if (!Initialize(upgradeSql))
				{
					database.Close();
					return false;
				}
				dbVersion = 3;
			}
			if (dbVersion < 4)
			{
				string upgradeSql = "CREATE TABLE info ( " + "key TEXT PRIMARY KEY, " + "value TEXT); "
					 + "INSERT INTO INFO (key, value) VALUES ('privateUUID', '" + TDMisc.TDCreateUUID
					() + "'); " + "INSERT INTO INFO (key, value) VALUES ('publicUUID',  '" + TDMisc.
					TDCreateUUID() + "'); " + "PRAGMA user_version = 4";
				if (!Initialize(upgradeSql))
				{
					database.Close();
					return false;
				}
			}
			try
			{
				attachments = new TDBlobStore(GetAttachmentStorePath());
			}
			catch (ArgumentException e)
			{
				Log.E(Couchbase.TouchDB.TDDatabase.TAG, "Could not initialize attachment store", 
					e);
				database.Close();
				return false;
			}
			open = true;
			return true;
		}

		public virtual bool Close()
		{
			if (!open)
			{
				return false;
			}
			if (views != null)
			{
				foreach (TDView view in views.Values)
				{
					view.DatabaseClosing();
				}
			}
			views = null;
			if (activeReplicators != null)
			{
				foreach (TDReplicator replicator in activeReplicators)
				{
					replicator.DatabaseClosing();
				}
				activeReplicators = null;
			}
			if (handlerThread != null)
			{
				handler = null;
				handlerThread.Quit();
				handlerThread = null;
			}
			if (database != null && database.IsOpen())
			{
				database.Close();
			}
			open = false;
			transactionLevel = 0;
			return true;
		}

		public virtual bool DeleteDatabase()
		{
			if (open)
			{
				if (!Close())
				{
					return false;
				}
			}
			else
			{
				if (!Exists())
				{
					return true;
				}
			}
			FilePath file = new FilePath(path);
			FilePath attachmentsFile = new FilePath(GetAttachmentStorePath());
			bool deleteStatus = file.Delete();
			//recursively delete attachments path
			bool deleteAttachmentStatus = FileDirUtils.DeleteRecursive(attachmentsFile);
			return deleteStatus && deleteAttachmentStatus;
		}

		public virtual string GetPath()
		{
			return path;
		}

		public virtual string GetName()
		{
			return name;
		}

		public virtual void SetName(string name)
		{
			this.name = name;
		}

		// Leave this package protected, so it can only be used
		// TDView uses this accessor
		internal virtual SQLiteDatabase GetDatabase()
		{
			return database;
		}

		public virtual TDBlobStore GetAttachments()
		{
			return attachments;
		}

		public virtual long TotalDataSize()
		{
			FilePath f = new FilePath(path);
			long size = f.Length() + attachments.TotalDataSize();
			return size;
		}

		/// <summary>Begins a database transaction.</summary>
		/// <remarks>
		/// Begins a database transaction. Transactions can nest.
		/// Every beginTransaction() must be balanced by a later endTransaction()
		/// </remarks>
		public virtual bool BeginTransaction()
		{
			try
			{
				database.BeginTransaction();
				++transactionLevel;
			}
			catch (SQLException)
			{
				//Log.v(TAG, "Begin transaction (level " + Integer.toString(transactionLevel) + ")...");
				return false;
			}
			return true;
		}

		/// <summary>Commits or aborts (rolls back) a transaction.</summary>
		/// <remarks>Commits or aborts (rolls back) a transaction.</remarks>
		/// <param name="commit">If true, commits; if false, aborts and rolls back, undoing all changes made since the matching -beginTransaction call, *including* any committed nested transactions.
		/// 	</param>
		public virtual bool EndTransaction(bool commit)
		{
			////asert(transactionLevel > 0);
			if (commit)
			{
				//Log.v(TAG, "Committing transaction (level " + Integer.toString(transactionLevel) + ")...");
				database.SetTransactionSuccessful();
				database.EndTransaction();
			}
			else
			{
				Log.V(TAG, "CANCEL transaction (level " + Sharpen.Extensions.ToString(transactionLevel
					) + ")...");
				try
				{
					database.EndTransaction();
				}
				catch (SQLException)
				{
					return false;
				}
			}
			--transactionLevel;
			return true;
		}

		/// <summary>Compacts the database storage by removing the bodies and attachments of obsolete revisions.
		/// 	</summary>
		/// <remarks>Compacts the database storage by removing the bodies and attachments of obsolete revisions.
		/// 	</remarks>
		public virtual TDStatus Compact()
		{
			// Can't delete any rows because that would lose revision tree history.
			// But we can remove the JSON of non-current revisions, which is most of the space.
			try
			{
				Log.V(Couchbase.TouchDB.TDDatabase.TAG, "Deleting JSON of old revisions...");
				ContentValues args = new ContentValues();
				args.Put("json", (string)null);
				database.Update("revs", args, "current=0", null);
			}
			catch (SQLException e)
			{
				Log.E(Couchbase.TouchDB.TDDatabase.TAG, "Error compacting", e);
				return new TDStatus(TDStatus.INTERNAL_SERVER_ERROR);
			}
			Log.V(Couchbase.TouchDB.TDDatabase.TAG, "Deleting old attachments...");
			TDStatus result = GarbageCollectAttachments();
			Log.V(Couchbase.TouchDB.TDDatabase.TAG, "Vacuuming SQLite database...");
			try
			{
				database.ExecSQL("VACUUM");
			}
			catch (SQLException e)
			{
				Log.E(Couchbase.TouchDB.TDDatabase.TAG, "Error vacuuming database", e);
				return new TDStatus(TDStatus.INTERNAL_SERVER_ERROR);
			}
			return result;
		}

		public virtual string PrivateUUID()
		{
			string result = null;
			Cursor cursor = null;
			try
			{
				cursor = database.RawQuery("SELECT value FROM info WHERE key='privateUUID'", null
					);
				if (cursor.MoveToFirst())
				{
					result = cursor.GetString(0);
				}
			}
			catch (SQLException e)
			{
				Log.E(TAG, "Error querying privateUUID", e);
			}
			finally
			{
				if (cursor != null)
				{
					cursor.Close();
				}
			}
			return result;
		}

		public virtual string PublicUUID()
		{
			string result = null;
			Cursor cursor = null;
			try
			{
				cursor = database.RawQuery("SELECT value FROM info WHERE key='publicUUID'", null);
				if (cursor.MoveToFirst())
				{
					result = cursor.GetString(0);
				}
			}
			catch (SQLException e)
			{
				Log.E(TAG, "Error querying privateUUID", e);
			}
			finally
			{
				if (cursor != null)
				{
					cursor.Close();
				}
			}
			return result;
		}

		/// <summary>GETTING DOCUMENTS:</summary>
		public virtual int GetDocumentCount()
		{
			string sql = "SELECT COUNT(DISTINCT doc_id) FROM revs WHERE current=1 AND deleted=0";
			Cursor cursor = null;
			int result = 0;
			try
			{
				cursor = database.RawQuery(sql, null);
				if (cursor.MoveToFirst())
				{
					result = cursor.GetInt(0);
				}
			}
			catch (SQLException e)
			{
				Log.E(Couchbase.TouchDB.TDDatabase.TAG, "Error getting document count", e);
			}
			finally
			{
				if (cursor != null)
				{
					cursor.Close();
				}
			}
			return result;
		}

		public virtual long GetLastSequence()
		{
			string sql = "SELECT MAX(sequence) FROM revs";
			Cursor cursor = null;
			long result = 0;
			try
			{
				cursor = database.RawQuery(sql, null);
				if (cursor.MoveToFirst())
				{
					result = cursor.GetLong(0);
				}
			}
			catch (SQLException e)
			{
				Log.E(Couchbase.TouchDB.TDDatabase.TAG, "Error getting last sequence", e);
			}
			finally
			{
				if (cursor != null)
				{
					cursor.Close();
				}
			}
			return result;
		}

		/// <summary>Splices the contents of an NSDictionary into JSON data (that already represents a dict), without parsing the JSON.
		/// 	</summary>
		/// <remarks>Splices the contents of an NSDictionary into JSON data (that already represents a dict), without parsing the JSON.
		/// 	</remarks>
		public virtual byte[] AppendDictToJSON(byte[] json, IDictionary<string, object> dict
			)
		{
			if (dict.Count == 0)
			{
				return json;
			}
			byte[] extraJSON = null;
			try
			{
				extraJSON = TDServer.GetObjectMapper().WriteValueAsBytes(dict);
			}
			catch (Exception e)
			{
				Log.E(Couchbase.TouchDB.TDDatabase.TAG, "Error convert extra JSON to bytes", e);
				return null;
			}
			int jsonLength = json.Length;
			int extraLength = extraJSON.Length;
			if (jsonLength == 2)
			{
				// Original JSON was empty
				return extraJSON;
			}
			byte[] newJson = new byte[jsonLength + extraLength - 1];
			System.Array.Copy(json, 0, newJson, 0, jsonLength - 1);
			// Copy json w/o trailing '}'
			newJson[jsonLength - 1] = (byte)(',');
			// Add a ','
			System.Array.Copy(extraJSON, 1, newJson, jsonLength, extraLength - 1);
			return newJson;
		}

		/// <summary>Inserts the _id, _rev and _attachments properties into the JSON data and stores it in rev.
		/// 	</summary>
		/// <remarks>
		/// Inserts the _id, _rev and _attachments properties into the JSON data and stores it in rev.
		/// Rev must already have its revID and sequence properties set.
		/// </remarks>
		public virtual IDictionary<string, object> ExtraPropertiesForRevision(TDRevision 
			rev, EnumSet<TDDatabase.TDContentOptions> contentOptions)
		{
			string docId = rev.GetDocId();
			string revId = rev.GetRevId();
			long sequenceNumber = rev.GetSequence();
			////asert(revId != null);
			////asert(sequenceNumber > 0);
			// Get attachment metadata, and optionally the contents:
			bool withAttachments = contentOptions.Contains(TDDatabase.TDContentOptions.TDIncludeAttachments
				);
			IDictionary<string, object> attachmentsDict = GetAttachmentsDictForSequenceWithContent
				(sequenceNumber, withAttachments);
			// Get more optional stuff to put in the properties:
			//OPT: This probably ends up making redundant SQL queries if multiple options are enabled.
			long localSeq = null;
			if (contentOptions.Contains(TDDatabase.TDContentOptions.TDIncludeLocalSeq))
			{
				localSeq = sequenceNumber;
			}
			IDictionary<string, object> revHistory = null;
			if (contentOptions.Contains(TDDatabase.TDContentOptions.TDIncludeRevs))
			{
				revHistory = GetRevisionHistoryDict(rev);
			}
			IList<object> revsInfo = null;
			if (contentOptions.Contains(TDDatabase.TDContentOptions.TDIncludeRevsInfo))
			{
				revsInfo = new AList<object>();
				IList<TDRevision> revHistoryFull = GetRevisionHistory(rev);
				foreach (TDRevision historicalRev in revHistoryFull)
				{
					IDictionary<string, object> revHistoryItem = new Dictionary<string, object>();
					string status = "available";
					if (historicalRev.IsDeleted())
					{
						status = "deleted";
					}
					// TODO: Detect missing revisions, set status="missing"
					revHistoryItem.Put("rev", historicalRev.GetRevId());
					revHistoryItem.Put("status", status);
					revsInfo.AddItem(revHistoryItem);
				}
			}
			IList<string> conflicts = null;
			if (contentOptions.Contains(TDDatabase.TDContentOptions.TDIncludeConflicts))
			{
				TDRevisionList revs = GetAllRevisionsOfDocumentID(docId, true);
				if (revs.Count > 1)
				{
					conflicts = new AList<string>();
					foreach (TDRevision historicalRev in revs)
					{
						if (!historicalRev.Equals(rev))
						{
							conflicts.AddItem(historicalRev.GetRevId());
						}
					}
				}
			}
			IDictionary<string, object> result = new Dictionary<string, object>();
			result.Put("_id", docId);
			result.Put("_rev", revId);
			if (rev.IsDeleted())
			{
				result.Put("_deleted", true);
			}
			if (attachmentsDict != null)
			{
				result.Put("_attachments", attachmentsDict);
			}
			if (localSeq != null)
			{
				result.Put("_local_seq", localSeq);
			}
			if (revHistory != null)
			{
				result.Put("_revisions", revHistory);
			}
			if (revsInfo != null)
			{
				result.Put("_revs_info", revsInfo);
			}
			if (conflicts != null)
			{
				result.Put("_conflicts", conflicts);
			}
			return result;
		}

		/// <summary>Inserts the _id, _rev and _attachments properties into the JSON data and stores it in rev.
		/// 	</summary>
		/// <remarks>
		/// Inserts the _id, _rev and _attachments properties into the JSON data and stores it in rev.
		/// Rev must already have its revID and sequence properties set.
		/// </remarks>
		public virtual void ExpandStoredJSONIntoRevisionWithAttachments(byte[] json, TDRevision
			 rev, EnumSet<TDDatabase.TDContentOptions> contentOptions)
		{
			IDictionary<string, object> extra = ExtraPropertiesForRevision(rev, contentOptions
				);
			if (json != null)
			{
				rev.SetJson(AppendDictToJSON(json, extra));
			}
			else
			{
				rev.SetProperties(extra);
			}
		}

		public virtual IDictionary<string, object> DocumentPropertiesFromJSON(byte[] json
			, string docId, string revId, long sequence, EnumSet<TDDatabase.TDContentOptions
			> contentOptions)
		{
			TDRevision rev = new TDRevision(docId, revId, false);
			rev.SetSequence(sequence);
			IDictionary<string, object> extra = ExtraPropertiesForRevision(rev, contentOptions
				);
			if (json == null)
			{
				return extra;
			}
			IDictionary<string, object> docProperties = null;
			try
			{
				docProperties = TDServer.GetObjectMapper().ReadValue<IDictionary>(json);
				docProperties.PutAll(extra);
				return docProperties;
			}
			catch (Exception e)
			{
				Log.E(Couchbase.TouchDB.TDDatabase.TAG, "Error serializing properties to JSON", e
					);
			}
			return docProperties;
		}

		public virtual TDRevision GetDocumentWithIDAndRev(string id, string rev, EnumSet<
			TDDatabase.TDContentOptions> contentOptions)
		{
			TDRevision result = null;
			string sql;
			Cursor cursor = null;
			try
			{
				cursor = null;
				string cols = "revid, deleted, sequence";
				if (!contentOptions.Contains(TDDatabase.TDContentOptions.TDNoBody))
				{
					cols += ", json";
				}
				if (rev != null)
				{
					sql = "SELECT " + cols + " FROM revs, docs WHERE docs.docid=? AND revs.doc_id=docs.doc_id AND revid=? LIMIT 1";
					string[] args = new string[] { id, rev };
					cursor = database.RawQuery(sql, args);
				}
				else
				{
					sql = "SELECT " + cols + " FROM revs, docs WHERE docs.docid=? AND revs.doc_id=docs.doc_id and current=1 and deleted=0 ORDER BY revid DESC LIMIT 1";
					string[] args = new string[] { id };
					cursor = database.RawQuery(sql, args);
				}
				if (cursor.MoveToFirst())
				{
					if (rev == null)
					{
						rev = cursor.GetString(0);
					}
					bool deleted = (cursor.GetInt(1) > 0);
					result = new TDRevision(id, rev, deleted);
					result.SetSequence(cursor.GetLong(2));
					if (!contentOptions.Equals(EnumSet.Of(TDDatabase.TDContentOptions.TDNoBody)))
					{
						byte[] json = null;
						if (!contentOptions.Contains(TDDatabase.TDContentOptions.TDNoBody))
						{
							json = cursor.GetBlob(3);
						}
						ExpandStoredJSONIntoRevisionWithAttachments(json, result, contentOptions);
					}
				}
			}
			catch (SQLException e)
			{
				Log.E(Couchbase.TouchDB.TDDatabase.TAG, "Error getting document with id and rev", 
					e);
			}
			finally
			{
				if (cursor != null)
				{
					cursor.Close();
				}
			}
			return result;
		}

		public virtual bool ExistsDocumentWithIDAndRev(string docId, string revId)
		{
			return GetDocumentWithIDAndRev(docId, revId, EnumSet.Of(TDDatabase.TDContentOptions
				.TDNoBody)) != null;
		}

		public virtual TDStatus LoadRevisionBody(TDRevision rev, EnumSet<TDDatabase.TDContentOptions
			> contentOptions)
		{
			if (rev.GetBody() != null)
			{
				return new TDStatus(TDStatus.OK);
			}
			////asert((rev.getDocId() != null) && (rev.getRevId() != null));
			Cursor cursor = null;
			TDStatus result = new TDStatus(TDStatus.NOT_FOUND);
			try
			{
				string sql = "SELECT sequence, json FROM revs, docs WHERE revid=? AND docs.docid=? AND revs.doc_id=docs.doc_id LIMIT 1";
				string[] args = new string[] { rev.GetRevId(), rev.GetDocId() };
				cursor = database.RawQuery(sql, args);
				if (cursor.MoveToFirst())
				{
					result.SetCode(TDStatus.OK);
					rev.SetSequence(cursor.GetLong(0));
					ExpandStoredJSONIntoRevisionWithAttachments(cursor.GetBlob(1), rev, contentOptions
						);
				}
			}
			catch (SQLException e)
			{
				Log.E(Couchbase.TouchDB.TDDatabase.TAG, "Error loading revision body", e);
				return new TDStatus(TDStatus.INTERNAL_SERVER_ERROR);
			}
			finally
			{
				if (cursor != null)
				{
					cursor.Close();
				}
			}
			return result;
		}

		public virtual long GetDocNumericID(string docId)
		{
			Cursor cursor = null;
			string[] args = new string[] { docId };
			long result = -1;
			try
			{
				cursor = database.RawQuery("SELECT doc_id FROM docs WHERE docid=?", args);
				if (cursor.MoveToFirst())
				{
					result = cursor.GetLong(0);
				}
				else
				{
					result = 0;
				}
			}
			catch (Exception e)
			{
				Log.E(Couchbase.TouchDB.TDDatabase.TAG, "Error getting doc numeric id", e);
			}
			finally
			{
				if (cursor != null)
				{
					cursor.Close();
				}
			}
			return result;
		}

		/// <summary>Returns all the known revisions (or all current/conflicting revisions) of a document.
		/// 	</summary>
		/// <remarks>Returns all the known revisions (or all current/conflicting revisions) of a document.
		/// 	</remarks>
		public virtual TDRevisionList GetAllRevisionsOfDocumentID(string docId, long docNumericID
			, bool onlyCurrent)
		{
			string sql = null;
			if (onlyCurrent)
			{
				sql = "SELECT sequence, revid, deleted FROM revs " + "WHERE doc_id=? AND current ORDER BY sequence DESC";
			}
			else
			{
				sql = "SELECT sequence, revid, deleted FROM revs " + "WHERE doc_id=? ORDER BY sequence DESC";
			}
			string[] args = new string[] { System.Convert.ToString(docNumericID) };
			Cursor cursor = null;
			cursor = database.RawQuery(sql, args);
			TDRevisionList result;
			try
			{
				cursor.MoveToFirst();
				result = new TDRevisionList();
				while (!cursor.IsAfterLast())
				{
					TDRevision rev = new TDRevision(docId, cursor.GetString(1), (cursor.GetInt(2) > 0
						));
					rev.SetSequence(cursor.GetLong(0));
					result.AddItem(rev);
					cursor.MoveToNext();
				}
			}
			catch (SQLException e)
			{
				Log.E(Couchbase.TouchDB.TDDatabase.TAG, "Error getting all revisions of document"
					, e);
				return null;
			}
			finally
			{
				if (cursor != null)
				{
					cursor.Close();
				}
			}
			return result;
		}

		public virtual TDRevisionList GetAllRevisionsOfDocumentID(string docId, bool onlyCurrent
			)
		{
			long docNumericId = GetDocNumericID(docId);
			if (docNumericId < 0)
			{
				return null;
			}
			else
			{
				if (docNumericId == 0)
				{
					return new TDRevisionList();
				}
				else
				{
					return GetAllRevisionsOfDocumentID(docId, docNumericId, onlyCurrent);
				}
			}
		}

		public virtual IList<string> GetConflictingRevisionIDsOfDocID(string docID)
		{
			long docIdNumeric = GetDocNumericID(docID);
			if (docIdNumeric < 0)
			{
				return null;
			}
			IList<string> result = new AList<string>();
			Cursor cursor = null;
			try
			{
				string[] args = new string[] { System.Convert.ToString(docIdNumeric) };
				cursor = database.RawQuery("SELECT revid FROM revs WHERE doc_id=? AND current " +
					 "ORDER BY revid DESC OFFSET 1", args);
				cursor.MoveToFirst();
				while (!cursor.IsAfterLast())
				{
					result.AddItem(cursor.GetString(0));
					cursor.MoveToNext();
				}
			}
			catch (SQLException e)
			{
				Log.E(Couchbase.TouchDB.TDDatabase.TAG, "Error getting all revisions of document"
					, e);
				return null;
			}
			finally
			{
				if (cursor != null)
				{
					cursor.Close();
				}
			}
			return result;
		}

		public virtual string FindCommonAncestorOf(TDRevision rev, IList<string> revIDs)
		{
			string result = null;
			if (revIDs.Count == 0)
			{
				return null;
			}
			string docId = rev.GetDocId();
			long docNumericID = GetDocNumericID(docId);
			if (docNumericID <= 0)
			{
				return null;
			}
			string quotedRevIds = JoinQuoted(revIDs);
			string sql = "SELECT revid FROM revs " + "WHERE doc_id=? and revid in (" + quotedRevIds
				 + ") and revid <= ? " + "ORDER BY revid DESC LIMIT 1";
			string[] args = new string[] { System.Convert.ToString(docNumericID) };
			Cursor cursor = null;
			try
			{
				cursor = database.RawQuery(sql, args);
				cursor.MoveToFirst();
				if (!cursor.IsAfterLast())
				{
					result = cursor.GetString(0);
				}
			}
			catch (SQLException e)
			{
				Log.E(Couchbase.TouchDB.TDDatabase.TAG, "Error getting all revisions of document"
					, e);
			}
			finally
			{
				if (cursor != null)
				{
					cursor.Close();
				}
			}
			return result;
		}

		/// <summary>Returns an array of TDRevs in reverse chronological order, starting with the given revision.
		/// 	</summary>
		/// <remarks>Returns an array of TDRevs in reverse chronological order, starting with the given revision.
		/// 	</remarks>
		public virtual IList<TDRevision> GetRevisionHistory(TDRevision rev)
		{
			string docId = rev.GetDocId();
			string revId = rev.GetRevId();
			////asert((docId != null) && (revId != null));
			long docNumericId = GetDocNumericID(docId);
			if (docNumericId < 0)
			{
				return null;
			}
			else
			{
				if (docNumericId == 0)
				{
					return new AList<TDRevision>();
				}
			}
			string sql = "SELECT sequence, parent, revid, deleted FROM revs " + "WHERE doc_id=? ORDER BY sequence DESC";
			string[] args = new string[] { System.Convert.ToString(docNumericId) };
			Cursor cursor = null;
			IList<TDRevision> result;
			try
			{
				cursor = database.RawQuery(sql, args);
				cursor.MoveToFirst();
				long lastSequence = 0;
				result = new AList<TDRevision>();
				while (!cursor.IsAfterLast())
				{
					long sequence = cursor.GetLong(0);
					bool matches = false;
					if (lastSequence == 0)
					{
						matches = revId.Equals(cursor.GetString(2));
					}
					else
					{
						matches = (sequence == lastSequence);
					}
					if (matches)
					{
						revId = cursor.GetString(2);
						bool deleted = (cursor.GetInt(3) > 0);
						TDRevision aRev = new TDRevision(docId, revId, deleted);
						aRev.SetSequence(cursor.GetLong(0));
						result.AddItem(aRev);
						lastSequence = cursor.GetLong(1);
						if (lastSequence == 0)
						{
							break;
						}
					}
					cursor.MoveToNext();
				}
			}
			catch (SQLException e)
			{
				Log.E(Couchbase.TouchDB.TDDatabase.TAG, "Error getting revision history", e);
				return null;
			}
			finally
			{
				if (cursor != null)
				{
					cursor.Close();
				}
			}
			return result;
		}

		// Splits a revision ID into its generation number and opaque suffix string
		public static int ParseRevIDNumber(string rev)
		{
			int result = -1;
			int dashPos = rev.IndexOf("-");
			if (dashPos >= 0)
			{
				try
				{
					result = System.Convert.ToInt32(Sharpen.Runtime.Substring(rev, 0, dashPos));
				}
				catch (FormatException)
				{
				}
			}
			// ignore, let it return -1
			return result;
		}

		// Splits a revision ID into its generation number and opaque suffix string
		public static string ParseRevIDSuffix(string rev)
		{
			string result = null;
			int dashPos = rev.IndexOf("-");
			if (dashPos >= 0)
			{
				result = Sharpen.Runtime.Substring(rev, dashPos + 1);
			}
			return result;
		}

		public static IDictionary<string, object> MakeRevisionHistoryDict(IList<TDRevision
			> history)
		{
			if (history == null)
			{
				return null;
			}
			// Try to extract descending numeric prefixes:
			IList<string> suffixes = new AList<string>();
			int start = -1;
			int lastRevNo = -1;
			foreach (TDRevision rev in history)
			{
				int revNo = ParseRevIDNumber(rev.GetRevId());
				string suffix = ParseRevIDSuffix(rev.GetRevId());
				if (revNo > 0 && suffix.Length > 0)
				{
					if (start < 0)
					{
						start = revNo;
					}
					else
					{
						if (revNo != lastRevNo - 1)
						{
							start = -1;
							break;
						}
					}
					lastRevNo = revNo;
					suffixes.AddItem(suffix);
				}
				else
				{
					start = -1;
					break;
				}
			}
			IDictionary<string, object> result = new Dictionary<string, object>();
			if (start == -1)
			{
				// we failed to build sequence, just stuff all the revs in list
				suffixes = new AList<string>();
				foreach (TDRevision rev_1 in history)
				{
					suffixes.AddItem(rev_1.GetRevId());
				}
			}
			else
			{
				result.Put("start", start);
			}
			result.Put("ids", suffixes);
			return result;
		}

		/// <summary>Returns the revision history as a _revisions dictionary, as returned by the REST API's ?revs=true option.
		/// 	</summary>
		/// <remarks>Returns the revision history as a _revisions dictionary, as returned by the REST API's ?revs=true option.
		/// 	</remarks>
		public virtual IDictionary<string, object> GetRevisionHistoryDict(TDRevision rev)
		{
			return MakeRevisionHistoryDict(GetRevisionHistory(rev));
		}

		public virtual TDRevisionList ChangesSince(long lastSeq, TDChangesOptions options
			, TDFilterBlock filter)
		{
			// http://wiki.apache.org/couchdb/HTTP_database_API#Changes
			if (options == null)
			{
				options = new TDChangesOptions();
			}
			bool includeDocs = options.IsIncludeDocs() || (filter != null);
			string additionalSelectColumns = string.Empty;
			if (includeDocs)
			{
				additionalSelectColumns = ", json";
			}
			string sql = "SELECT sequence, revs.doc_id, docid, revid, deleted" + additionalSelectColumns
				 + " FROM revs, docs " + "WHERE sequence > ? AND current=1 " + "AND revs.doc_id = docs.doc_id "
				 + "ORDER BY revs.doc_id, revid DESC";
			string[] args = new string[] { System.Convert.ToString(lastSeq) };
			Cursor cursor = null;
			TDRevisionList changes = null;
			try
			{
				cursor = database.RawQuery(sql, args);
				cursor.MoveToFirst();
				changes = new TDRevisionList();
				long lastDocId = 0;
				while (!cursor.IsAfterLast())
				{
					if (!options.IsIncludeConflicts())
					{
						// Only count the first rev for a given doc (the rest will be losing conflicts):
						long docNumericId = cursor.GetLong(1);
						if (docNumericId == lastDocId)
						{
							cursor.MoveToNext();
							continue;
						}
						lastDocId = docNumericId;
					}
					TDRevision rev = new TDRevision(cursor.GetString(2), cursor.GetString(3), (cursor
						.GetInt(4) > 0));
					rev.SetSequence(cursor.GetLong(0));
					if (includeDocs)
					{
						ExpandStoredJSONIntoRevisionWithAttachments(cursor.GetBlob(5), rev, options.GetContentOptions
							());
					}
					if ((filter == null) || (filter.Filter(rev)))
					{
						changes.AddItem(rev);
					}
					cursor.MoveToNext();
				}
			}
			catch (SQLException e)
			{
				Log.E(Couchbase.TouchDB.TDDatabase.TAG, "Error looking for changes", e);
			}
			finally
			{
				if (cursor != null)
				{
					cursor.Close();
				}
			}
			if (options.IsSortBySequence())
			{
				changes.SortBySequence();
			}
			changes.Limit(options.GetLimit());
			return changes;
		}

		/// <summary>Define or clear a named filter function.</summary>
		/// <remarks>
		/// Define or clear a named filter function.
		/// These aren't used directly by TDDatabase, but they're looked up by TDRouter when a _changes request has a ?filter parameter.
		/// </remarks>
		public virtual void DefineFilter(string filterName, TDFilterBlock filter)
		{
			if (filters == null)
			{
				filters = new Dictionary<string, TDFilterBlock>();
			}
			filters.Put(filterName, filter);
		}

		public virtual TDFilterBlock GetFilterNamed(string filterName)
		{
			TDFilterBlock result = null;
			if (filters != null)
			{
				result = filters.Get(filterName);
			}
			return result;
		}

		/// <summary>VIEWS:</summary>
		public virtual TDView RegisterView(TDView view)
		{
			if (view == null)
			{
				return null;
			}
			if (views == null)
			{
				views = new Dictionary<string, TDView>();
			}
			views.Put(view.GetName(), view);
			return view;
		}

		public virtual TDView GetViewNamed(string name)
		{
			TDView view = null;
			if (views != null)
			{
				view = views.Get(name);
			}
			if (view != null)
			{
				return view;
			}
			return RegisterView(new TDView(this, name));
		}

		public virtual TDView GetExistingViewNamed(string name)
		{
			TDView view = null;
			if (views != null)
			{
				view = views.Get(name);
			}
			if (view != null)
			{
				return view;
			}
			view = new TDView(this, name);
			if (view.GetViewId() == 0)
			{
				return null;
			}
			return RegisterView(view);
		}

		public virtual IList<TDView> GetAllViews()
		{
			Cursor cursor = null;
			IList<TDView> result = null;
			try
			{
				cursor = database.RawQuery("SELECT name FROM views", null);
				cursor.MoveToFirst();
				result = new AList<TDView>();
				while (!cursor.IsAfterLast())
				{
					result.AddItem(GetViewNamed(cursor.GetString(0)));
					cursor.MoveToNext();
				}
			}
			catch (Exception e)
			{
				Log.E(Couchbase.TouchDB.TDDatabase.TAG, "Error getting all views", e);
			}
			finally
			{
				if (cursor != null)
				{
					cursor.Close();
				}
			}
			return result;
		}

		public virtual TDStatus DeleteViewNamed(string name)
		{
			TDStatus result = new TDStatus(TDStatus.INTERNAL_SERVER_ERROR);
			try
			{
				string[] whereArgs = new string[] { name };
				int rowsAffected = database.Delete("views", "name=?", whereArgs);
				if (rowsAffected > 0)
				{
					result.SetCode(TDStatus.OK);
				}
				else
				{
					result.SetCode(TDStatus.NOT_FOUND);
				}
			}
			catch (SQLException e)
			{
				Log.E(Couchbase.TouchDB.TDDatabase.TAG, "Error deleting view", e);
			}
			return result;
		}

		//FIX: This has a lot of code in common with -[TDView queryWithOptions:status:]. Unify the two!
		public virtual IDictionary<string, object> GetDocsWithIDs(IList<string> docIDs, TDQueryOptions
			 options)
		{
			if (options == null)
			{
				options = new TDQueryOptions();
			}
			long updateSeq = 0;
			if (options.IsUpdateSeq())
			{
				updateSeq = GetLastSequence();
			}
			// TODO: needs to be atomic with the following SELECT
			// Generate the SELECT statement, based on the options:
			string additionalCols = string.Empty;
			if (options.IsIncludeDocs())
			{
				additionalCols = ", json, sequence";
			}
			string sql = "SELECT revs.doc_id, docid, revid, deleted" + additionalCols + " FROM revs, docs WHERE";
			if (docIDs != null)
			{
				sql += " docid IN (" + JoinQuoted(docIDs) + ")";
			}
			else
			{
				sql += " deleted=0";
			}
			sql += " AND current=1 AND docs.doc_id = revs.doc_id";
			IList<string> argsList = new AList<string>();
			object minKey = options.GetStartKey();
			object maxKey = options.GetEndKey();
			bool inclusiveMin = true;
			bool inclusiveMax = options.IsInclusiveEnd();
			if (options.IsDescending())
			{
				minKey = maxKey;
				maxKey = options.GetStartKey();
				inclusiveMin = inclusiveMax;
				inclusiveMax = true;
			}
			if (minKey != null)
			{
				////asert(minKey instanceof String);
				if (inclusiveMin)
				{
					sql += " AND docid >= ?";
				}
				else
				{
					sql += " AND docid > ?";
				}
				argsList.AddItem((string)minKey);
			}
			if (maxKey != null)
			{
				////asert(maxKey instanceof String);
				if (inclusiveMax)
				{
					sql += " AND docid <= ?";
				}
				else
				{
					sql += " AND docid < ?";
				}
				argsList.AddItem((string)maxKey);
			}
			string order = "ASC";
			if (options.IsDescending())
			{
				order = "DESC";
			}
			sql += " ORDER BY docid " + order + ", revid DESC LIMIT ? OFFSET ?";
			argsList.AddItem(Sharpen.Extensions.ToString(options.GetLimit()));
			argsList.AddItem(Sharpen.Extensions.ToString(options.GetSkip()));
			Cursor cursor = null;
			long lastDocID = 0;
			IList<IDictionary<string, object>> rows = null;
			try
			{
				cursor = database.RawQuery(sql, Sharpen.Collections.ToArray(argsList, new string[
					argsList.Count]));
				cursor.MoveToFirst();
				rows = new AList<IDictionary<string, object>>();
				while (!cursor.IsAfterLast())
				{
					long docNumericID = cursor.GetLong(0);
					if (docNumericID == lastDocID)
					{
						cursor.MoveToNext();
						continue;
					}
					lastDocID = docNumericID;
					string docId = cursor.GetString(1);
					string revId = cursor.GetString(2);
					IDictionary<string, object> docContents = null;
					bool deleted = cursor.GetInt(3) > 0;
					if (options.IsIncludeDocs() && !deleted)
					{
						byte[] json = cursor.GetBlob(4);
						long sequence = cursor.GetLong(5);
						docContents = DocumentPropertiesFromJSON(json, docId, revId, sequence, options.GetContentOptions
							());
					}
					IDictionary<string, object> valueMap = new Dictionary<string, object>();
					valueMap.Put("rev", revId);
					IDictionary<string, object> change = new Dictionary<string, object>();
					change.Put("id", docId);
					change.Put("key", docId);
					change.Put("value", valueMap);
					if (docContents != null)
					{
						change.Put("doc", docContents);
					}
					if (deleted)
					{
						change.Put("deleted", true);
					}
					rows.AddItem(change);
					cursor.MoveToNext();
				}
			}
			catch (SQLException e)
			{
				Log.E(Couchbase.TouchDB.TDDatabase.TAG, "Error getting all docs", e);
				return null;
			}
			finally
			{
				if (cursor != null)
				{
					cursor.Close();
				}
			}
			int totalRows = cursor.GetCount();
			//??? Is this true, or does it ignore limit/offset?
			IDictionary<string, object> result = new Dictionary<string, object>();
			result.Put("rows", rows);
			result.Put("total_rows", totalRows);
			result.Put("offset", options.GetSkip());
			if (updateSeq != 0)
			{
				result.Put("update_seq", updateSeq);
			}
			return result;
		}

		public virtual IDictionary<string, object> GetAllDocs(TDQueryOptions options)
		{
			return GetDocsWithIDs(null, options);
		}

		public virtual TDStatus InsertAttachmentForSequenceWithNameAndType(InputStream contentStream
			, long sequence, string name, string contentType, int revpos)
		{
			////asert(sequence > 0);
			////asert(name != null);
			TDBlobKey key = new TDBlobKey();
			if (!attachments.StoreBlobStream(contentStream, key))
			{
				return new TDStatus(TDStatus.INTERNAL_SERVER_ERROR);
			}
			byte[] keyData = key.GetBytes();
			try
			{
				ContentValues args = new ContentValues();
				args.Put("sequence", sequence);
				args.Put("filename", name);
				args.Put("key", keyData);
				args.Put("type", contentType);
				args.Put("length", attachments.GetSizeOfBlob(key));
				args.Put("revpos", revpos);
				database.Insert("attachments", null, args);
				return new TDStatus(TDStatus.CREATED);
			}
			catch (SQLException e)
			{
				Log.E(Couchbase.TouchDB.TDDatabase.TAG, "Error inserting attachment", e);
				return new TDStatus(TDStatus.INTERNAL_SERVER_ERROR);
			}
		}

		public virtual TDStatus CopyAttachmentNamedFromSequenceToSequence(string name, long
			 fromSeq, long toSeq)
		{
			//asert(name != null);
			//asert(toSeq > 0);
			if (fromSeq < 0)
			{
				return new TDStatus(TDStatus.NOT_FOUND);
			}
			Cursor cursor = null;
			string[] args = new string[] { System.Convert.ToString(toSeq), name, System.Convert.ToString
				(fromSeq), name };
			try
			{
				database.ExecSQL("INSERT INTO attachments (sequence, filename, key, type, length, revpos) "
					 + "SELECT ?, ?, key, type, length, revpos FROM attachments " + "WHERE sequence=? AND filename=?"
					, args);
				cursor = database.RawQuery("SELECT changes()", null);
				cursor.MoveToFirst();
				int rowsUpdated = cursor.GetInt(0);
				if (rowsUpdated == 0)
				{
					// Oops. This means a glitch in our attachment-management or pull code,
					// or else a bug in the upstream server.
					Log.W(Couchbase.TouchDB.TDDatabase.TAG, "Can't find inherited attachment " + name
						 + " from seq# " + System.Convert.ToString(fromSeq) + " to copy to " + System.Convert.ToString
						(toSeq));
					return new TDStatus(TDStatus.NOT_FOUND);
				}
				else
				{
					return new TDStatus(TDStatus.OK);
				}
			}
			catch (SQLException e)
			{
				Log.E(Couchbase.TouchDB.TDDatabase.TAG, "Error copying attachment", e);
				return new TDStatus(TDStatus.INTERNAL_SERVER_ERROR);
			}
			finally
			{
				if (cursor != null)
				{
					cursor.Close();
				}
			}
		}

		/// <summary>Returns the content and MIME type of an attachment</summary>
		public virtual TDAttachment GetAttachmentForSequence(long sequence, string filename
			, TDStatus status)
		{
			////asert(sequence > 0);
			////asert(filename != null);
			Cursor cursor = null;
			string[] args = new string[] { System.Convert.ToString(sequence), filename };
			try
			{
				cursor = database.RawQuery("SELECT key, type FROM attachments WHERE sequence=? AND filename=?"
					, args);
				if (!cursor.MoveToFirst())
				{
					status.SetCode(TDStatus.NOT_FOUND);
					return null;
				}
				byte[] keyData = cursor.GetBlob(0);
				//TODO add checks on key here? (ios version)
				TDBlobKey key = new TDBlobKey(keyData);
				InputStream contentStream = attachments.BlobStreamForKey(key);
				if (contentStream == null)
				{
					Log.E(Couchbase.TouchDB.TDDatabase.TAG, "Failed to load attachment");
					status.SetCode(TDStatus.INTERNAL_SERVER_ERROR);
					return null;
				}
				else
				{
					status.SetCode(TDStatus.OK);
					TDAttachment result = new TDAttachment();
					result.SetContentStream(contentStream);
					result.SetContentType(cursor.GetString(1));
					return result;
				}
			}
			catch (SQLException)
			{
				status.SetCode(TDStatus.INTERNAL_SERVER_ERROR);
				return null;
			}
			finally
			{
				if (cursor != null)
				{
					cursor.Close();
				}
			}
		}

		/// <summary>Constructs an "_attachments" dictionary for a revision, to be inserted in its JSON body.
		/// 	</summary>
		/// <remarks>Constructs an "_attachments" dictionary for a revision, to be inserted in its JSON body.
		/// 	</remarks>
		public virtual IDictionary<string, object> GetAttachmentsDictForSequenceWithContent
			(long sequence, bool withContent)
		{
			////asert(sequence > 0);
			Cursor cursor = null;
			string[] args = new string[] { System.Convert.ToString(sequence) };
			try
			{
				cursor = database.RawQuery("SELECT filename, key, type, length, revpos FROM attachments WHERE sequence=?"
					, args);
				if (!cursor.MoveToFirst())
				{
					return null;
				}
				IDictionary<string, object> result = new Dictionary<string, object>();
				while (!cursor.IsAfterLast())
				{
					byte[] keyData = cursor.GetBlob(1);
					TDBlobKey key = new TDBlobKey(keyData);
					string digestString = "sha1-" + Base64.EncodeBytes(keyData);
					string dataBase64 = null;
					if (withContent)
					{
						byte[] data = attachments.BlobForKey(key);
						if (data != null)
						{
							dataBase64 = Base64.EncodeBytes(data);
						}
						else
						{
							Log.W(Couchbase.TouchDB.TDDatabase.TAG, "Error loading attachment");
						}
					}
					IDictionary<string, object> attachment = new Dictionary<string, object>();
					if (dataBase64 == null)
					{
						attachment.Put("stub", true);
					}
					else
					{
						attachment.Put("data", dataBase64);
					}
					attachment.Put("digest", digestString);
					attachment.Put("content_type", cursor.GetString(2));
					attachment.Put("length", cursor.GetInt(3));
					attachment.Put("revpos", cursor.GetInt(4));
					result.Put(cursor.GetString(0), attachment);
					cursor.MoveToNext();
				}
				return result;
			}
			catch (SQLException e)
			{
				Log.E(Couchbase.TouchDB.TDDatabase.TAG, "Error getting attachments for sequence", 
					e);
				return null;
			}
			finally
			{
				if (cursor != null)
				{
					cursor.Close();
				}
			}
		}

		/// <summary>Modifies a TDRevision's body by changing all attachments with revpos &lt; minRevPos into stubs.
		/// 	</summary>
		/// <remarks>Modifies a TDRevision's body by changing all attachments with revpos &lt; minRevPos into stubs.
		/// 	</remarks>
		/// <param name="rev"></param>
		/// <param name="minRevPos"></param>
		public virtual void StubOutAttachmentsIn(TDRevision rev, int minRevPos)
		{
			if (minRevPos <= 1)
			{
				return;
			}
			IDictionary<string, object> properties = (IDictionary<string, object>)rev.GetProperties
				();
			IDictionary<string, object> attachments = null;
			if (properties != null)
			{
				attachments = (IDictionary<string, object>)properties.Get("_attachments");
			}
			IDictionary<string, object> editedProperties = null;
			IDictionary<string, object> editedAttachments = null;
			foreach (string name in attachments.Keys)
			{
				IDictionary<string, object> attachment = (IDictionary<string, object>)attachments
					.Get(name);
				int revPos = (int)attachment.Get("revpos");
				object stub = attachment.Get("stub");
				if (revPos > 0 && revPos < minRevPos && (stub == null))
				{
					// Strip this attachment's body. First make its dictionary mutable:
					if (editedProperties == null)
					{
						editedProperties = new Dictionary<string, object>(properties);
						editedAttachments = new Dictionary<string, object>(attachments);
						editedProperties.Put("_attachments", editedAttachments);
					}
					// ...then remove the 'data' and 'follows' key:
					IDictionary<string, object> editedAttachment = new Dictionary<string, object>(attachment
						);
					Sharpen.Collections.Remove(editedAttachment, "data");
					Sharpen.Collections.Remove(editedAttachment, "follows");
					editedAttachment.Put("stub", true);
					editedAttachments.Put(name, editedAttachment);
					Log.D(Couchbase.TouchDB.TDDatabase.TAG, "Stubbed out attachment" + rev + " " + name
						 + ": revpos" + revPos + " " + minRevPos);
				}
			}
			if (editedProperties != null)
			{
				rev.SetProperties(editedProperties);
			}
		}

		/// <summary>Given a newly-added revision, adds the necessary attachment rows to the database and stores inline attachments into the blob store.
		/// 	</summary>
		/// <remarks>Given a newly-added revision, adds the necessary attachment rows to the database and stores inline attachments into the blob store.
		/// 	</remarks>
		public virtual TDStatus ProcessAttachmentsForRevision(TDRevision rev, long parentSequence
			)
		{
			//asert(rev != null);
			long newSequence = rev.GetSequence();
			//asert(newSequence > parentSequence);
			// If there are no attachments in the new rev, there's nothing to do:
			IDictionary<string, object> newAttachments = null;
			IDictionary<string, object> properties = (IDictionary<string, object>)rev.GetProperties
				();
			if (properties != null)
			{
				newAttachments = (IDictionary<string, object>)properties.Get("_attachments");
			}
			if (newAttachments == null || newAttachments.Count == 0 || rev.IsDeleted())
			{
				return new TDStatus(TDStatus.OK);
			}
			foreach (string name in newAttachments.Keys)
			{
				TDStatus status = new TDStatus();
				IDictionary<string, object> newAttach = (IDictionary<string, object>)newAttachments
					.Get(name);
				string newContentBase64 = (string)newAttach.Get("data");
				if (newContentBase64 != null)
				{
					// New item contains data, so insert it. First decode the data:
					byte[] newContents;
					try
					{
						newContents = Base64.Decode(newContentBase64);
					}
					catch (IOException e)
					{
						Log.E(Couchbase.TouchDB.TDDatabase.TAG, "IOExeption parsing base64", e);
						return new TDStatus(TDStatus.BAD_REQUEST);
					}
					if (newContents == null)
					{
						return new TDStatus(TDStatus.BAD_REQUEST);
					}
					// Now determine the revpos, i.e. generation # this was added in. Usually this is
					// implicit, but a rev being pulled in replication will have it set already.
					int generation = rev.GetGeneration();
					//asert(generation > 0);
					object revposObj = newAttach.Get("revpos");
					int revpos = generation;
					if (revposObj != null && revposObj is int)
					{
						revpos = ((int)revposObj);
					}
					if (revpos > generation)
					{
						return new TDStatus(TDStatus.BAD_REQUEST);
					}
					// Finally insert the attachment:
					status = InsertAttachmentForSequenceWithNameAndType(new ByteArrayInputStream(newContents
						), newSequence, name, (string)newAttach.Get("content_type"), revpos);
				}
				else
				{
					// It's just a stub, so copy the previous revision's attachment entry:
					//? Should I enforce that the type and digest (if any) match?
					status = CopyAttachmentNamedFromSequenceToSequence(name, parentSequence, newSequence
						);
				}
				if (!status.IsSuccessful())
				{
					return status;
				}
			}
			return new TDStatus(TDStatus.OK);
		}

		/// <summary>Updates or deletes an attachment, creating a new document revision in the process.
		/// 	</summary>
		/// <remarks>
		/// Updates or deletes an attachment, creating a new document revision in the process.
		/// Used by the PUT / DELETE methods called on attachment URLs.
		/// </remarks>
		public virtual TDRevision UpdateAttachment(string filename, InputStream contentStream
			, string contentType, string docID, string oldRevID, TDStatus status)
		{
			status.SetCode(TDStatus.BAD_REQUEST);
			if (filename == null || filename.Length == 0 || (contentStream != null && contentType
				 == null) || (oldRevID != null && docID == null) || (contentStream != null && docID
				 == null))
			{
				return null;
			}
			BeginTransaction();
			try
			{
				TDRevision oldRev = new TDRevision(docID, oldRevID, false);
				if (oldRevID != null)
				{
					// Load existing revision if this is a replacement:
					TDStatus loadStatus = LoadRevisionBody(oldRev, EnumSet.NoneOf<TDDatabase.TDContentOptions
						>());
					status.SetCode(loadStatus.GetCode());
					if (!status.IsSuccessful())
					{
						if (status.GetCode() == TDStatus.NOT_FOUND && ExistsDocumentWithIDAndRev(docID, null
							))
						{
							status.SetCode(TDStatus.CONFLICT);
						}
						// if some other revision exists, it's a conflict
						return null;
					}
					IDictionary<string, object> attachments = (IDictionary<string, object>)oldRev.GetProperties
						().Get("_attachments");
					if (contentStream == null && attachments != null && !attachments.ContainsKey(filename
						))
					{
						status.SetCode(TDStatus.NOT_FOUND);
						return null;
					}
					// Remove the _attachments stubs so putRevision: doesn't copy the rows for me
					// OPT: Would be better if I could tell loadRevisionBody: not to add it
					if (attachments != null)
					{
						IDictionary<string, object> properties = new Dictionary<string, object>(oldRev.GetProperties
							());
						Sharpen.Collections.Remove(properties, "_attachments");
						oldRev.SetBody(new TDBody(properties));
					}
				}
				else
				{
					// If this creates a new doc, it needs a body:
					oldRev.SetBody(new TDBody(new Dictionary<string, object>()));
				}
				// Create a new revision:
				TDRevision newRev = PutRevision(oldRev, oldRevID, false, status);
				if (newRev == null)
				{
					return null;
				}
				if (oldRevID != null)
				{
					// Copy all attachment rows _except_ for the one being updated:
					string[] args = new string[] { System.Convert.ToString(newRev.GetSequence()), System.Convert.ToString
						(oldRev.GetSequence()), filename };
					database.ExecSQL("INSERT INTO attachments " + "(sequence, filename, key, type, length, revpos) "
						 + "SELECT ?, filename, key, type, length, revpos FROM attachments " + "WHERE sequence=? AND filename != ?"
						, args);
				}
				if (contentStream != null)
				{
					// If not deleting, add a new attachment entry:
					TDStatus insertStatus = InsertAttachmentForSequenceWithNameAndType(contentStream, 
						newRev.GetSequence(), filename, contentType, newRev.GetGeneration());
					status.SetCode(insertStatus.GetCode());
					if (!status.IsSuccessful())
					{
						return null;
					}
				}
				status.SetCode((contentStream != null) ? TDStatus.CREATED : TDStatus.OK);
				return newRev;
			}
			catch (SQLException e)
			{
				Log.E(TAG, "Error uploading attachment", e);
				status.SetCode(TDStatus.INTERNAL_SERVER_ERROR);
				return null;
			}
			finally
			{
				EndTransaction(status.IsSuccessful());
			}
		}

		/// <summary>Deletes obsolete attachments from the database and blob store.</summary>
		/// <remarks>Deletes obsolete attachments from the database and blob store.</remarks>
		public virtual TDStatus GarbageCollectAttachments()
		{
			// First delete attachment rows for already-cleared revisions:
			// OPT: Could start after last sequence# we GC'd up to
			try
			{
				database.ExecSQL("DELETE FROM attachments WHERE sequence IN " + "(SELECT sequence from revs WHERE json IS null)"
					);
			}
			catch (SQLException e)
			{
				Log.E(Couchbase.TouchDB.TDDatabase.TAG, "Error deleting attachments", e);
			}
			// Now collect all remaining attachment IDs and tell the store to delete all but these:
			Cursor cursor = null;
			try
			{
				cursor = database.RawQuery("SELECT DISTINCT key FROM attachments", null);
				cursor.MoveToFirst();
				IList<TDBlobKey> allKeys = new AList<TDBlobKey>();
				while (!cursor.IsAfterLast())
				{
					TDBlobKey key = new TDBlobKey(cursor.GetBlob(0));
					allKeys.AddItem(key);
					cursor.MoveToNext();
				}
				int numDeleted = attachments.DeleteBlobsExceptWithKeys(allKeys);
				if (numDeleted < 0)
				{
					return new TDStatus(TDStatus.INTERNAL_SERVER_ERROR);
				}
				Log.V(Couchbase.TouchDB.TDDatabase.TAG, "Deleted " + numDeleted + " attachments");
				return new TDStatus(TDStatus.OK);
			}
			catch (SQLException e)
			{
				Log.E(Couchbase.TouchDB.TDDatabase.TAG, "Error finding attachment keys in use", e
					);
				return new TDStatus(TDStatus.INTERNAL_SERVER_ERROR);
			}
			finally
			{
				if (cursor != null)
				{
					cursor.Close();
				}
			}
		}

		/// <summary>DOCUMENT & REV IDS:</summary>
		public static bool IsValidDocumentId(string id)
		{
			// http://wiki.apache.org/couchdb/HTTP_Document_API#Documents
			if (id == null || id.Length == 0)
			{
				return false;
			}
			if (id[0] == '_')
			{
				return (id.StartsWith("_design/"));
			}
			return true;
		}

		// "_local/*" is not a valid document ID. Local docs have their own API and shouldn't get here.
		public static string GenerateDocumentId()
		{
			return TDMisc.TDCreateUUID();
		}

		public virtual string GenerateNextRevisionID(string revisionId)
		{
			// Revision IDs have a generation count, a hyphen, and a UUID.
			int generation = 0;
			if (revisionId != null)
			{
				generation = TDRevision.GenerationFromRevID(revisionId);
				if (generation == 0)
				{
					return null;
				}
			}
			string digest = TDMisc.TDCreateUUID();
			//TODO: Generate canonical digest of body
			return Sharpen.Extensions.ToString(generation + 1) + "-" + digest;
		}

		public virtual long InsertDocumentID(string docId)
		{
			long rowId = -1;
			try
			{
				ContentValues args = new ContentValues();
				args.Put("docid", docId);
				rowId = database.Insert("docs", null, args);
			}
			catch (Exception e)
			{
				Log.E(Couchbase.TouchDB.TDDatabase.TAG, "Error inserting document id", e);
			}
			return rowId;
		}

		public virtual long GetOrInsertDocNumericID(string docId)
		{
			long docNumericId = GetDocNumericID(docId);
			if (docNumericId == 0)
			{
				docNumericId = InsertDocumentID(docId);
			}
			return docNumericId;
		}

		/// <summary>Parses the _revisions dict from a document into an array of revision ID strings
		/// 	</summary>
		public static IList<string> ParseCouchDBRevisionHistory(IDictionary<string, object
			> docProperties)
		{
			IDictionary<string, object> revisions = (IDictionary<string, object>)docProperties
				.Get("_revisions");
			if (revisions == null)
			{
				return null;
			}
			IList<string> revIDs = (IList<string>)revisions.Get("ids");
			int start = (int)revisions.Get("start");
			if (start != null)
			{
				for (int i = 0; i < revIDs.Count; i++)
				{
					string revID = revIDs[i];
					revIDs.Set(i, Sharpen.Extensions.ToString(start--) + "-" + revID);
				}
			}
			return revIDs;
		}

		/// <summary>INSERTION:</summary>
		public virtual byte[] EncodeDocumentJSON(TDRevision rev)
		{
			IDictionary<string, object> origProps = rev.GetProperties();
			if (origProps == null)
			{
				return null;
			}
			// Don't allow any "_"-prefixed keys. Known ones we'll ignore, unknown ones are an error.
			IDictionary<string, object> properties = new Dictionary<string, object>(origProps
				.Count);
			foreach (string key in origProps.Keys)
			{
				if (key.StartsWith("_"))
				{
					if (!KNOWN_SPECIAL_KEYS.Contains(key))
					{
						Log.E(TAG, "TDDatabase: Invalid top-level key '" + key + "' in document to be inserted"
							);
						return null;
					}
				}
				else
				{
					properties.Put(key, origProps.Get(key));
				}
			}
			byte[] json = null;
			try
			{
				json = TDServer.GetObjectMapper().WriteValueAsBytes(properties);
			}
			catch (Exception e)
			{
				Log.E(Couchbase.TouchDB.TDDatabase.TAG, "Error serializing " + rev + " to JSON", 
					e);
			}
			return json;
		}

		public virtual void NotifyChange(TDRevision rev, Uri source)
		{
			IDictionary<string, object> changeNotification = new Dictionary<string, object>();
			changeNotification.Put("rev", rev);
			changeNotification.Put("seq", rev.GetSequence());
			if (source != null)
			{
				changeNotification.Put("source", source);
			}
			SetChanged();
			NotifyObservers(changeNotification);
		}

		public virtual long InsertRevision(TDRevision rev, long docNumericID, long parentSequence
			, bool current, byte[] data)
		{
			long rowId = 0;
			try
			{
				ContentValues args = new ContentValues();
				args.Put("doc_id", docNumericID);
				args.Put("revid", rev.GetRevId());
				if (parentSequence != 0)
				{
					args.Put("parent", parentSequence);
				}
				args.Put("current", current);
				args.Put("deleted", rev.IsDeleted());
				args.Put("json", data);
				rowId = database.Insert("revs", null, args);
				rev.SetSequence(rowId);
			}
			catch (Exception e)
			{
				Log.E(Couchbase.TouchDB.TDDatabase.TAG, "Error inserting revision", e);
			}
			return rowId;
		}

		private TDRevision PutRevision(TDRevision rev, string prevRevId, TDStatus resultStatus
			)
		{
			return PutRevision(rev, prevRevId, false, resultStatus);
		}

		/// <summary>Stores a new (or initial) revision of a document.</summary>
		/// <remarks>
		/// Stores a new (or initial) revision of a document.
		/// This is what's invoked by a PUT or POST. As with those, the previous revision ID must be supplied when necessary and the call will fail if it doesn't match.
		/// </remarks>
		/// <param name="rev">The revision to add. If the docID is null, a new UUID will be assigned. Its revID must be null. It must have a JSON body.
		/// 	</param>
		/// <param name="prevRevId">The ID of the revision to replace (same as the "?rev=" parameter to a PUT), or null if this is a new document.
		/// 	</param>
		/// <param name="allowConflict">If false, an error status 409 will be returned if the insertion would create a conflict, i.e. if the previous revision already has a child.
		/// 	</param>
		/// <param name="resultStatus">On return, an HTTP status code indicating success or failure.
		/// 	</param>
		/// <returns>A new TDRevision with the docID, revID and sequence filled in (but no body).
		/// 	</returns>
		public virtual TDRevision PutRevision(TDRevision rev, string prevRevId, bool allowConflict
			, TDStatus resultStatus)
		{
			// prevRevId is the rev ID being replaced, or nil if an insert
			string docId = rev.GetDocId();
			bool deleted = rev.IsDeleted();
			if ((rev == null) || ((prevRevId != null) && (docId == null)) || (deleted && (docId
				 == null)) || ((docId != null) && !IsValidDocumentId(docId)))
			{
				resultStatus.SetCode(TDStatus.BAD_REQUEST);
				return null;
			}
			resultStatus.SetCode(TDStatus.INTERNAL_SERVER_ERROR);
			BeginTransaction();
			Cursor cursor = null;
			//// PART I: In which are performed lookups and validations prior to the insert...
			long docNumericID = (docId != null) ? GetDocNumericID(docId) : 0;
			long parentSequence = 0;
			try
			{
				if (prevRevId != null)
				{
					// Replacing: make sure given prevRevID is current & find its sequence number:
					if (docNumericID <= 0)
					{
						resultStatus.SetCode(TDStatus.NOT_FOUND);
						return null;
					}
					string[] args = new string[] { System.Convert.ToString(docNumericID), prevRevId };
					string additionalWhereClause = string.Empty;
					if (!allowConflict)
					{
						additionalWhereClause = "AND current=1";
					}
					cursor = database.RawQuery("SELECT sequence FROM revs WHERE doc_id=? AND revid=? "
						 + additionalWhereClause + " LIMIT 1", args);
					if (cursor.MoveToFirst())
					{
						parentSequence = cursor.GetLong(0);
					}
					if (parentSequence == 0)
					{
						// Not found: either a 404 or a 409, depending on whether there is any current revision
						if (!allowConflict && ExistsDocumentWithIDAndRev(docId, null))
						{
							resultStatus.SetCode(TDStatus.CONFLICT);
							return null;
						}
						else
						{
							resultStatus.SetCode(TDStatus.NOT_FOUND);
							return null;
						}
					}
					if (validations != null && validations.Count > 0)
					{
						// Fetch the previous revision and validate the new one against it:
						TDRevision prevRev = new TDRevision(docId, prevRevId, false);
						TDStatus status = ValidateRevision(rev, prevRev);
						if (!status.IsSuccessful())
						{
							resultStatus.SetCode(status.GetCode());
							return null;
						}
					}
					// Make replaced rev non-current:
					ContentValues updateContent = new ContentValues();
					updateContent.Put("current", 0);
					database.Update("revs", updateContent, "sequence=" + parentSequence, null);
				}
				else
				{
					// Inserting first revision.
					if (deleted && (docId != null))
					{
						// Didn't specify a revision to delete: 404 or a 409, depending
						if (ExistsDocumentWithIDAndRev(docId, null))
						{
							resultStatus.SetCode(TDStatus.CONFLICT);
							return null;
						}
						else
						{
							resultStatus.SetCode(TDStatus.NOT_FOUND);
							return null;
						}
					}
					// Validate:
					TDStatus status = ValidateRevision(rev, null);
					if (!status.IsSuccessful())
					{
						resultStatus.SetCode(status.GetCode());
						return null;
					}
					if (docId != null)
					{
						// Inserting first revision, with docID given (PUT):
						if (docNumericID <= 0)
						{
							// Doc doesn't exist at all; create it:
							docNumericID = InsertDocumentID(docId);
							if (docNumericID <= 0)
							{
								return null;
							}
						}
						else
						{
							// Doc exists; check whether current winning revision is deleted:
							string[] args = new string[] { System.Convert.ToString(docNumericID) };
							cursor = database.RawQuery("SELECT sequence, deleted FROM revs WHERE doc_id=? and current=1 ORDER BY revid DESC LIMIT 1"
								, args);
							if (cursor.MoveToFirst())
							{
								bool wasAlreadyDeleted = (cursor.GetInt(1) > 0);
								if (wasAlreadyDeleted)
								{
									// Make the deleted revision no longer current:
									ContentValues updateContent = new ContentValues();
									updateContent.Put("current", 0);
									database.Update("revs", updateContent, "sequence=" + cursor.GetLong(0), null);
								}
								else
								{
									if (!allowConflict)
									{
										// docId already exists, current not deleted, conflict
										resultStatus.SetCode(TDStatus.CONFLICT);
										return null;
									}
								}
							}
						}
					}
					else
					{
						// Inserting first revision, with no docID given (POST): generate a unique docID:
						docId = Couchbase.TouchDB.TDDatabase.GenerateDocumentId();
						docNumericID = InsertDocumentID(docId);
						if (docNumericID <= 0)
						{
							return null;
						}
					}
				}
				//// PART II: In which insertion occurs...
				// Bump the revID and update the JSON:
				string newRevId = GenerateNextRevisionID(prevRevId);
				byte[] data = null;
				if (!rev.IsDeleted())
				{
					data = EncodeDocumentJSON(rev);
					if (data == null)
					{
						// bad or missing json
						resultStatus.SetCode(TDStatus.BAD_REQUEST);
						return null;
					}
				}
				rev = rev.CopyWithDocID(docId, newRevId);
				// Now insert the rev itself:
				long newSequence = InsertRevision(rev, docNumericID, parentSequence, true, data);
				if (newSequence == 0)
				{
					return null;
				}
				// Store any attachments:
				if (attachments != null)
				{
					TDStatus status = ProcessAttachmentsForRevision(rev, parentSequence);
					if (!status.IsSuccessful())
					{
						resultStatus.SetCode(status.GetCode());
						return null;
					}
				}
				// Success!
				if (deleted)
				{
					resultStatus.SetCode(TDStatus.OK);
				}
				else
				{
					resultStatus.SetCode(TDStatus.CREATED);
				}
			}
			catch (SQLException e1)
			{
				Log.E(Couchbase.TouchDB.TDDatabase.TAG, "Error putting revision", e1);
				return null;
			}
			finally
			{
				if (cursor != null)
				{
					cursor.Close();
				}
				EndTransaction(resultStatus.IsSuccessful());
			}
			//// EPILOGUE: A change notification is sent...
			NotifyChange(rev, null);
			return rev;
		}

		/// <summary>Inserts an already-existing revision replicated from a remote database.</summary>
		/// <remarks>
		/// Inserts an already-existing revision replicated from a remote database.
		/// It must already have a revision ID. This may create a conflict! The revision's history must be given; ancestor revision IDs that don't already exist locally will create phantom revisions with no content.
		/// </remarks>
		public virtual TDStatus ForceInsert(TDRevision rev, IList<string> revHistory, Uri
			 source)
		{
			string docId = rev.GetDocId();
			string revId = rev.GetRevId();
			if (!IsValidDocumentId(docId) || (revId == null))
			{
				return new TDStatus(TDStatus.BAD_REQUEST);
			}
			int historyCount = revHistory.Count;
			if (historyCount == 0)
			{
				revHistory = new AList<string>();
				revHistory.AddItem(revId);
				historyCount = 1;
			}
			else
			{
				if (!revHistory[0].Equals(rev.GetRevId()))
				{
					return new TDStatus(TDStatus.BAD_REQUEST);
				}
			}
			bool success = false;
			BeginTransaction();
			try
			{
				// First look up all locally-known revisions of this document:
				long docNumericID = GetOrInsertDocNumericID(docId);
				TDRevisionList localRevs = GetAllRevisionsOfDocumentID(docId, docNumericID, false
					);
				if (localRevs == null)
				{
					return new TDStatus(TDStatus.INTERNAL_SERVER_ERROR);
				}
				// Walk through the remote history in chronological order, matching each revision ID to
				// a local revision. When the list diverges, start creating blank local revisions to fill
				// in the local history:
				long sequence = 0;
				long localParentSequence = 0;
				for (int i = revHistory.Count - 1; i >= 0; --i)
				{
					revId = revHistory[i];
					TDRevision localRev = localRevs.RevWithDocIdAndRevId(docId, revId);
					if (localRev != null)
					{
						// This revision is known locally. Remember its sequence as the parent of the next one:
						sequence = localRev.GetSequence();
						//asert(sequence > 0);
						localParentSequence = sequence;
					}
					else
					{
						// This revision isn't known, so add it:
						TDRevision newRev;
						byte[] data = null;
						bool current = false;
						if (i == 0)
						{
							// Hey, this is the leaf revision we're inserting:
							newRev = rev;
							if (!rev.IsDeleted())
							{
								data = EncodeDocumentJSON(rev);
								if (data == null)
								{
									return new TDStatus(TDStatus.BAD_REQUEST);
								}
							}
							current = true;
						}
						else
						{
							// It's an intermediate parent, so insert a stub:
							newRev = new TDRevision(docId, revId, false);
						}
						// Insert it:
						sequence = InsertRevision(newRev, docNumericID, sequence, current, data);
						if (sequence <= 0)
						{
							return new TDStatus(TDStatus.INTERNAL_SERVER_ERROR);
						}
						if (i == 0)
						{
							// Write any changed attachments for the new revision:
							TDStatus status = ProcessAttachmentsForRevision(rev, localParentSequence);
							if (!status.IsSuccessful())
							{
								return status;
							}
						}
					}
				}
				// Mark the latest local rev as no longer current:
				if (localParentSequence > 0 && localParentSequence != sequence)
				{
					ContentValues args = new ContentValues();
					args.Put("current", 0);
					string[] whereArgs = new string[] { System.Convert.ToString(localParentSequence) };
					try
					{
						database.Update("revs", args, "sequence=?", whereArgs);
					}
					catch (SQLException)
					{
						return new TDStatus(TDStatus.INTERNAL_SERVER_ERROR);
					}
				}
				success = true;
			}
			catch (SQLException)
			{
				EndTransaction(success);
				return new TDStatus(TDStatus.INTERNAL_SERVER_ERROR);
			}
			finally
			{
				EndTransaction(success);
			}
			// Notify and return:
			NotifyChange(rev, source);
			return new TDStatus(TDStatus.CREATED);
		}

		/// <summary>Define or clear a named document validation function.</summary>
		/// <remarks>Define or clear a named document validation function.</remarks>
		public virtual void DefineValidation(string name, TDValidationBlock validationBlock
			)
		{
			if (validations == null)
			{
				validations = new Dictionary<string, TDValidationBlock>();
			}
			validations.Put(name, validationBlock);
		}

		public virtual TDValidationBlock GetValidationNamed(string name)
		{
			TDValidationBlock result = null;
			if (validations != null)
			{
				result = validations.Get(name);
			}
			return result;
		}

		public virtual TDStatus ValidateRevision(TDRevision newRev, TDRevision oldRev)
		{
			TDStatus result = new TDStatus(TDStatus.OK);
			if (validations == null || validations.Count == 0)
			{
				return result;
			}
			TDValidationContextImpl context = new TDValidationContextImpl(this, oldRev);
			foreach (string validationName in validations.Keys)
			{
				TDValidationBlock validation = GetValidationNamed(validationName);
				if (!validation.Validate(newRev, context))
				{
					result.SetCode(context.GetErrorType().GetCode());
					break;
				}
			}
			return result;
		}

		public virtual IList<TDReplicator> GetActiveReplicators()
		{
			//TODO implement missing replication methods
			return activeReplicators;
		}

		public virtual TDReplicator GetActiveReplicator(Uri remote, bool push)
		{
			if (activeReplicators != null)
			{
				foreach (TDReplicator replicator in activeReplicators)
				{
					if (replicator.GetRemote().Equals(remote) && replicator.IsPush() == push && replicator
						.IsRunning())
					{
						return replicator;
					}
				}
			}
			return null;
		}

		public virtual TDReplicator GetReplicator(Uri remote, bool push, bool continuous)
		{
			TDReplicator replicator = GetReplicator(remote, null, push, continuous);
			return replicator;
		}

		public virtual TDReplicator GetReplicator(Uri remote, HttpClientFactory httpClientFactory
			, bool push, bool continuous)
		{
			TDReplicator result = GetActiveReplicator(remote, push);
			if (result != null)
			{
				return result;
			}
			result = push ? new TDPusher(this, remote, continuous, httpClientFactory) : new TDPuller
				(this, remote, continuous, httpClientFactory);
			if (activeReplicators == null)
			{
				activeReplicators = new AList<TDReplicator>();
			}
			activeReplicators.AddItem(result);
			return result;
		}

		public virtual string LastSequenceWithRemoteURL(Uri url, bool push)
		{
			Cursor cursor = null;
			string result = null;
			try
			{
				string[] args = new string[] { url.ToExternalForm(), Sharpen.Extensions.ToString(
					push ? 1 : 0) };
				cursor = database.RawQuery("SELECT last_sequence FROM replicators WHERE remote=? AND push=?"
					, args);
				if (cursor.MoveToFirst())
				{
					result = cursor.GetString(0);
				}
			}
			catch (SQLException e)
			{
				Log.E(Couchbase.TouchDB.TDDatabase.TAG, "Error getting last sequence", e);
				return null;
			}
			finally
			{
				if (cursor != null)
				{
					cursor.Close();
				}
			}
			return result;
		}

		public virtual bool SetLastSequence(string lastSequence, Uri url, bool push)
		{
			ContentValues values = new ContentValues();
			values.Put("remote", url.ToExternalForm());
			values.Put("push", push);
			values.Put("last_sequence", lastSequence);
			long newId = database.InsertWithOnConflict("replicators", null, values, SQLiteDatabase
				.CONFLICT_REPLACE);
			return (newId == -1);
		}

		public static string Quote(string @string)
		{
			return @string.Replace("'", "''");
		}

		public static string JoinQuoted(IList<string> strings)
		{
			if (strings.Count == 0)
			{
				return string.Empty;
			}
			string result = "'";
			bool first = true;
			foreach (string @string in strings)
			{
				if (first)
				{
					first = false;
				}
				else
				{
					result = result + "','";
				}
				result = result + Quote(@string);
			}
			result = result + "'";
			return result;
		}

		public virtual bool FindMissingRevisions(TDRevisionList touchRevs)
		{
			if (touchRevs.Count == 0)
			{
				return true;
			}
			string quotedDocIds = JoinQuoted(touchRevs.GetAllDocIds());
			string quotedRevIds = JoinQuoted(touchRevs.GetAllRevIds());
			string sql = "SELECT docid, revid FROM revs, docs " + "WHERE docid IN (" + quotedDocIds
				 + ") AND revid in (" + quotedRevIds + ")" + " AND revs.doc_id == docs.doc_id";
			Cursor cursor = null;
			try
			{
				cursor = database.RawQuery(sql, null);
				cursor.MoveToFirst();
				while (!cursor.IsAfterLast())
				{
					TDRevision rev = touchRevs.RevWithDocIdAndRevId(cursor.GetString(0), cursor.GetString
						(1));
					if (rev != null)
					{
						touchRevs.Remove(rev);
					}
					cursor.MoveToNext();
				}
			}
			catch (SQLException e)
			{
				Log.E(Couchbase.TouchDB.TDDatabase.TAG, "Error finding missing revisions", e);
				return false;
			}
			finally
			{
				if (cursor != null)
				{
					cursor.Close();
				}
			}
			return true;
		}

		public virtual TDRevision GetLocalDocument(string docID, string revID)
		{
			TDRevision result = null;
			Cursor cursor = null;
			try
			{
				string[] args = new string[] { docID };
				cursor = database.RawQuery("SELECT revid, json FROM localdocs WHERE docid=?", args
					);
				if (cursor.MoveToFirst())
				{
					string gotRevID = cursor.GetString(0);
					if (revID != null && (!revID.Equals(gotRevID)))
					{
						return null;
					}
					byte[] json = cursor.GetBlob(1);
					IDictionary<string, object> properties = null;
					try
					{
						properties = TDServer.GetObjectMapper().ReadValue<IDictionary>(json);
						properties.Put("_id", docID);
						properties.Put("_rev", gotRevID);
						result = new TDRevision(docID, gotRevID, false);
						result.SetProperties(properties);
					}
					catch (Exception e)
					{
						Log.W(TAG, "Error parsing local doc JSON", e);
						return null;
					}
				}
				return result;
			}
			catch (SQLException e)
			{
				Log.E(Couchbase.TouchDB.TDDatabase.TAG, "Error getting local document", e);
				return null;
			}
			finally
			{
				if (cursor != null)
				{
					cursor.Close();
				}
			}
		}

		public virtual TDRevision PutLocalRevision(TDRevision revision, string prevRevID, 
			TDStatus status)
		{
			string docID = revision.GetDocId();
			if (!docID.StartsWith("_local/"))
			{
				status.SetCode(TDStatus.BAD_REQUEST);
				return null;
			}
			if (!revision.IsDeleted())
			{
				// PUT:
				byte[] json = EncodeDocumentJSON(revision);
				string newRevID;
				if (prevRevID != null)
				{
					int generation = TDRevision.GenerationFromRevID(prevRevID);
					if (generation == 0)
					{
						status.SetCode(TDStatus.BAD_REQUEST);
						return null;
					}
					newRevID = Sharpen.Extensions.ToString(++generation) + "-local";
					ContentValues values = new ContentValues();
					values.Put("revid", newRevID);
					values.Put("json", json);
					string[] whereArgs = new string[] { docID, prevRevID };
					try
					{
						int rowsUpdated = database.Update("localdocs", values, "docid=? AND revid=?", whereArgs
							);
						if (rowsUpdated == 0)
						{
							status.SetCode(TDStatus.CONFLICT);
							return null;
						}
					}
					catch (SQLException)
					{
						status.SetCode(TDStatus.INTERNAL_SERVER_ERROR);
						return null;
					}
				}
				else
				{
					newRevID = "1-local";
					ContentValues values = new ContentValues();
					values.Put("docid", docID);
					values.Put("revid", newRevID);
					values.Put("json", json);
					try
					{
						database.InsertWithOnConflict("localdocs", null, values, SQLiteDatabase.CONFLICT_IGNORE
							);
					}
					catch (SQLException)
					{
						status.SetCode(TDStatus.INTERNAL_SERVER_ERROR);
						return null;
					}
				}
				status.SetCode(TDStatus.CREATED);
				return revision.CopyWithDocID(docID, newRevID);
			}
			else
			{
				// DELETE:
				TDStatus deleteStatus = DeleteLocalDocument(docID, prevRevID);
				status.SetCode(deleteStatus.GetCode());
				return (status.IsSuccessful()) ? revision : null;
			}
		}

		public virtual TDStatus DeleteLocalDocument(string docID, string revID)
		{
			if (docID == null)
			{
				return new TDStatus(TDStatus.BAD_REQUEST);
			}
			if (revID == null)
			{
				// Didn't specify a revision to delete: 404 or a 409, depending
				return (GetLocalDocument(docID, null) != null) ? new TDStatus(TDStatus.CONFLICT) : 
					new TDStatus(TDStatus.NOT_FOUND);
			}
			string[] whereArgs = new string[] { docID, revID };
			try
			{
				int rowsDeleted = database.Delete("localdocs", "docid=? AND revid=?", whereArgs);
				if (rowsDeleted == 0)
				{
					return (GetLocalDocument(docID, null) != null) ? new TDStatus(TDStatus.CONFLICT) : 
						new TDStatus(TDStatus.NOT_FOUND);
				}
				return new TDStatus(TDStatus.OK);
			}
			catch (SQLException)
			{
				return new TDStatus(TDStatus.INTERNAL_SERVER_ERROR);
			}
		}

		public virtual Handler GetHandler()
		{
			return handler;
		}
	}

	internal class TDValidationContextImpl : TDValidationContext
	{
		private TDDatabase database;

		private TDRevision currentRevision;

		private TDStatus errorType;

		private string errorMessage;

		public TDValidationContextImpl(TDDatabase database, TDRevision currentRevision)
		{
			this.database = database;
			this.currentRevision = currentRevision;
			this.errorType = new TDStatus(TDStatus.FORBIDDEN);
			this.errorMessage = "invalid document";
		}

		public virtual TDRevision GetCurrentRevision()
		{
			if (currentRevision != null)
			{
				database.LoadRevisionBody(currentRevision, EnumSet.NoneOf<TDDatabase.TDContentOptions
					>());
			}
			return currentRevision;
		}

		public virtual TDStatus GetErrorType()
		{
			return errorType;
		}

		public virtual void SetErrorType(TDStatus status)
		{
			this.errorType = status;
		}

		public virtual string GetErrorMessage()
		{
			return errorMessage;
		}

		public virtual void SetErrorMessage(string message)
		{
			this.errorMessage = message;
		}
	}
}
