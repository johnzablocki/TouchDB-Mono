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
using System.IO;
using Couchbase.TouchDB;
using Couchbase.TouchDB.Support;
using Org.Codehaus.Jackson.Map;
using Sharpen;

namespace Couchbase.TouchDB
{
	/// <summary>Manages a directory containing TDDatabases.</summary>
	/// <remarks>Manages a directory containing TDDatabases.</remarks>
	public class TDServer
	{
		private static readonly ObjectMapper mapper = new ObjectMapper();

		public static readonly string LEGAL_CHARACTERS = "abcdefghijklmnopqrstuvwxyz0123456789_$()+-/";

		public static readonly string DATABASE_SUFFIX = ".touchdb";

		private FilePath directory;

		private IDictionary<string, TDDatabase> databases;

		private HttpClientFactory defaultHttpClientFactory;

		public static ObjectMapper GetObjectMapper()
		{
			return mapper;
		}

		/// <exception cref="System.IO.IOException"></exception>
		public TDServer(string directoryName)
		{
			this.directory = new FilePath(directoryName);
			this.databases = new Dictionary<string, TDDatabase>();
			//create the directory, but don't fail if it already exists
			if (!directory.Exists())
			{
				bool result = directory.Mkdir();
				if (!result)
				{
					throw new IOException("Unable to create directory " + directory);
				}
			}
		}

		private string PathForName(string name)
		{
			if ((name == null) || (name.Length == 0) || Sharpen.Pattern.Matches("^" + LEGAL_CHARACTERS
				, name) || !System.Char.IsLower(name[0]))
			{
				return null;
			}
			name = name.Replace('/', ':');
			string result = directory.GetPath() + FilePath.separator + name + DATABASE_SUFFIX;
			return result;
		}

		public virtual TDDatabase GetDatabaseNamed(string name, bool create)
		{
			TDDatabase db = databases.Get(name);
			if (db == null)
			{
				string path = PathForName(name);
				if (path == null)
				{
					return null;
				}
				db = new TDDatabase(path);
				if (!create && !db.Exists())
				{
					return null;
				}
				db.SetName(name);
				databases.Put(name, db);
			}
			return db;
		}

		public virtual TDDatabase GetDatabaseNamed(string name)
		{
			return GetDatabaseNamed(name, true);
		}

		public virtual TDDatabase GetExistingDatabaseNamed(string name)
		{
			TDDatabase db = GetDatabaseNamed(name, false);
			if ((db != null) && !db.Open())
			{
				return null;
			}
			return db;
		}

		public virtual bool DeleteDatabaseNamed(string name)
		{
			TDDatabase db = databases.Get(name);
			if (db == null)
			{
				return false;
			}
			db.DeleteDatabase();
			Sharpen.Collections.Remove(databases, name);
			return true;
		}

		public virtual IList<string> AllDatabaseNames()
		{
			string[] databaseFiles = directory.List(new _FilenameFilter_116());
			IList<string> result = new AList<string>();
			foreach (string databaseFile in databaseFiles)
			{
				string trimmed = Sharpen.Runtime.Substring(databaseFile, 0, databaseFile.Length -
					 DATABASE_SUFFIX.Length);
				string replaced = trimmed.Replace(':', '/');
				result.AddItem(replaced);
			}
			result.Sort();
			return result;
		}

		private sealed class _FilenameFilter_116 : FilenameFilter
		{
			public _FilenameFilter_116()
			{
			}

			public bool Accept(FilePath dir, string filename)
			{
				if (filename.EndsWith(Couchbase.TouchDB.TDServer.DATABASE_SUFFIX))
				{
					return true;
				}
				return false;
			}
		}

		public virtual ICollection<TDDatabase> AllOpenDatabases()
		{
			return databases.Values;
		}

		public virtual void Close()
		{
			foreach (TDDatabase database in databases.Values)
			{
				database.Close();
			}
			databases.Clear();
		}

		public virtual HttpClientFactory GetDefaultHttpClientFactory()
		{
			return defaultHttpClientFactory;
		}

		public virtual void SetDefaultHttpClientFactory(HttpClientFactory defaultHttpClientFactory
			)
		{
			this.defaultHttpClientFactory = defaultHttpClientFactory;
		}
	}
}
