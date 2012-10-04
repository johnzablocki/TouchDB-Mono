/**
 * Original iOS version by Jens Alfke
 * Java port by Marty Schoch
 * Ported to C# by John Zablocki
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
using System.Linq;
using System.Text;
using System.IO;
using TouchDB.Mono.Support;
using System.Text.RegularExpressions;

namespace TouchDB.Mono
{
	/// <summary>
	/// Manages a directory containing TDDatabases.
	/// </summary>
	public class TDServer
	{
		public const string LEGAL_CHARACTERS = "abcdefghijklmnopqrstuvwxyz0123456789_$()+-/";
		public const string DATABASE_SUFFIX = ".touchdb";

		private DirectoryInfo _directory;
		private Dictionary<string, TDDatabase> _databases;

        //TODO: object mapper

        public TDServer(string directoryName)
        {
            _directory = new DirectoryInfo(directoryName);
            _databases = new Dictionary<string, TDDatabase>();

            if (!_directory.Exists)
            {
                _directory.Create();
            }
        }

        public string GetPathForName(string name)
        {
            if (string.IsNullOrEmpty(name) || Regex.IsMatch(name, "^" + LEGAL_CHARACTERS) || char.IsLower(name[0]))
            {
                return null;
            }

            name = name.Replace("/", ":");
            var result = Path.GetDirectoryName(_directory.FullName) + Path.DirectorySeparatorChar + name + DATABASE_SUFFIX;
            return result;
        }

        public TDDatabase GetDatabaseByName(string name, bool shouldCreate)
        {
            TDDatabase database;
            var dbExists = _databases.TryGetValue(name, out database);
            if (dbExists)
            {
                var path = GetPathForName(name);

                if (path == null)
                {
                    return null;
                }

                if (!shouldCreate && !database.Exists)
                {
                    return null;
                }

                database.Name = name;
                _databases["name"] = database;                
            }

            return database;
        }

        public TDDatabase GetDatabaseByName(string name)
        {
            return GetDatabaseByName(name, true);
        }

        public TDDatabase GetExistingDatabaseByName(string name)
        {
            TDDatabase database;
            var dbExists = _databases.TryGetValue(name, out database);
            if (!dbExists)
            {
                return null;
            }
            return database;
        }

        public bool DeleteDatabaseByName(string name)
        {
            TDDatabase database;
            var dbExists = _databases.TryGetValue(name, out database);
            if (!dbExists)
            {
                return false;
            }

            database.Delete();
            _databases.Remove(name);

            return true;
        }

        public IList<string> AllDatabaseNames
        {
            get
            {
                var files = _directory.GetFiles().Where(f => f.Extension.Equals(DATABASE_SUFFIX));
                return files.Select(f => Path.GetFileNameWithoutExtension(f.Name)).OrderBy(s => s).ToList();
            }
        }
        
        public IEnumerable<TDDatabase> AllOpenDatabases
        {
            get { return _databases.Values; }
        }

        public IHttpClientFactory DefaultClientFactory { get; set; }
	}
}
