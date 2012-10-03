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
using System.Security.Cryptography;

namespace TouchDB.Mono
{
    public class TDBlobStore
    {
        public const string FILE_EXTENSION = ".blob";
        public const string TMP_FILE_EXTENSION = ".blobtmp";
        public const string TMP_FILE_PREFIX = "tmp";

        public TDBlobStore(string path)
        {
            Path = path;

            var dirInfo = new DirectoryInfo(path);
            if (!dirInfo.Exists)
            {
                dirInfo.Create();
            }

            //TODO: check if path is directory
        }

        public static TDBlobKey GetKeyForBlob(byte[] bytes)
        {
            //TODO: check for failure to create SHA-1?
            var ha = new SHA1Managed();
            var sha1Hash = new byte[40];
            var hashed = ha.ComputeHash(sha1Hash);
            var result = new TDBlobKey(hashed);
            return result;
        }

        public static TDBlobKey GetKeyForBlobFromFile(FileInfo file)
        {
            //TODO: check for failure to create SHA-1?
            var ha = new SHA1Managed();
            var sha1Hash = new byte[40];
            var buffer = new byte[65536];            

            using (var fileStream = new FileStream(file.FullName, FileMode.Open))
            {
                var lenRead = fileStream.Read(buffer, 0, 8);
                while (lenRead > 0)
                {
                    lenRead = fileStream.Read(buffer, lenRead, 8);                    
                }
            }

            //TODO: Revisit Java approach of updating digest on read
            sha1Hash = ha.ComputeHash(buffer);
            var result = new TDBlobKey(sha1Hash);
            return result;

        }

        public string GetPathForKey(TDBlobKey key)
        {
            return Path + System.IO.Path.PathSeparator + TDBlobKey.ConvertToHex(key.Bytes) + FILE_EXTENSION;
        }

        public long GetSizeOfBlob(TDBlobKey key)
        {
            var path = GetPathForKey(key);
            var fileInfo = new FileInfo(path);
            return fileInfo.OpenText().ReadToEnd().Length; //TODO: make efficient            
        }

        public bool GetKeyForFileName(TDBlobKey outKey, string fileName)
        {
            if (!fileName.EndsWith(FILE_EXTENSION))
                return false;

            var rest = System.IO.Path.GetFileNameWithoutExtension(fileName);
            outKey.Bytes = TDBlobKey.ConvertFromHex(rest);
            return true;
        }

        public byte[] GetBlobForKey(TDBlobKey key)
        {
            var path = GetPathForKey(key);
            var fileInfo = new FileInfo(path);
            byte[] result = null;
            result = GetBytesFromFile(fileInfo);

            return result;
        }

        public Stream GetBlobStreamForKey(TDBlobKey key)
        {
            var path = GetPathForKey(key);
            var fileInfo = new FileInfo(path);

            var fs = new FileStream(fileInfo.FullName, FileMode.Open);

            if (fs.CanRead)
            {
                return fs;
            }

            return null;
            
        }

        public bool StoreBlobStream(Stream stream, TDBlobKey outKey)
        {
            FileStream fs = null;
            try
            {
                var tempFileName = System.IO.Path.Combine(Path, TMP_FILE_PREFIX);
                tempFileName = System.IO.Path.Combine(tempFileName, TMP_FILE_EXTENSION); //would be nice to have this in SL4 profile
                fs = File.Create(tempFileName);
                var buffer = new byte[65536];

                var lenRead = stream.Read(buffer, 0, 8);
                using (var sr = new StreamReader(stream))
                {
                    
                }
            }
            catch (Exception)
            {
                
                throw;
            }

            throw new NotImplementedException();
        }

        public bool StoreBlob(byte[] data, TDBlobKey outKey)
        {
            var newKey = GetKeyForBlob(data);
            outKey.Bytes = newKey.Bytes;
            var path = GetPathForKey(outKey);
            var fileStream = new FileStream(path, FileMode.Open);

            if (fileStream.CanRead)
                return true;

            using (var sw = new StreamWriter(fileStream))
            {
                
            }

            throw new NotImplementedException();


        }

        public static byte[] GetBytesFromFile(FileInfo file)
        {
            throw new NotImplementedException();
        }

        public IList<TDBlobKey> AllKeys
        {
            get
            {
                var files = Directory.GetFiles(Path);
                var result = new List<TDBlobKey>();
                foreach (var file in files)
                {
                    var attachmentKey = new TDBlobKey();
                    GetKeyForFileName(attachmentKey, file);
                    result.Add(attachmentKey);
                }

                return result;
            }
        }

        public int Count
        {
            get { return Directory.GetFiles(Path).Length; }
        }

        public long TotalDataSize 
        { 
            get
            {
                var files = new DirectoryInfo(Path).GetFiles();
                var total = 0L;
                foreach (var fileInfo in files)
                {
                    //TODO: find a better way to get size
                    total += fileInfo.OpenRead().Length;
                }

                return total;
            }
        }

        public int DeleteBlobExceptWithKeys(IList<TDBlobKey> keysToKeep)
        {
            var numDeleted = 0;
            var files = Directory.GetFiles(Path);

            foreach (var file in files)
            {
                var attachmentKey = new TDBlobKey();
                GetKeyForFileName(attachmentKey, file);
                if (!keysToKeep.Contains(attachmentKey))
                {
                    File.Delete(file);
                    ++numDeleted;
                }
            }
            return numDeleted;
        }

        public string Path { get; set; }
    }
}
