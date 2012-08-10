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
using System.IO;
using Android.Util;
using Couchbase.TouchDB;
using Sharpen;

namespace Couchbase.TouchDB
{
	/// <summary>A persistent content-addressable store for arbitrary-size data blobs.</summary>
	/// <remarks>
	/// A persistent content-addressable store for arbitrary-size data blobs.
	/// Each blob is stored as a file named by its SHA-1 digest.
	/// </remarks>
	public class TDBlobStore
	{
		public static string FILE_EXTENSION = ".blob";

		public static string TMP_FILE_EXTENSION = ".blobtmp";

		public static string TMP_FILE_PREFIX = "tmp";

		private string path;

		public TDBlobStore(string path)
		{
			this.path = path;
			FilePath directory = new FilePath(path);
			if (!directory.Exists())
			{
				bool result = directory.Mkdirs();
				if (result == false)
				{
					throw new ArgumentException("Unable to create directory for blob store");
				}
			}
			else
			{
				if (!directory.IsDirectory())
				{
					throw new ArgumentException("Directory for blob store is not a directory");
				}
			}
		}

		public static TDBlobKey KeyForBlob(byte[] data)
		{
			MessageDigest md;
			try
			{
				md = MessageDigest.GetInstance("SHA-1");
			}
			catch (NoSuchAlgorithmException)
			{
				Log.E(TDDatabase.TAG, "Error, SHA-1 digest is unavailable.");
				return null;
			}
			byte[] sha1hash = new byte[40];
			md.Update(data, 0, data.Length);
			sha1hash = md.Digest();
			TDBlobKey result = new TDBlobKey(sha1hash);
			return result;
		}

		public static TDBlobKey KeyForBlobFromFile(FilePath file)
		{
			MessageDigest md;
			try
			{
				md = MessageDigest.GetInstance("SHA-1");
			}
			catch (NoSuchAlgorithmException)
			{
				Log.E(TDDatabase.TAG, "Error, SHA-1 digest is unavailable.");
				return null;
			}
			byte[] sha1hash = new byte[40];
			try
			{
				FileInputStream fis = new FileInputStream(file);
				byte[] buffer = new byte[65536];
				int lenRead = fis.Read(buffer);
				while (lenRead > 0)
				{
					md.Update(buffer, 0, lenRead);
					lenRead = fis.Read(buffer);
				}
				fis.Close();
			}
			catch (IOException)
			{
				Log.E(TDDatabase.TAG, "Error readin tmp file to compute key");
			}
			sha1hash = md.Digest();
			TDBlobKey result = new TDBlobKey(sha1hash);
			return result;
		}

		public virtual string PathForKey(TDBlobKey key)
		{
			return path + FilePath.separator + TDBlobKey.ConvertToHex(key.GetBytes()) + FILE_EXTENSION;
		}

		public virtual long GetSizeOfBlob(TDBlobKey key)
		{
			string path = PathForKey(key);
			FilePath file = new FilePath(path);
			return file.Length();
		}

		public virtual bool GetKeyForFilename(TDBlobKey outKey, string filename)
		{
			if (!filename.EndsWith(FILE_EXTENSION))
			{
				return false;
			}
			//trim off extension
			string rest = Sharpen.Runtime.Substring(filename, path.Length + 1, filename.Length
				 - FILE_EXTENSION.Length);
			outKey.SetBytes(TDBlobKey.ConvertFromHex(rest));
			return true;
		}

		public virtual byte[] BlobForKey(TDBlobKey key)
		{
			string path = PathForKey(key);
			FilePath file = new FilePath(path);
			byte[] result = null;
			try
			{
				result = GetBytesFromFile(file);
			}
			catch (IOException e)
			{
				Log.E(TDDatabase.TAG, "Error reading file", e);
			}
			return result;
		}

		public virtual InputStream BlobStreamForKey(TDBlobKey key)
		{
			string path = PathForKey(key);
			FilePath file = new FilePath(path);
			if (file.CanRead())
			{
				try
				{
					return new FileInputStream(file);
				}
				catch (FileNotFoundException e)
				{
					Log.E(TDDatabase.TAG, "Unexpected file not found in blob store", e);
					return null;
				}
			}
			return null;
		}

		public virtual bool StoreBlobStream(InputStream inputStream, TDBlobKey outKey)
		{
			FilePath tmp = null;
			try
			{
				tmp = FilePath.CreateTempFile(TMP_FILE_PREFIX, TMP_FILE_EXTENSION, new FilePath(path
					));
				FileOutputStream fos = new FileOutputStream(tmp);
				byte[] buffer = new byte[65536];
				int lenRead = inputStream.Read(buffer);
				while (lenRead > 0)
				{
					fos.Write(buffer, 0, lenRead);
					lenRead = inputStream.Read(buffer);
				}
				inputStream.Close();
				fos.Close();
			}
			catch (IOException e)
			{
				Log.E(TDDatabase.TAG, "Error writing blog to tmp file", e);
				return false;
			}
			TDBlobKey newKey = KeyForBlobFromFile(tmp);
			outKey.SetBytes(newKey.GetBytes());
			string path = PathForKey(outKey);
			FilePath file = new FilePath(path);
			if (file.CanRead())
			{
				// object with this hash already exists, we should delete tmp file and return true
				tmp.Delete();
				return true;
			}
			else
			{
				// does not exist, we should rename tmp file to this name
				tmp.RenameTo(file);
			}
			return true;
		}

		public virtual bool StoreBlob(byte[] data, TDBlobKey outKey)
		{
			TDBlobKey newKey = KeyForBlob(data);
			outKey.SetBytes(newKey.GetBytes());
			string path = PathForKey(outKey);
			FilePath file = new FilePath(path);
			if (file.CanRead())
			{
				return true;
			}
			FileOutputStream fos = null;
			try
			{
				fos = new FileOutputStream(file);
				fos.Write(data);
			}
			catch (FileNotFoundException e)
			{
				Log.E(TDDatabase.TAG, "Error opening file for output", e);
				return false;
			}
			catch (IOException ioe)
			{
				Log.E(TDDatabase.TAG, "Error writing to file", ioe);
				return false;
			}
			finally
			{
				if (fos != null)
				{
					try
					{
						fos.Close();
					}
					catch (IOException)
					{
					}
				}
			}
			// ignore
			return true;
		}

		/// <exception cref="System.IO.IOException"></exception>
		private static byte[] GetBytesFromFile(FilePath file)
		{
			InputStream @is = new FileInputStream(file);
			// Get the size of the file
			long length = file.Length();
			// Create the byte array to hold the data
			byte[] bytes = new byte[(int)length];
			// Read in the bytes
			int offset = 0;
			int numRead = 0;
			while (offset < bytes.Length && (numRead = @is.Read(bytes, offset, bytes.Length -
				 offset)) >= 0)
			{
				offset += numRead;
			}
			// Ensure all the bytes have been read in
			if (offset < bytes.Length)
			{
				throw new IOException("Could not completely read file " + file.GetName());
			}
			// Close the input stream and return bytes
			@is.Close();
			return bytes;
		}

		public virtual ICollection<TDBlobKey> AllKeys()
		{
			ICollection<TDBlobKey> result = new HashSet<TDBlobKey>();
			FilePath file = new FilePath(path);
			FilePath[] contents = file.ListFiles();
			foreach (FilePath attachment in contents)
			{
				TDBlobKey attachmentKey = new TDBlobKey();
				GetKeyForFilename(attachmentKey, attachment.GetPath());
				result.AddItem(attachmentKey);
			}
			return result;
		}

		public virtual int Count()
		{
			FilePath file = new FilePath(path);
			FilePath[] contents = file.ListFiles();
			return contents.Length;
		}

		public virtual long TotalDataSize()
		{
			long total = 0;
			FilePath file = new FilePath(path);
			FilePath[] contents = file.ListFiles();
			foreach (FilePath attachment in contents)
			{
				total += attachment.Length();
			}
			return total;
		}

		public virtual int DeleteBlobsExceptWithKeys(IList<TDBlobKey> keysToKeep)
		{
			int numDeleted = 0;
			FilePath file = new FilePath(path);
			FilePath[] contents = file.ListFiles();
			foreach (FilePath attachment in contents)
			{
				TDBlobKey attachmentKey = new TDBlobKey();
				GetKeyForFilename(attachmentKey, attachment.GetPath());
				if (!keysToKeep.Contains(attachmentKey))
				{
					bool result = attachment.Delete();
					if (result)
					{
						++numDeleted;
					}
					else
					{
						Log.E(TDDatabase.TAG, "Error deleting attachmetn");
					}
				}
			}
			return numDeleted;
		}
	}
}
