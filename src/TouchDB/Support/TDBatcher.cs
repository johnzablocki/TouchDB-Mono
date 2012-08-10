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
using Sharpen;

namespace Couchbase.TouchDB.Support
{
	/// <summary>
	/// Utility that queues up objects until the queue fills up or a time interval elapses,
	/// then passes all the objects at once to a client-supplied processor block.
	/// </summary>
	/// <remarks>
	/// Utility that queues up objects until the queue fills up or a time interval elapses,
	/// then passes all the objects at once to a client-supplied processor block.
	/// </remarks>
	public class TDBatcher<T>
	{
		private Handler handler;

		private int capacity;

		private int delay;

		private IList<T> inbox;

		private TDBatchProcessor<T> processor;

		private sealed class _Runnable_23 : Runnable
		{
			public _Runnable_23(TDBatcher<T> _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public void Run()
			{
				try
				{
					this._enclosing.ProcessNow();
				}
				catch (Exception e)
				{
					// we don't want this to crash the batcher
					Log.E(TDDatabase.TAG, "TDBatchProcessor throw exception", e);
				}
			}

			private readonly TDBatcher<T> _enclosing;
		}

		private Runnable processNowRunnable;

		public TDBatcher(Handler handler, int capacity, int delay, TDBatchProcessor<T> processor
			)
		{
			processNowRunnable = new _Runnable_23(this);
			this.handler = handler;
			this.capacity = capacity;
			this.delay = delay;
			this.processor = processor;
		}

		public virtual void ProcessNow()
		{
			IList<T> toProcess = null;
			lock (this)
			{
				if (inbox == null || inbox.Count == 0)
				{
					return;
				}
				toProcess = inbox;
				inbox = null;
			}
			if (toProcess != null)
			{
				processor.Process(toProcess);
			}
		}

		public virtual void QueueObject(T @object)
		{
			lock (this)
			{
				if (inbox != null && inbox.Count >= capacity)
				{
					Flush();
				}
				if (inbox == null)
				{
					inbox = new AList<T>();
					if (handler != null)
					{
						handler.PostDelayed(processNowRunnable, delay);
					}
				}
				inbox.AddItem(@object);
			}
		}

		public virtual void Flush()
		{
			lock (this)
			{
				if (inbox != null)
				{
					handler.RemoveCallbacks(processNowRunnable);
					ProcessNow();
				}
			}
		}

		public virtual int Count()
		{
			lock (this)
			{
				if (inbox == null)
				{
					return 0;
				}
				return inbox.Count;
			}
		}

		public virtual void Close()
		{
		}
	}
}
