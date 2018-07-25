#if NET_4_6
using System;
using System.IO;
using System.Threading.Tasks;
using BeatThat.AsyncAwaitUntil;
using BeatThat.Bindings;
using BeatThat.Pools;
using BeatThat.Serializers;
using BeatThat.DependencyInjection;
using UnityEngine;
using BeatThat.Requests;
using System.Collections.Generic;

namespace BeatThat.Entities.Persistence
{
    public class FilePersistenceService<DataType> : FilePersistenceService<DataType, DataType>
    {
        protected override bool Data2Serial(DataType data, ref DataType result, out string error)
        {
            error = null;
            result = data;
            return true;
        }

        protected override bool Serial2Data(DataType ser, ref DataType result, out string error)
        {
            error = null;
            result = ser;
            return true;
        }
    }

    public abstract class FilePersistenceService<DataType, SerializedType> : BindingService
	{
        [Inject] HasEntities<DataType> entities;

		override protected void BindAll()
		{
            BindIfReady();
		}

        /// <summary>
        /// Will load stored entities and bind a listen to store updates.
        /// Normally this happens when the service binds.
        /// Override if you want to delay init for whatever reason.
        /// </summary>
        /// <returns><c>true</c>, if if ready was bound, <c>false</c> otherwise.</returns>
        virtual protected async Task<bool> BindIfReady()
        {
            await LoadAndStoreUpdates(EntityDirectory());
            return true;
        }

        /// <summary>
        /// default entity directory will be 
        /// {Application.temporaryCachePath}/beatthat/entities/{DataType.FullName}.
        /// 
        /// Override here to change from default.
        /// </summary>
        /// <returns>The directory.</returns>
        virtual protected DirectoryInfo EntityDirectory()
        {
            return EntityDirectoryDefault();
        }

        protected DirectoryInfo EntityDirectoryDefault(params string[] additionalPathParts)
        {
            var nAdditional = (additionalPathParts != null) ? additionalPathParts.Length : 0;

            using (var pathParts = ArrayPool<string>.Get(4 + nAdditional)) 
            {
                pathParts.array[0] = Application.temporaryCachePath;
                pathParts.array[1] = "beatthat";
                pathParts.array[2] = "entities";
                pathParts.array[3] = typeof(DataType).FullName;
                if(nAdditional > 0) {
                    Array.Copy(additionalPathParts, 0, pathParts.array, 4, additionalPathParts.Length);
                }
                return new DirectoryInfo(Path.Combine(pathParts.array).ToLower());
            }
        }

        //class DataReader : ReaderBase<DataType>
        //{
        //    public override DataType[] ReadArray(Stream s)
        //    {
        //        throw new NotImplementedException();
        //    }

        //    public override DataType ReadOne(Stream s)
        //    {
        //        var dto = this.re
        //    }

        //    public override DataType ReadOne(Stream s, ref DataType toObject)
        //    {
        //        throw new NotImplementedException();
        //    }
        //}

        virtual protected async Task LoadAndStoreUpdates(DirectoryInfo d)
        {
            this.directory = d;

#if UNITY_EDITOR || DEBUG_UNSTRIP
            Debug.Log("persistence for " + typeof(DataType).Name + " at " + this.directory.FullName);
#endif

            await LoadStored();
            Bind<string>(Entity<DataType>.UPDATED, this.OnEntityUpdated);
            Bind<string>(Entity<DataType>.DID_REMOVE, this.OnEntityRemoved);
        }

        virtual protected void OnLoadedInvalid(ICollection<FileInfo> files)
        {
            foreach(var f in files){
                try {
                    f.Delete();
                }
                catch(Exception e) {
                    
                }
            }
        }

        virtual protected async Task LoadStored()
        {
            var entitiesLoaded = ListPool<ResolveSucceededDTO<DataType>>.Get();

            EntityPersistence<DataType>.LoadStarted();

            var logs = ListPool<string>.Get();
            var exceptions = ListPool<Exception>.Get();

            // load stored entities on a background thread,
            // then switch back to main thread a notifiy main store loaded
            await Task.Run(() =>
            {
                var d = this.directory;
                if (!d.Exists)
                {
                    return;
                }

                var entityFiles = d.GetFiles("*.ser", SearchOption.TopDirectoryOnly);

                var serializer = GetSerializer();

                using(var invalidFiles = ListPool<FileInfo>.Get()) {
                    DataType curData = default(DataType);
                    string error;
                    foreach (var f in entityFiles)
                    {
                        logs.Add("...try read file '" + f.FullName);
                        try
                        {
                            using (var s = f.OpenRead())
                            {
                                var entitySer = serializer.ReadOne(s);

                                if (!IsValid(ref entitySer))
                                {
                                    logs.Add("not valid: " + f.FullName);
                                    invalidFiles.Add(f);
                                    continue;
                                }

                                var id = Path.GetFileNameWithoutExtension(f.Name);

                                if (!Serial2Data(entitySer, ref curData, out error))
                                {
                                    logs.Add("fail to marshal: " + f.FullName + ": error=" + error);
                                    invalidFiles.Add(f);
                                    continue;
                                }

                                logs.Add("ADDED: " + f.FullName);

                                entitiesLoaded.Add(new ResolveSucceededDTO<DataType>
                                {
                                    id = id,
                                    key = id,
                                    data = curData
                                });
                            }
                        }
#pragma warning disable 168
                        catch (Exception e)
                        {
                            invalidFiles.Add(f);
                            exceptions.Add(e);
                        }
#pragma warning restore 168
                    }

                    if(invalidFiles.Count > 0) {
                        OnLoadedInvalid(invalidFiles);
                    }
                }

            }).ConfigureAwait(false);

            await new WaitForUpdate();

#if UNITY_EDITOR || DEBUG_UNSTRIP
            if(exceptions.Count > 0) {
                foreach(var e in exceptions) {
                    Debug.LogError("error loading " + typeof(DataType).Name + ": " + e.Message);
                }
            }
#endif
            exceptions.Dispose();


#if UNITY_EDITOR || DEBUG_UNSTRIP
            if (logs.Count > 0)
            {
                foreach (var logMsg in logs)
                {
                    Debug.Log(typeof(DataType).Name + ": " + logMsg);
                }
            }
#endif
            logs.Dispose();

            using(var entitiesLoadedArr = ArrayPool<ResolveSucceededDTO<DataType>>.GetCopy(entitiesLoaded)) {
                entitiesLoaded.Dispose();
                this.ignoreUpdates = true;
                try
                {
                    Debug.LogError(typeof(DataType).Name + " loaded:\n" + JsonUtility.ToJson(new ResolvedMultipleDTO<DataType>
                    {
                        entities = entitiesLoadedArr.array
                    }));

                    Entity<DataType>.ResolvedMultiple(new ResolvedMultipleDTO<DataType>
                    {
                        entities = entitiesLoadedArr.array
                    });
                }
                catch (Exception e)
                {
#if UNITY_EDITOR || DEBUG_UNSTRIP
                    Debug.LogError("Error on dispatch: " + e.Message);
#endif
                }

                this.ignoreUpdates = false;
            }

            EntityPersistence<DataType>.LoadDone();
        }

        abstract protected bool Data2Serial(DataType data, ref SerializedType result, out string error);
        abstract protected bool Serial2Data(SerializedType ser, ref DataType result, out string error);

        virtual protected bool ignoreUpdates { get; set; }

        virtual protected async void OnEntityUpdated(string id)
        {
            if(this.ignoreUpdates) {
                return;
            }

            // TODO: handle aliases
            Entity<DataType> entity;
            if(!this.entities.GetEntity(id, out entity)) {
                return;
            }

            if(!entity.status.hasResolved) {
                return;
            }

            await Store(entity, id);
        }

        /// <summary>
        /// Override to filter serialized items that are no longer valid
        /// </summary>
        /// <returns><c>true</c>, if valid was ised, <c>false</c> otherwise.</returns>
        /// <param name="item">Item.</param>
        virtual protected bool IsValid(ref SerializedType item)
        {
            return true;
        }

        //public Request<ResolveResultDTO<DataType>> Resolve(string key, Action<Request<ResolveResultDTO<DataType>>> callback)
        //{
        //    var file = FileForId(key);
        //    if (!file.Exists)
        //    {
        //        var result = new LocalRequest<ResolveResultDTO<DataType>>(
        //            new ResolveResultDTO<DataType>
        //            {
        //                status = Constants.STATUS_NOT_FOUND
        //            });

        //        result.Execute(callback);
        //        return result;
        //    }

        //}

        virtual protected async Task Store(Entity<DataType> entity, string id)
        {
            EntityPersistence<DataType>.WillPersist(id);

            string error = null;

            await Task.Run(() =>
            {
                
                var tmp = new FileInfo(Path.GetTempFileName());
                var serializer = GetSerializer();

                SerializedType serialized = default(SerializedType);
                if (!Data2Serial(entity.data, ref serialized, out error))
                {
                    return;
                }

                if (!IsValid(ref serialized))
                {
                    return;
                }

                using (var fs = tmp.OpenWrite())
                {
                    serializer.WriteOne(fs, serialized);
                }

                var file = FileForId(id);
                if (file.Exists)
                {
                    file.Delete();
                }

                tmp.MoveTo(file.FullName);

            }).ConfigureAwait(false);

            await new WaitForUpdate();

            if (string.IsNullOrEmpty(error))
            {
                EntityPersistence<DataType>.DidPersist(id);
                return;
            }

#if UNITY_EDITOR || DEBUG_UNSTRIP
            Debug.LogError("Failed to store entity with id " + id + ": " + error);
#endif
        }

        virtual protected async void OnEntityRemoved(string id)
        {
            await Remove(id);
        }

        virtual protected async Task Remove(string id)
        {
            string error = null;

            await Task.Run(() =>
            {
                var file = FileForId(id);

#if UNITY_EDITOR || DEBUG_UNSTRIP
                Debug.Log("will remove entity for id " + id + " at path " + file);
#endif

                if(!file.Exists) {
                    return;
                }

                file.Delete();

            }).ConfigureAwait(false);

#if UNITY_EDITOR || DEBUG_UNSTRIP
            if (string.IsNullOrEmpty(error))
            {
                return;
            }

            await new WaitForUpdate();

            Debug.LogError("Failed to store entity with id " + id + ": " + error);
#endif
        }


        virtual protected Serializer<SerializedType> GetSerializer()
        {
            return new JsonSerializer<SerializedType>();
        }


        virtual protected FileInfo FileForId(string id)
        {
            var d = this.directory;
            if(!d.Exists) {
                d.Create();
            }
            return new FileInfo(Path.Combine(d.FullName, string.Format("{0}.{1}", id, "ser")));
        }

        protected DirectoryInfo directory { get; set; }
	}

}
#endif