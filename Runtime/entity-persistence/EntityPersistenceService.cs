#if NET_4_6
using System;
using System.IO;
using System.Threading.Tasks;
using BeatThat.AsyncAwaitUntil;
using BeatThat.Bindings;
using BeatThat.Pools;
using BeatThat.Serializers;
using BeatThat.Service;
using UnityEngine;

namespace BeatThat.Entities.Persistence
{
    public class EntityPersistenceService<DataType> : EntityPersistenceService<DataType, DataType>
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

    public abstract class EntityPersistenceService<DataType, SerializedType> : BindingService
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

        protected DirectoryInfo EntityDirectory(params string[] additionalPathParts)
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

        virtual protected async Task LoadStored()
        {
            var entitiesLoaded = ListPool<ResolveSucceededDTO<DataType>>.Get();

            EntityPersistence<DataType>.LoadStarted();

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

                DataType curData = default(DataType);
                string error;
                foreach (var f in entityFiles)
                {
                    try
                    {
                        using (var s = new FileStream(f.FullName, FileMode.Open, FileAccess.Read))
                        {
                            var entitySer = serializer.ReadOne(s);
                            var id = Path.GetFileNameWithoutExtension(f.Name);

                            if(!Serial2Data(entitySer.data, ref curData, out error)) {
                                continue;
                            }

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

                    }
#pragma warning restore 168
                }
            }).ConfigureAwait(false);

            await new WaitForUpdate();

            //Debug.LogError("Read " + entitiesLoaded.Count + " " + typeof(DataType).Name);
            using(var entitiesLoadedArr = ArrayPool<ResolveSucceededDTO<DataType>>.GetCopy(entitiesLoaded)) {
                entitiesLoaded.Dispose();
                this.ignoreUpdates = true;
                try
                {
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

        virtual protected async Task Store(Entity<DataType> entity, string id)
        {
            EntityPersistence<DataType>.WillPersist(id);

            string error = null;

            await Task.Run(() =>
            {
                var path = WritePathForId(id);
                var serializer = GetSerializer();

                using (var fs = new FileStream(path, FileMode.OpenOrCreate, FileAccess.Write))
                {
                    SerializedType serialized = default(SerializedType);
                    if (!Data2Serial(entity.data, ref serialized, out error))
                    {
                        return;
                    }
                    serializer.WriteOne(fs, new Entity<SerializedType>
                    {
                        data = serialized,
                        status = entity.status,
                    });
                }
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
                var path = WritePathForId(id);

#if UNITY_EDITOR || DEBUG_UNSTRIP
                Debug.Log("will remove entity for id " + id + " at path " + path);
#endif

                if(!File.Exists(path)) {
                    return;
                }

                File.Delete(path);

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


        virtual protected Serializer<Entity<SerializedType>> GetSerializer()
        {
            return new JsonSerializer<Entity<SerializedType>>();
        }


        virtual protected string WritePathForId(string id)
        {
            var d = this.directory;
            if(!d.Exists) {
                d.Create();
            }
            return Path.Combine(d.FullName, string.Format("{0}.{1}", id, "ser"));
        }

        protected DirectoryInfo directory { get; set; }
	}

}
#endif