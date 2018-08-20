#if NET_4_6
using System;
using System.IO;
using System.Threading.Tasks;
using BeatThat.Bindings;
using BeatThat.DependencyInjection;
using BeatThat.Pools;
using BeatThat.Requests;
using UnityEngine;

namespace BeatThat.Entities.Persistence
{
    public struct PersistenceInfo
    {
        public string key;
        public bool isStored;
    }

    public class FSEntityPersistence<DataType> : FSEntityPersistence<DataType, DataType>
    {
        override protected EntityPersistenceDAO<DataType, DataType> CreateDAO()
        {
            return new FSDirectoryPersistence<DataType>(EntityDirectory());
        }
    }

    public abstract class FSEntityPersistence<DataType, SerializedType> : BindingService, EntityPersistence<DataType>
    {
        [Inject] HasEntities<DataType> entities;

        override protected void BindAll()
        {
#pragma warning disable 4014
            BindIfReady();
#pragma warning restore 4014
        }

        protected EntityPersistenceDAO<DataType, SerializedType> dao { get; set; }
        abstract protected EntityPersistenceDAO<DataType, SerializedType> CreateDAO();

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

        /// <summary>
        /// Provides an easy way to wipe out / ignore old invalid data format during dev
        /// </summary>
        /// <value>The version.</value>
        virtual protected int version { get { return 1; } }

        protected DirectoryInfo EntityDirectoryDefault(params string[] additionalPathParts)
        {
            var nAdditional = (additionalPathParts != null) ? additionalPathParts.Length : 0;

            using (var pathParts = ArrayPool<string>.Get(5 + nAdditional)) 
            {
                pathParts.array[0] = Application.temporaryCachePath;
                pathParts.array[1] = "beatthat";
                pathParts.array[2] = "entities";
                pathParts.array[3] = typeof(DataType).FullName.ToLower();
                pathParts.array[4] = version.ToString();

                if(nAdditional > 0) {
                    Array.Copy(additionalPathParts, 0, pathParts.array, 5, additionalPathParts.Length);
                }
                return new DirectoryInfo(Path.Combine(pathParts.array));
            }
        }

#pragma warning disable 1998
        virtual protected async Task LoadAndStoreUpdates(DirectoryInfo d)
        {
            this.directory = d;
            this.dao = CreateDAO();
            //this.directory = d;

#if UNITY_EDITOR || DEBUG_UNSTRIP
            Debug.Log("persistence for " + typeof(DataType).Name + " at " + this.directory.FullName);
#endif

            await LoadStored();
            Bind<string>(Entity<DataType>.UPDATED, this.OnEntityUpdated);
            Bind<string>(Entity<DataType>.DID_REMOVE, this.OnEntityRemoved);
        }
#pragma warning restore 1998
        
        protected bool ignoreUpdates { get; private set; }

        /// <summary>
        /// Override for custom behaviour on an entity following update event.
        /// By default, Stores the entity as long as Entity.hasResolved is TRUE/
        /// does nothing otherwise.
        /// </summary>
        virtual protected async Task OnEntityUpdated(string id, Entity<DataType> entity)
        {
            if(!entity.status.hasResolved) {
                return;
            }

            // await
            this.dao.Store(entity, id);
        }

#pragma warning disable 1998
        virtual protected async void OnEntityUpdated(string id)
        {
            if (this.ignoreUpdates)
            {
                return;
            }

            // TODO: handle aliases
            Entity<DataType> entity;
            if (!this.entities.GetEntity(id, out entity))
            {
                return;
            }

            try
            {
                await OnEntityUpdated(id, entity);
            }
            catch (Exception e)
            {
#if UNITY_EDITOR || DEBUG_UNSTRIP
                Debug.LogError("error on handling update of entity with id " + id + ": " + e.Message);
#endif
            }
        }
#pragma warning restore 1998

#pragma warning disable 1998
        virtual protected async void OnEntityRemoved(string id)
        {
            try
            {
                //await 
                this.dao.Remove(id);
            }
            catch (Exception e)
            {
#if UNITY_EDITOR || DEBUG_UNSTRIP
                Debug.LogError("error on remove entity with id " + id + ": " + e.Message);
#endif
            }
        }
#pragma warning restore 1998

#pragma warning disable 1998
        virtual protected async Task LoadStored()
        {
            using (var entities = ListPool<ResolveSucceededDTO<DataType>>.Get())
            {
                //await 
                this.dao.LoadStored(entities);


#if UNITY_EDITOR || DEBUG_UNSTRIP
                Debug.Log("persistence for " + typeof(DataType).Name + " loaded " + entities.Count + " entities");
#endif

                this.ignoreUpdates = true;
                try
                {
                    Entity<DataType>.ResolvedMultiple(new ResolvedMultipleDTO<DataType> {
                        entities = entities
                    });
                }
                catch (Exception e)
                {
#if UNITY_EDITOR || DEBUG_UNSTRIP
                    Debug.LogError("Error on dispatch: " + e.Message);
#endif
                }
            }

            this.ignoreUpdates = false;

            PersistenceNotifications<DataType>.LoadDone();
        }
#pragma warning restore 1998

        public Request<ResolveResultDTO<DataType>> Resolve(string key, Action<Request<ResolveResultDTO<DataType>>> callback)
        {
            return this.dao.Resolve(key, callback);
        }

        protected DirectoryInfo directory { get; set; }
	}

}
#endif