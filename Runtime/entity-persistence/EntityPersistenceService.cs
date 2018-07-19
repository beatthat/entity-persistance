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
    public class EntityPersistenceService<DataType> : BindingService
	{
        [Inject] HasEntities<DataType> entities;

		override protected void BindAll()
		{
            // set default root here because can't happen off main thread
            DEFAULT_FILE_ROOT = new DirectoryInfo(Path.Combine(
                    Application.temporaryCachePath,
                    "beatthat",
                    "entities",
                typeof(DataType).FullName.ToLower()
            ));

            LoadStored();
            Bind<string>(Entity<DataType>.UPDATED, this.OnEntityUpdated);

            //Debug.LogError("[" + Time.frameCount + "] awake with root: " + this.directory.FullName);
		}

        virtual protected async void LoadStored()
        {
            var entitiesLoaded = ListPool<ResolveSucceededDTO<DataType>>.Get();

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

                foreach (var f in entityFiles)
                {
                    try
                    {
                        using (var s = new FileStream(f.FullName, FileMode.Open, FileAccess.Read))
                        {
                            var entity = serializer.ReadOne(s);
                            var id = Path.GetFileNameWithoutExtension(f.Name);
                            entitiesLoaded.Add(new ResolveSucceededDTO<DataType>
                            {
                                id = id,
                                key = id,
                                data = entity.data
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

        }

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
            await Task.Run(() =>
            {
                var path = WritePathForId(id);
                var serializer = GetSerializer();

                using (var fs = new FileStream(path, FileMode.OpenOrCreate, FileAccess.Write))
                {
                    serializer.WriteOne(fs, entity);
                }
            }).ConfigureAwait(false);
        }


        virtual protected Serializer<Entity<DataType>> GetSerializer()
        {
            return new JsonSerializer<Entity<DataType>>();
        }


        virtual protected string WritePathForId(string id)
        {
            var d = this.directory;
            if(!d.Exists) {
                d.Create();
            }
            return Path.Combine(d.FullName, string.Format("{0}.{1}", id, "ser"));
        }

        virtual protected DirectoryInfo directory
        {
            get {
                return DEFAULT_FILE_ROOT;
            }
        }

        private static DirectoryInfo DEFAULT_FILE_ROOT;
	}

}
#endif