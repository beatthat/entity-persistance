#if NET_4_6
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using BeatThat.AsyncAwaitUntil;
using BeatThat.Pools;
using BeatThat.Requests;
using BeatThat.Serializers;
using UnityEngine;

namespace BeatThat.Entities.Persistence
{

    public class FSDirectoryPersistence<DataType> : FSDirectoryPersistence<DataType, DataType>
    {
        public FSDirectoryPersistence(DirectoryInfo d) 
            : base(d, FSDirectoryPersistence<DataType>.Data2Serial, 
                   FSDirectoryPersistence<DataType>.Serial2Data)
        {
            
        }

        private static bool Data2Serial(DataType data, ref DataType result, out string error)
        {
            error = null;
            result = data;
            return true;
        }

        private static bool Serial2Data(DataType ser, ref DataType result, out string error)
        {
            error = null;
            result = ser;
            return true;
        }
    }

    public class FSDirectoryPersistence<DataType, SerialType> : EntityPersistenceDAO<DataType, SerialType>
	{
        private static bool AllValid(ref SerialType serialized){
            return true;
        }

        public FSDirectoryPersistence(
            DirectoryInfo d,
            ConvertDelegate<DataType, SerialType> data2Serial,
            ConvertDelegate<SerialType, DataType> serial2Data,
            ValidationDelegate<SerialType> isValid = null,
            SerializerFactory<SerialType> serializerFac = null
        )
        {
            this.directory = d;
            this.data2Serial = data2Serial;
            this.serial2Data = serial2Data;
            this.isValid = isValid ?? AllValid;
            this.serializerFactory = serializerFac ?? JsonSerializer<SerialType>.SHARED_INSTANCE_FACTORY;
        }

        public EntityPersistenceDAO<DataType, SerialType> SetSerializerFactory(SerializerFactory<SerialType> sfac)
        {
            this.serializerFactory = sfac;
            return this;
        }

        public EntityPersistenceDAO<DataType, SerialType> SetSerialTypeValidation(ValidationDelegate<SerialType> isValid)
        {
            this.isValid = isValid;
            return this;
        }

        public Request<PersistenceInfo> GetInfo(string key, Action<Request<PersistenceInfo>> callback)
        {
            var f = FileForId(key);
            var r = new LocalRequest<PersistenceInfo>(new PersistenceInfo
            {
                key = key,
                isStored = f.Exists
            });
            r.Execute(callback);
            return r;
        }

        virtual public async Task LoadStored(ICollection<ResolveSucceededDTO<DataType>> result)
        {
            PersistenceNotifications<DataType>.LoadStarted();

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

                using (var invalidFiles = ListPool<FileInfo>.Get())
                {
                    DataType curData = default(DataType);
                    string error;
                    foreach (var f in entityFiles)
                    {
                        //logs.Add("...try read file '" + f.FullName);
                        try
                        {
                            using (var s = f.OpenRead())
                            {
                                var entitySer = serializer.ReadOne(s);

                                if (!this.isValid(ref entitySer))
                                {
                                    //logs.Add("not valid: " + f.FullName);
                                    invalidFiles.Add(f);
                                    continue;
                                }

                                var id = Path.GetFileNameWithoutExtension(f.Name);

                                if (!this.serial2Data(entitySer, ref curData, out error))
                                {
                                    //logs.Add("fail to marshal: " + f.FullName + ": error=" + error);
                                    invalidFiles.Add(f);
                                    continue;
                                }

                                //logs.Add("ADDED: " + f.FullName + ": \n" + JsonUtility.ToJson(curData));

                                result.Add(new ResolveSucceededDTO<DataType>
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

                    if (invalidFiles.Count > 0)
                    {
                        OnLoadedInvalid(invalidFiles);
                    }
                }

            }).ConfigureAwait(false);

            await new WaitForUpdate();

#if UNITY_EDITOR || DEBUG_UNSTRIP
            if (exceptions.Count > 0)
            {
                foreach (var e in exceptions)
                {
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
        }

        public Request<ResolveResultDTO<DataType>> Resolve(string key, Action<Request<ResolveResultDTO<DataType>>> callback)
        {
            var file = FileForId(key);
            if (!file.Exists)
            {
                var notFound = new LocalRequest<ResolveResultDTO<DataType>>(
                    new ResolveResultDTO<DataType>
                    {
                        status = ResolveStatusCode.NOT_FOUND
                    });

                notFound.Execute(callback);
                return notFound;
            }

            var result = new ItemRequest(key, this);
            result.Execute(callback);
            return result;
        }

        virtual protected bool serializeOnMainThread
        {
            get {
                var ot = typeof(UnityEngine.Object);

                return ot.IsAssignableFrom(typeof(DataType))
                         || ot.IsAssignableFrom(typeof(SerialType));
            }
        }

        virtual public async Task Store(Entity<DataType> entity, string id)
        {
            PersistenceNotifications<DataType>.WillPersist(id);

            string error = null;

            await Task.Run(async () =>
            {
                
                var tmp = new FileInfo(Path.GetTempFileName());
                var serializer = GetSerializer();

                SerialType serialized = default(SerialType);

                var serializeOnMain = this.serializeOnMainThread;

                if(serializeOnMain) {
                    await new WaitForUpdate();
                }

                if (!this.data2Serial(entity.data, ref serialized, out error))
                {
                    return;
                }

                await new WaitForBackgroundThread();

                if (!this.isValid(ref serialized))
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
                PersistenceNotifications<DataType>.DidPersist(id);
                return;
            }

#if UNITY_EDITOR || DEBUG_UNSTRIP
            Debug.LogError("Failed to store entity with id " + id + ": " + error);
#endif
        }

        virtual public async Task Remove(string id)
        {
            string error = null;

            await Task.Run(() =>
            {
                var file = FileForId(id);

#if UNITY_EDITOR || DEBUG_UNSTRIP
                Debug.Log("will remove entity for id " + id + " at path " + file);
#endif

                if (!file.Exists)
                {
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

        virtual protected void OnLoadedInvalid(ICollection<FileInfo> files)
        {
            foreach (var f in files)
            {
                try
                {
                    f.Delete();
                }
#pragma warning disable 168
                catch (Exception e)
                {

                }
#pragma warning restore 168
            }
        }

        private ResolveResultDTO<DataType> Resolve(string key)
        {
            var f = FileForId(key);
            if (!f.Exists)
            {
                return new ResolveResultDTO<DataType>
                {
                    status = ResolveStatusCode.NOT_FOUND,
                    message = "not found"
                };
            }

            var serializer = this.GetSerializer();

            try
            {
                SerialType dto;
                using (var s = f.OpenRead())
                {
                    dto = serializer.ReadOne(s);
                }

                if (!this.isValid(ref dto))
                {
#pragma warning disable 4014
                    Remove(key);
#pragma warning restore 4014

                    return new ResolveResultDTO<DataType>
                    {
                        status = ResolveStatusCode.NOT_FOUND,
                        message = "serialized content is invalid"
                    };
                }

                DataType result = default(DataType);
                string error;
                if (!this.serial2Data(dto, ref result, out error))
                {
#pragma warning disable 4014
                    Remove(key);
#pragma warning restore 4014

                    return new ResolveResultDTO<DataType>
                    {
                        status = ResolveStatusCode.NOT_FOUND,
                        message = error
                    };
                }

                return new ResolveResultDTO<DataType>
                {
                    status = ResolveStatusCode.OK,
                    data = result,
                    id = key, // TODO: alias handling !!!
                    key = key
                };
            }
            catch (Exception e)
            {
                return new ResolveResultDTO<DataType>
                {
                    status = ResolveStatusCode.ERROR,
                    message = e.Message
                };
            }
        }

        protected Serializer<SerialType> GetSerializer()
        {
            return this.serializerFactory.Create();
        }

        protected SerializerFactory<SerialType> serializerFactory { get; set; }

        virtual protected FileInfo FileForId(string id)
        {
            var d = this.directory;
            if (!d.Exists)
            {
                d.Create();
            }
            return new FileInfo(Path.Combine(d.FullName, string.Format("{0}.{1}", id, "ser")));
        }


        class ItemRequest : RequestBase, Request<ResolveResultDTO<DataType>>
        {
            public ItemRequest(string key, FSDirectoryPersistence<DataType, SerialType> owner)
            {
                this.key = key;
                this.owner = owner;
            }
            public string key { get; set; }
            public ResolveResultDTO<DataType> item { get; private set; }

            private FSDirectoryPersistence<DataType, SerialType> owner { get; set; }

            public object GetItem()
            {
                return this.item;
            }

            protected override void ExecuteRequest()
            {
                if(!this.owner.serializeOnMainThread) {
                    ExecuteAsync();
                    return;
                }
                this.item = this.owner.Resolve(key);
                CompleteRequest();
            }

            private async void ExecuteAsync()
            {
                ResolveResultDTO<DataType> result = 
                    default(ResolveResultDTO<DataType>);

                await Task.Run(() =>
                {
                    result = this.owner.Resolve(key);
                }).ConfigureAwait(false);


                await new WaitForUpdate();

                this.item = result;
                CompleteRequest();
            }
        }

        protected DirectoryInfo directory { get; private set; }
        protected ValidationDelegate<SerialType> isValid { get; set; }
        protected ConvertDelegate<DataType, SerialType> data2Serial { get; private set; }
        protected ConvertDelegate<SerialType, DataType> serial2Data { get; private set; }
	}

}
#endif