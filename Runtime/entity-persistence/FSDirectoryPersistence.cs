#if NET_4_6
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
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
        
        private static bool AllValid(ref SerialType serialized)
        {
            return true;
        }

        public FSDirectoryPersistence(
            DirectoryInfo d,
            ConvertDelegate<DataType, SerialType> data2Serial,
            ConvertDelegate<SerialType, DataType> serial2Data,
            ValidationDelegate<SerialType> isValid = null,
            SerializerFactory<EntitySerialized<SerialType>> serializerFac = null
        )
        {
            this.directory = d;
            this.data2Serial = data2Serial;
            this.serial2Data = serial2Data;
            this.isValid = isValid ?? AllValid;
            this.serializerFactory = serializerFac ?? JsonSerializer<EntitySerialized<SerialType>>.SHARED_INSTANCE_FACTORY;
        }

        public EntityPersistenceDAO<DataType, SerialType> SetSerializerFactory(SerializerFactory<EntitySerialized<SerialType>> sfac)
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

        virtual public void LoadStored(ICollection<ResolveSucceededDTO<DataType>> result)
        {
            PersistenceNotifications<DataType>.LoadStarted();

#if DEBUG_FSDirectoryPersistence
            var debug = ListPool<string>.Get();
#endif
            var exceptions = ListPool<Exception>.Get();


            try
            {
                // load stored entities on a background thread,
                // then switch back to main thread a notifiy main store loaded
                //await Task.Run(() =>
                //{
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

                                var dataSer = entitySer.data;

                                if (!this.isValid(ref dataSer))
                                {
                                    //logs.Add("not valid: " + f.FullName);
                                    invalidFiles.Add(f);
                                    continue;
                                }

                                var id = Path.GetFileNameWithoutExtension(f.Name);

                                if (!this.serial2Data(dataSer, ref curData, out error))
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
                                    data = curData,
                                    maxAgeSecs = entitySer.maxAgeSecs,
                                    timestamp = new DateTime(entitySer.timestamp)
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

#if UNITY_EDITOR || DEBUG_UNSTRIP
                    if (exceptions.Count > 0)
                    {
                        foreach (var e in exceptions)
                        {
                            Debug.LogError("error loading " + typeof(DataType).Name + ": " + e.Message);
                        }
                    }
#endif


#if DEBUG_FSDirectoryPersistence
                if (debug.Count > 0)
                {
                    foreach (var logMsg in debug)
                    {
                        Debug.Log(typeof(DataType).Name + ": " + logMsg);
                    }
                }
#endif

                }
                finally
                {
                    exceptions.Dispose();
#if DEBUG_FSDirectoryPersistence
                debug.Dispose();
#endif

                    
                }


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
            get
            {
                var ot = typeof(UnityEngine.Object);

                return ot.IsAssignableFrom(typeof(DataType))
                         || ot.IsAssignableFrom(typeof(SerialType));
            }
        }

        virtual public void Store(Entity<DataType> entity, string id)
        {
            PersistenceNotifications<DataType>.WillPersist(id);

            string error = null;

#if DEBUG_FSDirectoryPersistence
            var debug = ListPool<string>.Get();
#endif
            try
            {
                //await Task.Run(async () =>
                //{


                var file = FileForId(id);

                if (!file.Directory.Exists)
                {
#if DEBUG_FSDirectoryPersistence
                        debug.Add(typeof(DataType).Name + " " + id
                                  + " will attempt to create directory " + file.Directory.FullName);
#endif
                    file.Directory.Create();
                }

                var tmp = new FileInfo(file.FullName + "." + Guid.NewGuid().ToString());


#if DEBUG_FSDirectoryPersistence
                debug.Add(typeof(DataType).Name + " " + id
                              + " will write to " + tmp.FullName);
#endif
                var serializer = GetSerializer();

                SerialType serialData = default(SerialType);

                var serializeOnMain = this.serializeOnMainThread;

                //if (serializeOnMain)
                //{
                //    await new WaitForUpdate();
                //}

                if (!this.data2Serial(entity.data, ref serialData, out error))
                {
                    return;
                }

                //if (serializeOnMain)
                //{
                //    await new WaitForBackgroundThread();
                //}

                if (!this.isValid(ref serialData))
                {
                    return;
                }

                var entityStatus = entity.status;

                using (var fs = tmp.OpenWrite())
                {
                    serializer.WriteOne(fs, new EntitySerialized<SerialType> {
                        data = serialData,
                        timestamp = entityStatus.timestamp.Ticks,
                        maxAgeSecs = entityStatus.maxAgeSecs
                    });
                }


#if DEBUG_FSDirectoryPersistence
                debug.Add(typeof(DataType).Name + " " + id
                              + " finished write to " + tmp.FullName);

                var fileName = Path.Combine(this.directory.FullName,
                                            string.Format("{0}.{1}", id, "ser"));

                debug.Add(typeof(DataType).Name + " " + id
                          + " write filename is " + fileName);

                    var testF = new FileInfo(fileName);


                debug.Add(typeof(DataType).Name + " " + id
                          + " file.FullName= " + testF.FullName);

                debug.Add(typeof(DataType).Name + " " + id
                          + " " + testF.FullName + ".Exists=" + testF.Exists);
                
#endif
                    


#if DEBUG_FSDirectoryPersistence
                debug.Add(typeof(DataType).Name + " " + id
                              + " will use file " + file.FullName);
#endif
                if (file.Exists) {

#if DEBUG_FSDirectoryPersistence
                    debug.Add(typeof(DataType).Name + " " + id
                                  + " will delete file " + file.FullName);
#endif
                    file.Delete();
                }


#if DEBUG_FSDirectoryPersistence
                debug.Add(typeof(DataType).Name + " " + id
                              + " will move " + tmp.FullName
                              + " to " + file.FullName);
#endif
                

                tmp.MoveTo(file.FullName);



                //}).ConfigureAwait(false);

                //await new WaitForUpdate();

            }
            finally {

#if DEBUG_FSDirectoryPersistence
                foreach (var msg in debug)
                {
                    Debug.Log(msg);
                }
                ListPool<string>.Return(debug);
#endif
            }

            if (string.IsNullOrEmpty(error))
            {
                PersistenceNotifications<DataType>.DidPersist(id);
                return;
            }

#if UNITY_EDITOR || DEBUG_UNSTRIP
            Debug.LogError("Failed to store entity with id " + id + ": " + error);
#endif
        }

        virtual public void Remove(string id)
        {
            string error = null;

            var file = FileForId(id);

#if UNITY_EDITOR || DEBUG_UNSTRIP
            Debug.Log("will remove entity for id " + id + " at path " + file);
#endif

            if (!file.Exists)
            {
                return;
            }

            file.Delete();

#if UNITY_EDITOR || DEBUG_UNSTRIP
            if (string.IsNullOrEmpty(error))
            {
                return;
            }

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
                EntitySerialized<SerialType> serialEntity;
                using (var s = f.OpenRead())
                {
                    serialEntity = serializer.ReadOne(s);
                }

                var serialData = serialEntity.data;

                if (!this.isValid(ref serialData))
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
                if (!this.serial2Data(serialData, ref result, out error))
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
                    key = key,
                    maxAgeSecs = serialEntity.maxAgeSecs,
                    timestamp = new DateTime(serialEntity.timestamp)
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

        protected Serializer<EntitySerialized<SerialType>> GetSerializer()
        {
            return this.serializerFactory.Create();
        }

        protected SerializerFactory<EntitySerialized<SerialType>> serializerFactory { get; set; }

        virtual protected FileInfo FileForId(string id, bool ensureDirectory = false)
        {
            var d = this.directory;
            if (ensureDirectory && !d.Exists)
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

#pragma warning disable 1998
            private async void ExecuteAsync()
            {
                ResolveResultDTO<DataType> result = 
                    default(ResolveResultDTO<DataType>);

                //await Task.Run(() =>
                //{
                    result = this.owner.Resolve(key);
                //}).ConfigureAwait(false);


                //await new WaitForUpdate();

                this.item = result;
                CompleteRequest();
            }
        }
#pragma warning restore 1998

        protected DirectoryInfo directory { get; private set; }
        protected ValidationDelegate<SerialType> isValid { get; set; }
        protected ConvertDelegate<DataType, SerialType> data2Serial { get; private set; }
        protected ConvertDelegate<SerialType, DataType> serial2Data { get; private set; }
	}

}
#endif