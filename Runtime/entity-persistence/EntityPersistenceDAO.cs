using System;
using System.Collections.Generic;
using BeatThat.Requests;
using BeatThat.Serializers;

namespace BeatThat.Entities.Persistence
{
    [Serializable]
    public struct EntitySerialized<SerialType>
    {
        public SerialType data;
        public long timestamp;
        public int maxAgeSecs;
    }

    public delegate bool ValidationDelegate<T>(ref T ser);
    public delegate bool ConvertDelegate<T1, T2>(T1 data, ref T2 result, out string error);

    public interface EntityPersistenceDAO<DataType, SerialType>
    {
        EntityPersistenceDAO<DataType, SerialType> SetSerializerFactory(SerializerFactory<EntitySerialized<SerialType>> serializerFactory);

        EntityPersistenceDAO<DataType, SerialType> SetSerialTypeValidation(ValidationDelegate<SerialType> isValid);

        void LoadStored(ICollection<StoreEntityDTO<DataType>> result);

        Request<ResolveResultDTO<DataType>> Resolve(string key, Action<Request<ResolveResultDTO<DataType>>> callback);

        void Store(Entity<DataType> entity, string id);

        void Remove(string id);

        void RemoveAll();
       
	}

}