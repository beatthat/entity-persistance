using System;
using System.Collections.Generic;
#if NET_4_6
using System.Threading.Tasks;
using BeatThat.Requests;
using BeatThat.Serializers;

namespace BeatThat.Entities.Persistence
{
    
    public delegate bool ValidationDelegate<T>(ref T ser);
    public delegate bool ConvertDelegate<T1, T2>(T1 data, ref T2 result, out string error);

    public interface EntityPersistenceDAO<DataType, SerializedType>
    {
        EntityPersistenceDAO<DataType, SerializedType> SetSerializerFactory(SerializerFactory<SerializedType> serializerFactory);

        EntityPersistenceDAO<DataType, SerializedType> SetSerialTypeValidation(ValidationDelegate<SerializedType> isValid);

        Task LoadStored(ICollection<ResolveSucceededDTO<DataType>> result);

        Request<ResolveResultDTO<DataType>> Resolve(string key, Action<Request<ResolveResultDTO<DataType>>> callback);

        Task Store(Entity<DataType> entity, string id);

        Task Remove(string id);
       
	}

}
#endif