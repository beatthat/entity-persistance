using System;
using BeatThat.Requests;

namespace BeatThat.Entities.Persistence
{

    public interface EntityPersistence<DataType>
    {


        //Request<PersistenceInfo> GetInfo(string key, Action<Request<PersistenceInfo>> callback);


        Request<ResolveResultDTO<DataType>> Resolve(string key, Action<Request<ResolveResultDTO<DataType>>> callback);
       
	}

}