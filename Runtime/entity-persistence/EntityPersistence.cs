using System;
using BeatThat.Requests;

namespace BeatThat.Entities.Persistence
{
    // TODO: make an attr RegisterEntityPersistence to simpify
    public interface EntityPersistence<DataType>
    {


        //Request<PersistenceInfo> GetInfo(string key, Action<Request<PersistenceInfo>> callback);


        Request<ResolveResultDTO<DataType>> Resolve(string key, Action<Request<ResolveResultDTO<DataType>>> callback);
       
	}

}