#if NET_4_6
using System;
using System.IO;
using BeatThat.Serializers;
using UnityEngine;

namespace BeatThat.Entities.Persistence
{

    public class FSImagePersistence : FSDirectoryPersistence<Texture2D, byte[]>
    {
        public FSImagePersistence(DirectoryInfo d) 
            : base(d, 
                   FSImagePersistence.Data2Serial, 
                   FSImagePersistence.Serial2Data,
                   serializerFac: ByteArraySerializer.SHARED_INSTANCE_FACTORY)
        {
            
        }

        private static bool Data2Serial(Texture2D data, ref byte[] result, out string error)
        {
            try {
                result = data.EncodeToPNG();
                error = null;
                return true;
            }
            catch(Exception e) {
                result = null;
                error = e.Message;
                return false;
            }
        }

        private static bool Serial2Data(byte[] ser, ref Texture2D result, out string error)
        {
            error = null;
            result = new Texture2D(2, 2);
            result.LoadImage(ser);
            return true;
        }
    }


}
#endif