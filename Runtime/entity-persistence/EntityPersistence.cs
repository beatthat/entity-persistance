using BeatThat.Notifications;

namespace BeatThat.Entities.Persistence
{

    using N = NotificationBus;
    using Opts = NotificationReceiverOptions;
    public struct EntityPersistence<DataType>
    {
        public static readonly string LOAD_STARTED = typeof(DataType).FullName + "_PERSISTENCE_LOAD_STARTED";
        public static void LoadStarted(Opts opts = Opts.DontRequireReceiver)
        {
            N.Send(LOAD_STARTED, opts);
        }

        public static readonly string LOAD_DONE = typeof(DataType).FullName + "_PERSISTENCE_LOAD_DONE";
        public static void LoadDone(Opts opts = Opts.DontRequireReceiver)
        {
            N.Send(LOAD_DONE, opts);
        }

        public static readonly string WILL_PERSIST = typeof(DataType).FullName + "_PERSISTENCE_WILL_PERSIST";
        public static void WillPersist(string id, Opts opts = Opts.DontRequireReceiver)
        {
            N.Send(WILL_PERSIST, id, opts);
        }

        public static readonly string DID_PERSIST = typeof(DataType).FullName + "_PERSISTENCE_DID_PERSIST";
        public static void DidPersist(string id, Opts opts = Opts.DontRequireReceiver)
        {
            N.Send(DID_PERSIST, id, opts);
        }
    }
}