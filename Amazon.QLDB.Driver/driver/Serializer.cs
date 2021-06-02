using Amazon.QLDBSession.Model;

public interface ISerializer
{
    ValueHolder Serialize(object o);
    T Deserialize<T>(ValueHolder s);
}