# Querying QLDB as C# Types

In addition to executing statement queries against QLDB which return an Ion Value, `IIonValue`, the driver also provides the interface method `public Generic.IResult<T> Execute<T>(IQuery<T> query);`.

This interface is designed to completely bypass the need to work with Ion by allowing native C# types to be passed in as parameters as well as returning C# native types, which the generic type is. 

### Basic Usage Example

```c#
using System;
using Amazon.QLDB.Driver;
using Amazon.QLDB.Driver.Generic;
using Amazon.QLDB.Driver.Serialization;

namespace Example
{
    public class Person
    {
        public string Name { get; init; }

        public int Age { get; init; }
    }

    class Example
    {
        static void Main(string[] args)
        {
            // ISerializer implementation we provide
            ISerializer serializer = new ObjectSerializer();

            IQldbDriver driver = QldbDriver.Builder()
                .WithLedger("vehicle-registration")
                .WithSerializer(serializer)
                .Build();

            Person myPerson = driver.Execute(txn =>
            {
                // Creating a query with native C# type parameters specifying the expected output type Person
                IQuery<Person> myQuery = txn.Query<Person>("SELECT * FROM People WHERE Age = ?", 21);

                // Execute the created query
                IResult<Person> myResult = txn.Execute(myQuery);

                foreach (var Person in myResult)
                {
                    return Person;
                }
                return null;
            });

            // myPerson and its properties are directly returned from QLDB now as C# types instead of IIonType.
            Console.WriteLine(myPerson.Name);
        }
    }
}
```

### ISerializer

In the above example, this library provided an `ISerializer` to the driver, which `txn.Query` uses to create an `IQuery`. The `ObjectSerializer` is a wrapper around the [Amazon Ion Object Mapper](https://github.com/amzn/ion-object-mapper-dotnet) with default settings which supports the following [primitive type conversions](https://github.com/amzn/ion-object-mapper-dotnet/blob/main/SPEC.md#primitive-type-conversion). If desired, a custom implementation can be used as long as it implements the `ISerializer` interface.

```c#
public interface ISerializer
{
    /// <summary>
    /// Serialize a C# object into a ValueHolder object containing the Ion binary or text value.
    /// </summary>
    ///
    /// <param name="o">The C# object to be serialized into ValueHolder.</param>
    ///
    /// <returns>The ValueHolder object containing the Ion binary or text value.</returns>
    ValueHolder Serialize(object o);

    /// <summary>
    /// Deserialize a ValueHolder object containing the Ion binary or text value into an object of type T.
    /// </summary>
    ///
    /// <param name="s">The ValueHolder object to be deserialized into an object of type T.</param>
    /// <typeparam name="T">The return type.</typeparam>
    ///
    /// <returns>The object of type T.</returns>
    T Deserialize<T>(ValueHolder s);
}
```

However, as implementing a fully-functioning `ISerializer` is challenging, it is *highly recommended* to instead utilize the `IonSerializer` in the Amazon Ion Object Mapper mentioned above. It is highly configurable and can be wrapped to call it's own `Serialize` and `Deserialize<T>` methods to satisfy the interface of a custom `Iserializer`.

### IQuery

In the Basic Usage Example, it is shown that `txn.Execute<T>(IQuery<T> query)` is used to return an `IResult<T>` where `T` is the type determined by the `IQuery` parameter. `txn.Query` was used to generate this, using the `ISerializer` that was passed into the builder for the driver. If a different `ISerializer` is desired for a specific `IQuery`, it is possible to do so.

```c#
using System;
using Amazon.IonDotnet;
using Amazon.IonDotnet.Builders;
using Amazon.QLDB.Driver;
using Amazon.QLDB.Driver.Generic;
using Amazon.QLDB.Driver.Serialization;
using Amazon.QLDBSession.Model;

namespace Example
{
    public class AgeAsStringQuery : IQuery<string>
    {
        public string Statement => "SELECT VALUE Age from People";

        public ValueHolder[] Parameters => Array.Empty<ValueHolder>();

        public string Deserialize(ValueHolder ionValueHolder)
        {
            IIonReader reader = IonReaderBuilder.Build(ionValueHolder.IonBinary);
            reader.MoveNext();
            return reader.IntValue().ToString();
        }
    }

    class Example
    {
        static void Main(string[] args)
        {
            // ISerializer implementation we provide but will not be used
            ISerializer serializer = new ObjectSerializer();

            IQldbDriver driver = QldbDriver.Builder()
                .WithLedger("vehicle-registration")
                .WithSerializer(serializer)
                .Build();

            string myAge = driver.Execute(txn =>
            {
                // Custom IQuery implementation
                IQuery<string> myQuery = new AgeAsStringQuery();

                // Execute the created query
                IResult<string> myResult = txn.Execute(myQuery);

                foreach (var age in myResult)
                {
                    return age;
                }
                return null;
            });

            // The custom query returns the age as a string despite the type being an Ion Integer
            Console.WriteLine(myAge);
        }
    }
}
```

