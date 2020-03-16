# Amazon QLDB .NET Driver

This is the .NET driver for [Amazon Quantum Ledger Database (QLDB)](https://aws.amazon.com/qldb/), which allows .NET developers
to write software that makes use of AmazonQLDB.

This is a preview release of the Amazon QLDB Driver for .NET, and we do not recommend that it be used for production purposes.

## Requirements

### Basic Configuration

See [Accessing Amazon QLDB](https://docs.aws.amazon.com/qldb/latest/developerguide/accessing.html) for information on connecting to AWS.

### .NET

The driver targets .NET Standard 2.0. Please see the link below for more information on compatibility:

* [.NET Standard](https://docs.microsoft.com/en-us/dotnet/standard/net-standard)

## Getting Started

To use the driver, it can be installed using NuGet package manager. The driver package is named:

```Amazon.QLDB.Driver```

Then, using the driver's namespace, you can now use the driver in your application:

```c#
using System;

namespace Hello
{
    using Amazon.QLDB.Driver;
    using Amazon.QLDBSession;
    using Amazon.QLDBSession.Model;
    using Amazon.Runtime;
    using System.Threading;
    using System.Text;

    class Program
    {
        static void Main(string[] args)
        {
            // Assuming the ledger is already created
            string ledgerName= "my-ledger";

            AmazonQLDBSessionConfig amazonQldbSessionConfig = new AmazonQLDBSessionConfig();

            Console.WriteLine($"Create the QLDB Driver");
            IQldbDriver driver = PooledQldbDriver.Builder()
                .WithQLDBSessionConfig(amazonQldbSessionConfig)
                .WithLedger(ledgerName)
                .Build();

            using (IQldbSession qldbSession = driver.GetSession())
            {
                int tablesCreated = 0;
                tablesCreated = qldbSession.Execute((txn) =>
                {
                    string tableName = "MyTable";
                    Console.WriteLine($"Creating the '{ tableName }' table...");
                    string query = $"CREATE TABLE {tableName}";
                    IResult result = txn.Execute(query);
                    Console.WriteLine($"'{ tableName }' table created sucessfully.");

                    int count = 0;
                    foreach (var row in result)
                    {
                        count++;
                    }
                    return count;
                });
                Console.WriteLine($"Tables created '{ tablesCreated }'");
            }
        }
    }
}

```

### See Also

1. QLDB .NET Driver accepts and returns [Amazon ION](http://amzn.github.io/ion-docs/) Documents. Amazon Ion is a richly-typed, self-describing, hierarchical data serialization format offering interchangeable binary and text representations. For more information read the [ION docs](http://amzn.github.io/ion-docs/docs.html).
2. [Amazon ION Cookbook](http://amzn.github.io/ion-docs/guides/cookbook.html): This cookbook provides code samples for some simple Amazon Ion use cases.
3. Amazon QLDB supports the [PartiQL](https://partiql.org/) query language. PartiQL provides SQL-compatible query access across multiple data stores containing structured data, semistructured data, and nested data. For more information read the [PartiQL docs](https://partiql.org/docs.html).
4. Refer the section [Common Errors while using the Amazon QLDB Drivers](https://docs.aws.amazon.com/qldb/latest/developerguide/driver-errors.html) which describes runtime errors that can be thrown by the Amazon QLDB Driver when calling the qldb-session APIs.

## Development

### Setup

Assuming that Visual Studio is being used, open the solution file (Amazon.QLDB.Driver.sln).

Right click the solution in the Solution Explorer and press "Restore NuGet Packages" if it does not do so automatically.

### Running Tests

You can run the unit tests by right clicking the Amazon.QLDB.Driver.Tests project, that is a part of the solution file, and pressing "Run Tests".

Alternatively you can run the unit tests on the command line with the following:

```dotnet test```

### Documentation 

DocFx is used for documentation. Download [Docfx](https://github.com/dotnet/docfx/releases) as docfx.zip, unzip and extract it to a local folder, and add it to PATH.

You can generate the docstring HTML locally by running the following in the root directory of this repository:

```docfx docs/docfx.json --serve```

## Release Notes

### Release 0.1.0-beta

* Initial preview release of the Amazon QLDB Driver for .NET.

## License

This library is licensed under the Apache 2.0 License.
