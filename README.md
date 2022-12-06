# Amazon QLDB .NET Driver

This is the .NET driver for [Amazon Quantum Ledger Database (QLDB)](https://aws.amazon.com/qldb/), which allows .NET developers
to write software that makes use of AmazonQLDB.

[![nuget](https://img.shields.io/nuget/v/Amazon.QLDB.Driver.svg)](https://www.nuget.org/packages/Amazon.QLDB.Driver/)
[![license](https://img.shields.io/badge/license-Apache%202.0-blue)](https://github.com/awslabs/amazon-qldb-driver-dotnet/blob/master/LICENSE)
[![AWS Provider](https://img.shields.io/badge/provider-AWS-orange?logo=amazon-aws&color=ff9900)](https://aws.amazon.com/qldb/)
[![codecov](https://codecov.io/gh/awslabs/amazon-qldb-driver-dotnet/branch/master/graph/badge.svg?token=JF7HMNH8IR)](https://codecov.io/gh/awslabs/amazon-qldb-driver-dotnet)


## Requirements

### Basic Configuration

See [Accessing Amazon QLDB](https://docs.aws.amazon.com/qldb/latest/developerguide/accessing.html) for information on connecting to AWS.

### .NET

The driver targets .NET Standard 2.0. Please see the link below for more information on compatibility:

* [.NET Standard](https://learn.microsoft.com/en-us/dotnet/standard/net-standard?tabs=net-standard-2-0)

## Getting Started

Please see the [Quickstart guide for the Amazon QLDB Driver for .Net](https://docs.aws.amazon.com/qldb/latest/developerguide/driver-quickstart-dotnet.html).


### See Also

1. Using the Amazon QLDB Driver for .NET — The best way to get familiar with the Amazon QLDB Driver for .NET is to read [Getting Started with the Amazon QLDB Driver](https://docs.aws.amazon.com/qldb/latest/developerguide/getting-started.dotnet.html) in the [Amazon QLDB Developer Guide](https://docs.aws.amazon.com/qldb/latest/developerguide/what-is.html).
1. QLDB .NET Driver accepts and returns [Amazon ION](http://amzn.github.io/ion-docs/) Documents. Amazon Ion is a richly-typed, self-describing, hierarchical data serialization format offering interchangeable binary and text representations. For more information read the [ION docs](http://amzn.github.io/ion-docs/docs.html).
   1. In version >=1.3.0, support is added for accepting and returning native C# types. See [here](SERIALIZATION.md) for a quick guide on how to use this new feature.
1. [Amazon ION Cookbook](http://amzn.github.io/ion-docs/guides/cookbook.html): This cookbook provides code samples for some simple Amazon Ion use cases.
1. Amazon QLDB supports the [PartiQL](https://partiql.org/) query language. PartiQL provides SQL-compatible query access across multiple data stores containing structured data, semistructured data, and nested data. For more information read the [PartiQL docs](https://partiql.org/docs.html).
1. Refer the section [Common Errors while using the Amazon QLDB Drivers](https://docs.aws.amazon.com/qldb/latest/developerguide/driver-errors.html) which describes runtime errors that can be thrown by the Amazon QLDB Driver when calling the qldb-session APIs.

## Development

### Setup

Assuming that Visual Studio is being used, open the solution file (Amazon.QLDB.Driver.sln).

Right click the solution in the Solution Explorer and press "Restore NuGet Packages" if it does not do so automatically.

### Running Tests

You can run the unit tests by right clicking the Amazon.QLDB.Driver.Tests project, that is a part of the solution file, and pressing "Run Tests".

Alternatively you can run the unit tests on the command line with the following:

```dotnet test Amazon.QLDB.Driver.Tests```

To run the integration tests, you must run it on the command line with the following:

```dotnet test Amazon.QLDB.Driver.IntegrationTests --settings Amazon.QLDB.Driver.IntegrationTests/.runsettings```

### Documentation 

DocFx is used for documentation. Download [Docfx](https://github.com/dotnet/docfx/releases) as docfx.zip, unzip and extract it to a local folder, and add it to PATH.

You can generate the docstring HTML locally by running the following in the root directory of this repository:

```docfx docs/docfx.json --serve```


## License

This library is licensed under the Apache 2.0 License.
