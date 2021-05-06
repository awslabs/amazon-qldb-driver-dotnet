# Amazon QLDB .NET Driver

[![nuget](https://img.shields.io/nuget/v/Amazon.QLDB.Driver.svg)](https://www.nuget.org/packages/Amazon.QLDB.Driver/)
[![license](https://img.shields.io/badge/license-Apache%202.0-blue)](https://github.com/awslabs/amazon-qldb-driver-dotnet/blob/master/LICENSE)
[![AWS Provider](https://img.shields.io/badge/provider-AWS-orange?logo=amazon-aws&color=ff9900)](https://aws.amazon.com/qldb/)

This is the .NET driver for [Amazon Quantum Ledger Database (QLDB)](https://aws.amazon.com/qldb/), which allows .NET developers
to write software that makes use of Amazon QLDB.

For getting started with the driver, see [.NET and Amazon QLDB](https://docs.aws.amazon.com/qldb/latest/developerguide/getting-started.dotnet.html).

## Requirements

### Basic Configuration

See [Accessing Amazon QLDB](https://docs.aws.amazon.com/qldb/latest/developerguide/accessing.html) for information on connecting to AWS.

### .NET

The driver targets .NET Standard 2.0. Please see the link below for more information on compatibility:

* [.NET Standard](https://docs.microsoft.com/en-us/dotnet/standard/net-standard)

The driver targets .NET Core 2.1. Please see the link below for more information on compatibility:

* [.NET Core](https://dotnet.microsoft.com/download/dotnet-core)

## Getting Started

Please see the [Quickstart guide for the Amazon QLDB Driver for .Net](https://docs.aws.amazon.com/qldb/latest/developerguide/driver-quickstart-dotnet.html).


### See Also

1. Using the Amazon QLDB Driver for .NET — The best way to get familiar with the Amazon QLDB Driver for .NET is to read [Getting Started with the Amazon QLDB Driver](https://docs.aws.amazon.com/qldb/latest/developerguide/getting-started.dotnet.html) in the [Amazon QLDB Developer Guide](https://docs.aws.amazon.com/qldb/latest/developerguide/what-is.html).
2. [QLDB .NET Driver Cookbook](https://docs.aws.amazon.com/qldb/latest/developerguide/driver-cookbook-dotnet.html) The cookbook provides code samples for some simple QLDB Python driver use cases. 
3. [Amazon QLDB .NET Driver Samples](https://github.com/aws-samples/amazon-qldb-dmv-sample-dotnet): A DMV based example application which demonstrates how to use QLDB with the QLDB Driver for Python.
4. QLDB .NET Driver accepts and returns [Amazon ION](http://amzn.github.io/ion-docs/) Documents. Amazon Ion is a richly-typed, self-describing, hierarchical data serialization format offering interchangeable binary and text representations. For more information read the [ION docs](http://amzn.github.io/ion-docs/docs.html).
5. [Amazon ION Cookbook](http://amzn.github.io/ion-docs/guides/cookbook.html): This cookbook provides code samples for some simple Amazon Ion use cases.
6. Amazon QLDB supports the [PartiQL](https://partiql.org/) query language. PartiQL provides SQL-compatible query access across multiple data stores containing structured data, semistructured data, and nested data. For more information read the [PartiQL docs](https://partiql.org/docs.html).
7. Refer the section [Common Errors while using the Amazon QLDB Drivers](https://docs.aws.amazon.com/qldb/latest/developerguide/driver-errors.html) which describes runtime errors that can be thrown by the Amazon QLDB Driver when calling the qldb-session APIs.

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

## Getting Help

Please use these community resources for getting help.
* Ask a question on StackOverflow and tag it with the [amazon-qldb](https://stackoverflow.com/questions/tagged/amazon-qldb) tag.
* Open a support ticket with [AWS Support](http://docs.aws.amazon.com/awssupport/latest/user/getting-started.html).
* Make a new thread at [AWS QLDB Forum](https://forums.aws.amazon.com/forum.jspa?forumID=353&start=0).
* If you think you may have found a bug, please open an [issue](https://github.com/awslabs/amazon-qldb-driver-dotnet/issues/new).

## Opening Issues

If you encounter a bug with the Amazon QLDB .NET Driver, we would like to hear about it. Please search the [existing issues](https://github.com/awslabs/amazon-qldb-driver-dotnet/issues) and see if others are also experiencing the issue before opening a new issue. When opening a new issue, we will need the version of Amazon QLDB .NET Driver, .NET language version, and OS you’re using. Please also include reproduction case for the issue when appropriate.

The GitHub issues are intended for bug reports and feature requests. For help and questions with using AWS QLDB .NET Driver, please make use of the resources listed in the [Getting Help](https://github.com/awslabs/amazon-qldb-driver-dotnet#getting-help) section. Keeping the list of open issues lean will help us respond in a timely manner.

## License

This library is licensed under the Apache 2.0 License.
