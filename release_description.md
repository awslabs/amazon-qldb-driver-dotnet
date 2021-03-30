### The Amazon QLDB team is pleased to announce the release of v1.2.0 of the QLDB .NET driver. This release adds asynchronous support to the driver.

* Add the asynchronous version of the driver.
* Improved retry logic to now handle more types of failure.

The full list of changes are included in the [change log](https://github.com/awslabs/amazon-qldb-driver-dotnet/blob/master/CHANGELOG.md).

Here is some sample usage for creating an async driver and executing a query:

```c#
    AsyncQldbDriver driver = AsyncQldbDriver.Builder().WithQLDBSessionConfig(config).WithLedger(ledgerName).Build();

    var result = await driver.Execute(async txn => await txn.Execute(query));
```
