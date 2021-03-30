The Amazon QLDB team is pleased to announce the release of v1.2.0 of the QLDB .NET driver.

## Enhancements
* Add the asynchronous version of the driver.
* Improved retry logic to now handle more types of failure.

The full list of changes are included in the [change log](https://github.com/awslabs/amazon-qldb-driver-dotnet/blob/master/CHANGELOG.md).

The following is some sample usage for the async driver:

Create async driver:
```c#
    AmazonQLDBSessionConfig amazonQLDBSessionConfig = new AmazonQLDBSessionConfig();
    IAsyncQldbDriver driver = AsyncQldbDriver.Builder()
        .WithQLDBSessionConfig(amazonQLDBSessionConfig)
        .WithLedger("testLedger")
        .Build();
```

Execute a query:
```c#
    IAsyncResult result = await driver.Execute(async txn => await txn.Execute("SELECT * from Person");
```

Iterate through a result:
```c#
    await foreach (IIonValue ionValue in result)
    {
        Console.WriteLine(ionValue.ToPrettyString());
    }
```
