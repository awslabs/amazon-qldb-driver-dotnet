## Release v1.4.1

### :beetle: Bug Fixes:
* Fix default backoff function for full jitter method. The backoff time between retries will sleep shorter periods of time. [PR 182](https://github.com/awslabs/amazon-qldb-driver-dotnet/pull/182)

## Release v1.4.0

* Update the minimum version of the driver's Ion dependency to 1.2.2
  * This dependency update includes a fix to a [JSON decimal conversion issue](https://github.com/amzn/ion-dotnet/commit/e13d44f6108fe067495cf5201fa102e9ef35fe8a)
* Update the minimum version of the driver's Ion Hash dependency to 1.1.1
* Update the minimum version of the driver's Ion Object Mapper dependency to 1.0.0
* Update the minimum version of the driver's logging dependency to 6.0.0

## Release v1.3.0

v1.3 adds support for passing in parameters and retrieving rows to and from QLDB as native C# types, bypassing the need to use `IIonValue`!

See [Serialization](SERIALIZATION.md) for a quick tutorial and examples.

## Release v1.2.0

v1.2 adds the asynchronous version of the driver. C# version updated to 9.0.0 minimum for development of the driver.

### :hammer_and_wrench: Improvements

* Added `AsyncQldbDriver`, `AsyncQldbDriverBuilder`, and various other classes to support asynchronous workflows
* Improved retry logic to now handle more types of failure

### :beetle: Bug Fixes:
* Fixed some resource leaks upon calling `Dispose` on the driver
* Fixed a rare race condition when calling `Execute` on the driver
* Added a safeguard to release resources when user-passed functions threw exceptions in `retryAction` and `IBackoffStrategy.CalculateDelay`

## Release v1.1.1

* Update the minimum version of the driver's logging dependency to 2.0.0
* Update the minimum version of the driver's AWS SDK dependency to 3.5.2
* Fixed a bug where it was throwing NullReferenceException after session expiry.

## Release v1.1.0

v1.1 adds support for obtaining basic server-side statistics on individual statement executions.

### :hammer_and_wrench: Improvements

* Added `IOUsage` and `TimingInformation` struct types containing information on server-side statistics
  * IOUsage contains `long ReadIOs`
  * TimingInformation contains `long ProcessingTimeMilliseconds`
* Added `IOUsage? GetConsumedIOs()` and `TimingInformation? GetTimingInformation()` to the `IResult` interface
  * IOUsage and TimingInformation are stateful, meaning the statistics returned by the method reflect the state at the time of method execution

## Release v1.0.1

### :beetle: Bug Fixes:
* Fixed the bug which leads to infinite number of retries on expired long running transactions.
* Merged in the ION Hash fix on ION structs with multiple fields.

### :hammer_and_wrench: Improvements
* Customized exceptions in a transaction will abort the transaction immediately.

## GA Release v1.0.0

### :hammer_and_wrench: Improvements
* Added `Execute` methods that take a customized `RetryPolicy` object. It allows customer to set the retry limit and also create customized 'IBackoffStrategy'.
* Added `QldbDriverBuilder.WithRetryLogging` to enable logging retries.

### :warning: Deprecated
* The `Execute` methods in the `IQldbDriver` class that take a `retryAction` parameter are deprecated, and will be removed in the future release.

## Release the v1.0.0-rc-1
We have reworked the driver to deliver the next set of changes:


* Added the `getTableNames` method to the `QldbDriver` class. For more details please
read the [release notes](https://github.com/awslabs/amazon-qldb-driver-dotnet/releases/tag/v1.0.0-rc.1).
* Support for .NET Core

### :hammer_and_wrench: Improvements
* Improve the performance of the driver by removing unnecessary calls to QLDB.


### :warning: Removed
* `PooledQldbDriver` has been removed in favor of the `QldbDriver`. For more
details please read the [release notes](https://github.com/awslabs/amazon-qldb-driver-dotnet/releases/tag/v1.0.0-rc.1).

* `QldbSession` and `Transaction` classes are not accessible anymore. Please use
`QldbDriver` instead to execute transactions. For more details please read 
the [release notes](https://github.com/awslabs/amazon-qldb-driver-dotnet/releases/tag/v1.0.0-rc.1).

* `QldbDriver.Execute(String)` method has been removed. Please use `QldbDriver.Execute<T>(Func<TransactionExecutor, T> func)` or 
`QldbDriver.Execute(Action<TransactionExecutor> action)` methods instead.

## [v0.1.0-beta](https://github.com/awslabs/amazon-qldb-driver-dotnet/releases/tag/v0.1.0-beta) - 2020-03-13 

Preview release of the Amazon QLDB Driver for .NET.
### :tada: Features 
- Provides the PooledQldbDriver to access QLDB and execute transactions.


