## Release v1.0.1

#### Bug Fixes:
* Fixed the bug which leads to infinite number of retries on expried long running transactions.

### :hammer_and_wrench:​ Improvements
* Customized exceptions in a transaction will abort the transaction immediately.

## GA Release v1.0.0

### :hammer_and_wrench:​ Improvements
* Added `Execute` methods that take a customized `RetryPolicy` object. It allows customer to set the retry limit and also create customized 'IBackoffStrategy'.
* Added `QldbDriverBuilder.WithRetryLogging` to enable logging retries.

### :warning: Deprecated
* The `Execute` methods in the `IQldbDriver` class that take a `retryAction` parameter are deprecated, and will be removed in the future release.

## Release the v1.0.0-rc-1
We have reworked the driver to deliver the next set of changes:


* Added the `getTableNames` method to the `QldbDriver` class. For more details please
read the [release notes](https://github.com/awslabs/amazon-qldb-driver-dotnet/releases/tag/v1.0.0-rc.1).
* Support for .NET Core

### :hammer_and_wrench:​ Improvements
* Improve the performance of the driver by removing unnecessary calls to QLDB.


### :warning: Removed
* `PooledQldbDriver` has been removed in favor of the `QldbDriver`. For more
details please read the [release notes](https://github.com/awslabs/amazon-qldb-driver-dotnet/releases/tag/v1.0.0-rc.1).

* `QldbSession` and `Transaction` classes are not accesible anymore. Please use
`QldbDriver` instead to execute transactions. For more details please read 
the [release notes](https://github.com/awslabs/amazon-qldb-driver-dotnet/releases/tag/v1.0.0-rc.1).

* `QldbDriver.Execute(String)` method has been removed. Please use `QldbDriver.Execute<T>(Func<TransactionExecutor, T> func)` or 
`QldbDriver.Execute(Action<TransactionExecutor> action)` methods instead.

## [v0.1.0-beta](https://github.com/awslabs/amazon-qldb-driver-dotnet/releases/tag/v0.1.0-beta) - 2020-03-13 

Preview release of the Amazon QLDB Driver for .NET.
### :tada: Features 
- Provides the PooledQldbDriver to access QLDB and execute transactions.


