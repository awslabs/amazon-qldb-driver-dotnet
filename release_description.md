Release of QLDB .NET driver 1.2.0

### Release v1.2.0

* Add the asynchronous version of the driver.
* Improved retry logic to now handle more types of failure.
* Fixed some resource leaks upon calling `Dispose` on the driver.
* Fixed a rare race condition when calling `Execute` on the driver.
* Added a safeguard to release resources when user-passed functions threw exceptions in `retryAction` and `BackoffPolicy`.
