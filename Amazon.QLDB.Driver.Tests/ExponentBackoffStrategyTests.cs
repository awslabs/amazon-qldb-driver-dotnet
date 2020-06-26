
namespace Amazon.QLDB.Driver.Tests
{
    using System;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    class ExponentBackoffStrategyTests
    {
        [DataRow(1, 10)]
        [DataRow(2, 100)]
        [DataRow(3, 1000)]
        [DataRow(4, 5000)]
        [DataTestMethod]
        public void CalculateDelay_ForGivenRetryAttempts_ReturnExpectedDelay(int retryAttempted, int expectedMax)
        {
            var backoff = new ExponentBackoffStrategy();

            for (var i = 0; i < 1000; i++)
            {
                var delay = backoff.CalculateDelay(new RetryPolicyContext(retryAttempted, new Exception())).TotalMilliseconds;
                Assert.IsTrue(expectedMax >= delay && expectedMax * 0.5 <= delay, delay.ToString());
            }
        }

        [DataRow(1, 2)]
        [DataRow(2, 4)]
        [DataRow(3, 8)]
        [DataRow(4, 16)]
        [DataRow(5, 20)]
        [DataTestMethod]
        public void CalculateDelay_WithCustomizedSettingsForGivenRetryAttempts_ReturnExpectedDelay(int retryAttemped, int expectedMax)
        {   
            var backoff = new ExponentBackoffStrategy(2, 20);

            for (var i = 0; i < 1000; i++)
            {
                var delay = backoff.CalculateDelay(new RetryPolicyContext(retryAttemped, new Exception())).TotalMilliseconds;
                Assert.IsTrue(expectedMax >= delay && expectedMax * 0.5 <= delay, delay.ToString());
            }
        }

        [DataRow(-1, 8)]
        [DataRow(4, -6)]
        [DataRow(21, 20)]
        [DataTestMethod]
        public void Constructor_InvalidValues_ShouldThrow(int baseDelay, int delayCap)
        {
            Assert.ThrowsException<ArgumentException>(() => new ExponentBackoffStrategy(baseDelay, delayCap));
        }
    }
}
