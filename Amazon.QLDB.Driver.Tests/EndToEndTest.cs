namespace Amazon.QLDB.Driver.Tests
{
    using System.Diagnostics;
    using Amazon.QLDBSession;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class EndToEndTest
    {
        // TODO: Remove
        // Currently for dev purposes only
        [TestMethod]
        [Ignore]
        public void EndToEnd()
        {
            var builder = PooledQldbDriver.Builder();
            var driver = builder
                .WithLedger("vehicle-registration")
                .Build();
            var tableNames = driver.ListTableNames();
            foreach (var table in tableNames)
            {
                Debug.WriteLine(table);
            }
        }
    }
}
