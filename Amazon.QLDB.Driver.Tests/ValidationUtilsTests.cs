/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with
 * the License. A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

namespace Amazon.QLDB.Driver.Tests
{
    using System;
    using System.Collections.Generic;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class ValidationUtilsTests
    {
        [TestMethod]
        public void TestAssertStringNotEmpty()
        {
            ValidationUtils.AssertStringNotEmpty("foo", "foo");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestAssertStringNotEmptyWhenEmpty()
        {
            ValidationUtils.AssertStringNotEmpty("", "foo");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestAssertStringNotEmptyWhenNull()
        {
            ValidationUtils.AssertStringNotEmpty(null, "foo");
        }

        [TestMethod]
        public void TestAssertNotNegative()
        {
            ValidationUtils.AssertNotNegative(64, "foo");
            ValidationUtils.AssertNotNegative(0, "foo");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestAssertNotNegativeWhenNegative()
        {
            ValidationUtils.AssertNotNegative(-1, "foo");
        }

        [TestMethod]
        public void TestAssertNotNull()
        {
            ValidationUtils.AssertNotNull("foo", "foo");
            ValidationUtils.AssertNotNull("", "foo");
            ValidationUtils.AssertNotNull(1, "foo");
            ValidationUtils.AssertNotNull(0, "foo");
            ValidationUtils.AssertNotNull(true, "foo");
            ValidationUtils.AssertNotNull(false, "foo");
            ValidationUtils.AssertNotNull(new List<string>(), "foo");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestAssertNotNullWhenNull()
        {
            ValidationUtils.AssertNotNull(null, "foo");
        }
    }
}
