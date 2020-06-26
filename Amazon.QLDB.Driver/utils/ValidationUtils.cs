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

namespace Amazon.QLDB.Driver
{
    using System;

    /// <summary>
    /// Static utility methods to use for basic validations.
    /// </summary>
    internal static class ValidationUtils
    {
        /// <summary>
        /// Validates that the text is not Null nor empty.
        /// </summary>
        ///
        /// <param name="str">The string to validate.</param>
        /// <param name="fieldName">Name of the parameter.</param>
        internal static void AssertStringNotEmpty(string str, string fieldName)
        {
            if (string.IsNullOrEmpty(str))
            {
                throw new ArgumentException("Parameter must not be null or empty.", fieldName);
            }
        }

        /// <summary>
        /// Validates that the integer is not negative.
        /// </summary>
        ///
        /// <param name="num">The integer to validate.</param>
        /// <param name="fieldName">Name of the parameter.</param>
        internal static void AssertNotNegative(int num, string fieldName)
        {
            if (num < 0)
            {
                throw new ArgumentException("Parameter must not be negative.", fieldName);
            }
        }

        /// <summary>
        /// Validates that the integer is positive.
        /// </summary>
        ///
        /// <param name="num">The integer to validate.</param>
        /// <param name="fieldName">Name of the parameter.</param>
        internal static void AssertPositive(int num, string fieldName)
        {
            if (num < 0)
            {
                throw new ArgumentException("Parameter must not be zero or negative.", fieldName);
            }
        }

        /// <summary>
        /// Validates that the left integer is not greater than the right integer.
        /// </summary>
        /// <param name="left">The left integer.</param>
        /// <param name="right">The right integer.</param>
        /// <param name="leftName">Name of the left integer.</param>
        /// <param name="rightName">Name of the right integer.</param>
        internal static void AssertNotGreater(int left, int right, string leftName, string rightName)
        {
            if (left > right)
            {
                throw new ArgumentException(string.Format("Parameter {0} cannot be greater than {1}.", leftName, rightName), leftName);
            }
        }

        /// <summary>
        /// Validates that the input is not Null.
        /// </summary>
        ///
        /// <param name="obj">The object to validate.</param>
        /// <param name="fieldName">Name of the parameter.</param>
        internal static void AssertNotNull(object obj, string fieldName)
        {
            if (obj == null)
            {
                throw new ArgumentException("Parameter must not be null.", fieldName);
            }
        }
    }
}
