﻿/*
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

    internal static class ValidationUtils
    {
        internal static void AssertStringNotEmpty(string str, string fieldName)
        {
            if (string.IsNullOrEmpty(str))
            {
                throw new ArgumentException("Parameter must not be null or empty.", fieldName);
            }
        }

        internal static void AssertNotNegative(int num, string fieldName)
        {
            if (num < 0)
            {
                throw new ArgumentException("Parameter must not be negative.", fieldName);
            }
        }

        internal static void AssertNotNull(object obj, string fieldName)
        {
            if (obj == null)
            {
                throw new ArgumentException("Parameter must not be null.", fieldName);
            }
        }
    }
}
