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
    using System.Collections.Generic;
    using IonDotnet.Tree;

    /// <summary>
    /// Interface for the result of executing a statement in QLDB.
    ///
    /// Implements IEnumerable to allow iteration over Ion values within the result.
    /// </summary>
    public interface IResult : IEnumerable<IIonValue>
    {
    }
}
