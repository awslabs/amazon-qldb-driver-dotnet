/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
    using System.Collections.Generic;
    using System.Linq;
    using System.Security.Cryptography;
    using Amazon.IonDotnet;
    using Amazon.IonDotnet.Builders;
    using Amazon.IonDotnet.Tree;
    using Amazon.IonDotnet.Tree.Impl;
    using Amazon.IonHashDotnet;
    using Amazon.QLDBSession.Model;

    /// <summary>
    /// A QLDB hash is either a 256 bit number or a special empty hash.
    /// </summary>
    internal class QldbHash
    {
        private const int HashSize = 32;
        private static readonly IIonHasherProvider HasherProvider = new CryptoIonHasherProvider("SHA256");
        private static readonly IValueFactory ValueFactory = new ValueFactory();

        /// <summary>
        /// Initializes a new instance of the <see cref="QldbHash"/> class.
        /// </summary>
        ///
        /// <param name="qldbHash">Byte array of hash code.</param>
        internal QldbHash(byte[] qldbHash)
        {
            if (qldbHash == null || !(qldbHash.Length == HashSize || qldbHash.Length == 0))
            {
                throw new ArgumentException($"Hashes must either be empty or {HashSize} bytes long");
            }

            this.Hash = qldbHash;
        }

        /// <summary>
        /// Gets hash codes.
        /// </summary>
        internal byte[] Hash { get; private set; }

        /// <inheritdoc/>
        public override string ToString()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// The QldbHash of an IonValue is just the IonHash of that value.
        /// </summary>
        ///
        /// <param name="value">Value to be hashed.</param>
        ///
        /// <returns>Hashed result.</returns>
        internal static QldbHash ToQldbHash(string value)
        {
            return ToQldbHash(ValueFactory.NewString(value ?? string.Empty));
        }

        /// <summary>
        /// The QldbHash of an IonValue is just the IonHash of that value.
        /// </summary>
        ///
        /// <param name="value">Ion Value to be hashed.</param>
        ///
        /// <returns>Hashed result.</returns>
        internal static QldbHash ToQldbHash(IIonValue value)
        {
            return ToQldbHash(IonReaderBuilder.Build(value));
        }

        internal static QldbHash ToQldbHash(ValueHolder value)
        {
            return ToQldbHash(IonReaderBuilder.Build(value.IonBinary));
        }

        internal static QldbHash ToQldbHash(IIonReader reader)
        {
            IIonHashReader hashReader = IonHashReaderBuilder.Standard()
                .WithHasherProvider(HasherProvider)
                .WithReader(reader)
                .Build();
            while (hashReader.MoveNext() != IonType.None)
            {
            }

            return new QldbHash(hashReader.Digest());
        }

        /// <summary>
        /// Calculates the QLDB hash.
        /// </summary>
        ///
        /// <param name="that">Hashed PartiQL statement.</param>
        ///
        /// <returns>Returns a QldbHash instance.</returns>
        internal QldbHash Dot(QldbHash that)
        {
            byte[] concatenated = JoinHashesPairwise(this.Hash, that.Hash);
            HashAlgorithm hashAlgorithm = HashAlgorithm.Create("SHA256");
            return new QldbHash(hashAlgorithm.ComputeHash(concatenated));
        }

        /// <summary>
        /// Takes two hashes, sorts them, and concatenates them.
        /// </summary>
        ///
        /// <param name="h1">Hash codes of the first hashed PartiQL statement.</param>
        /// <param name="h2">Hash codes of the second hashed PartiQL statement.</param>
        ///
        /// <returns>No object or value is returned by this method when it completes.</returns>
        private static byte[] JoinHashesPairwise(byte[] h1, byte[] h2)
        {
            if (h1.Length == 0)
            {
                return h2;
            }

            if (h2.Length == 0)
            {
                return h1;
            }

            HashComparer comparer = new HashComparer();
            if (comparer.Compare(h1, h2) < 0)
            {
                return h1.Concat(h2).ToArray();
            }
            else
            {
                return h2.Concat(h1).ToArray();
            }
        }

        private class HashComparer : IComparer<byte[]>
        {
            public int Compare(byte[] h1, byte[] h2)
            {
                if (h1.Length != HashSize || h2.Length != HashSize)
                {
                    throw new ArgumentException("Invalid hash");
                }

                for (var i = h1.Length - 1; i >= 0; i--)
                {
                    var byteEqual = (sbyte)h1[i] - (sbyte)h2[i];
                    if (byteEqual != 0)
                    {
                        return byteEqual;
                    }
                }

                return 0;
            }
        }
    }
}
