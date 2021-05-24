using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;
using Amazon.DynamoDBv2.DataModel;

namespace AspectKinesisLamda.Tests
{
    internal class RecordHelper
    {
        internal static string RecordKey<T>(T record)
        {
            Type t = record.GetType();
            foreach (var prop in t.GetProperties(BindingFlags.Instance | BindingFlags.Public))
            {
                // if this type has a property marked as the DynamoDBHashKey, use that property value
                if (Attribute.IsDefined(prop, typeof(DynamoDBHashKeyAttribute)))
                    return prop.GetValue(record).ToString();
            }

            return null;
        }
    }
}
