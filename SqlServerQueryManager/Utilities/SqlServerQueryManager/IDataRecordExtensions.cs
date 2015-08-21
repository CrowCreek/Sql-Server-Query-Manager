/*
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE.txt', which is part of this source code package.
 */

using System;
using System.Data;

namespace CrowCreek.Utilities.SqlServerQueryManager
{
  public static class IDataRecordExtensions
  {
    public static TFieldType ReadReferenceFeild<TFieldType>(this IDataRecord dataRecord, string fieldName) where TFieldType : class
    {
      object raw;
      try
      {
        raw = dataRecord[fieldName];
      }
      catch (Exception ex)
      {
        throw new FieldReadException(fieldName, ex);
      }
      if (raw == DBNull.Value)
      {
        return null as TFieldType;
      }
      try
      {
        return raw as TFieldType;
      }
      catch (InvalidCastException ex)
      {
        throw new FieldReadCastException(fieldName, ex);
      }
      catch (Exception ex)
      {
        throw new FieldReadException(fieldName, ex);
      }
    }

    public static TFieldType ReadValueFeild<TFieldType>(this IDataRecord dataRecord, string fieldName) where TFieldType : struct
    {
      object raw;
      try
      {
        raw = dataRecord[fieldName];
      }
      catch (Exception ex)
      {
        throw new FieldReadException(fieldName, ex);
      }
      if (raw == DBNull.Value)
      {
        throw new FieldReadNullValueTypeException(fieldName);
      }
      try
      {
        return (TFieldType)raw;
      }
      catch (InvalidCastException ex)
      {
        throw new FieldReadCastException(fieldName, ex);
      }
      catch (Exception ex)
      {
        throw new FieldReadException(fieldName, ex);
      }
    }
  }
}
