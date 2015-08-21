/*
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE.txt', which is part of this source code package.
 */

namespace CrowCreek.Utilities.SqlServerQueryManager
{
  public class FieldReadNullValueTypeException : FieldReadCastException
  {
    public FieldReadNullValueTypeException(string fieldName) 
      : base(fieldName, $"Null value in field [{fieldName}] could not be converted to value type")
    {
    }
  }
}
