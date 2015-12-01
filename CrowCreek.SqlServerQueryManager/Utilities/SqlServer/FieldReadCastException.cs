/*
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE.txt', which is part of this source code package.
 */

using System;

namespace CrowCreek.Utilities.SqlServer
{
  public class FieldReadCastException : FieldReadException
  {
    public FieldReadCastException(string fieldName) : base(fieldName, $"Failed to cast [{fieldName}] could not be converted to value type")
    {
    }

    public FieldReadCastException(string fieldName, Exception innerException) 
      : base(fieldName, $"Failed to cast [{fieldName}] could not be converted to value type", innerException)
    {
    }

    protected FieldReadCastException(string fieldName, string message) : base(fieldName, message)
    {
    }
  }
}
