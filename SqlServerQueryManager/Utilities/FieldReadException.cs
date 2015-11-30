/*
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE.txt', which is part of this source code package.
 */

using System;

namespace CrowCreek.Utilities
{
  public class FieldReadException : Exception
  {
    public FieldReadException(string fieldName, Exception innerException)
      : base($"Failed to cast [{fieldName}] could not be converted to value type", innerException)
    {
      FieldName = fieldName;
    }

    public FieldReadException(string fieldName, string message, Exception innerException)
      : base(message, innerException)
    {
      FieldName = fieldName;
    }

    protected FieldReadException(string fieldName, string message) : base(message)
    {
      FieldName = fieldName;
    }

    public string FieldName { get; private set; }
  }
}
