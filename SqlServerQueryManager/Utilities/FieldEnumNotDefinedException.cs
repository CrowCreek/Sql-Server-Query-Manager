using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CrowCreek.Utilities
{
  public class FieldEnumNotDefinedException : FieldReadException
  {
    public FieldEnumNotDefinedException(string fieldName, Type enumType, string value)
      : base(fieldName, string.Format("Failed to read [{0}], [{1}] is not defined in enumeration {2}", fieldName, value, enumType.Name))
    {
    }

    public FieldEnumNotDefinedException(string fieldName, Type enumType, string value, Exception innerException)
      : base(fieldName, string.Format("Failed to read [{0}], [{1}] is not defined in enumeration {2}", fieldName, value, enumType.Name), innerException)
    {
    }

    protected FieldEnumNotDefinedException(string fieldName, Type enumType, string value, string message)
      : base(fieldName, message)
    {
    }
  }
}
