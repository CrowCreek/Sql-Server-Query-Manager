/*
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE.txt', which is part of this source code package.
 */

using System;
using System.Data;

namespace CrowCreek.Utilities.SqlServerQueryManager
{
  public class QueryOptions
  {
    public string ConnectionString { get; set; }

    public TimeSpan? CommandTimeout { get; set; }

    public CommandType? CommandType { get; set; }
  }
}
