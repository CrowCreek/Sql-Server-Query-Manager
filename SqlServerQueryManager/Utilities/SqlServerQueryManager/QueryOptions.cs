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
    #region Options
    private string _connectionString;
    public string ConnectionString
    {
      get
      {
        return _connectionString;
      }
      private set
      {
        if (string.IsNullOrWhiteSpace(value))
        {
          throw new ArgumentException("Connection string cannot be empty.");
        }
        _connectionString = value;
      }
    }

    private TimeSpan _commandTimeout = TimeSpan.FromSeconds(30);
    public TimeSpan CommandTimeout
    {
      get
      {
        return _commandTimeout;
      }
      private set
      {
        if (value.TotalMilliseconds < 0)
        {
          throw new ArgumentException("Timeout must be positive.");
        }
        _commandTimeout = value;
      }
    }

    public CommandType CommandType { get; private set; } = System.Data.CommandType.StoredProcedure;
    #endregion

    #region .ctor
    public QueryOptions(string connectionString)
    {
      ConnectionString = connectionString;
    }

    public QueryOptions(string connectionString, CommandType commandType, TimeSpan commandTimeout)
    {
      ConnectionString = connectionString;
      CommandType = commandType;
      CommandTimeout = commandTimeout;
    }
    #endregion

    #region Default
    private static readonly object _defaultOptionsLock = new object();
    private static QueryOptions _defaultOptions = null;
    /// <summary>
    /// Provides access to a static of default query options. 
    /// Before using the <see cref="Default"/> property you must call <see cref="ConfigureDefault(string)"/> with a connection string or <see cref="ConfigureDefault(QueryOptions)"/> with an entire set of options.
    /// </summary>
    /// <exception cref="InvalidOperationException">Default has not been configured with a connection string.</exception>
    public static QueryOptions Default
    {
      get
      {
        if (_defaultOptions == null) throw new InvalidOperationException("Default connection string has not been initialized.");
        lock (_defaultOptionsLock)
        {
          return _defaultOptions;
        }
      }
    }

    /// <summary>
    /// Configures the static default query options with a connection string.
    /// You must call <see cref="ConfigureDefault(string)"/> or <see cref="ConfigureDefault(QueryOptions)"/> calling <see cref="Default"/>
    /// </summary>
    /// <param name="connectionString"></param>
    /// <exception cref="ArgumentException">ConnectionString is null or empty.</exception>
    public static void ConfigureDefault(string connectionString)
    {
      if (String.IsNullOrWhiteSpace(connectionString)) throw new ArgumentException("Connection string cannot be empty.");
      lock (_defaultOptionsLock)
      {
        _defaultOptions = new QueryOptions(connectionString);
      }
    }

    /// <summary>
    /// Configures the static default query options with a connection string.
    /// You must call <see cref="ConfigureDefault(string)"/> or <see cref="ConfigureDefault(QueryOptions)"/> calling <see cref="Default"/>
    /// </summary>
    /// <param name="connectionString"></param>
    /// /// <exception cref="ArgumentException">Options.ConnectionString is null or empty.</exception>
    public static void ConfigureDefault(QueryOptions options)
    {
      if (string.IsNullOrWhiteSpace(options.ConnectionString)) throw new ArgumentException("Connection string cannot be empty.");
      lock (_defaultOptionsLock)
      {
        _defaultOptions = options;
      }
    }
    #endregion 
  }
}
