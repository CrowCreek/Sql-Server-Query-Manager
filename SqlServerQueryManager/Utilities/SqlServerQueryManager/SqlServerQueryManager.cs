/*
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE.txt', which is part of this source code package.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;

namespace CrowCreek.Utilities.SqlServerQueryManager
{
  public class SqlServerQueryManager
  {
    public static void Configure(string connectionString)
    {
      ConnectionString = connectionString;
    }

    public static void Configure(QueryOptions options)
    {
      ConnectionString = options.ConnectionString;
      if (options.CommandType.HasValue)
      {
        CommandType = options.CommandType.Value;
      }
      if (options.CommandTimeout.HasValue)
      {
        CommandTimeout = options.CommandTimeout.Value;
      }
    }

    #region Options

    private static string _connectionString;
    public static string ConnectionString
    {
      get
      {
        return _connectionString;
      }
      set
      {
        if (string.IsNullOrWhiteSpace(value))
        {
          throw new ArgumentException("Connection string cannot be empty.");
        }
        _connectionString = value;
      }
    }

    private static CommandType _commandType = CommandType.StoredProcedure;
    public static CommandType CommandType
    {
      get
      {
        return _commandType;
      }
      set
      {
        _commandType = value;
      }
    }

    private static TimeSpan _commandTimeout = TimeSpan.FromSeconds(30);
    public static TimeSpan CommandTimeout
    {
      get
      {
        return _commandTimeout;
      }
      set
      {
        if (value.TotalMilliseconds < 0)
        {
          throw new ArgumentException("Timeout must be positive.");
        }
        _commandTimeout = value;
      }
    }

    #endregion

    #region Sync Methods

    public static IEnumerable<TResult> SelectToObjects<TResult>(string query, Func<IDataRecord, TResult> rowMap, params SqlParameter[] queryParameters)
    {
      return ExecuteCommandFromDefaults(query, command => ExecuteCommandToObjects(command, rowMap), queryParameters);
    }

    public static TResult SelectToObject<TResult>(string query, Func<IDataRecord, TResult> rowMap, params SqlParameter[] queryParameters) where TResult : class
    {
      return ExecuteCommandFromDefaults(query, command => ExecuteCommandToObject(command, rowMap), queryParameters);
    }

    public static TResult SelectValueScalar<TResult>(string query, params SqlParameter[] queryParameters) where TResult : struct
    {
      return ExecuteCommandFromDefaults(query, ExecuteCommandToValueScalar<TResult>, queryParameters);
    }

    public static TResult SelectReferenceScalar<TResult>(string query, params SqlParameter[] queryParameters) where TResult : class
    {
      return ExecuteCommandFromDefaults(query, ExecuteCommandToReferenceScalar<TResult>, queryParameters);
    }

    public static void ExecuteNonQuery(string query, params SqlParameter[] queryParameters)
    {
      ExecuteCommandFromDefaults(query, command => command.ExecuteNonQuery(), queryParameters);
    }

    public static IEnumerable<TResult> SelectToObjects<TResult>(string query, Func<IDataRecord, TResult> rowMap, QueryOptions options, params SqlParameter[] queryParameters)
    {
      return ExecuteCommandFromOptions(query, command => ExecuteCommandToObjects(command, rowMap), options, queryParameters);
    }

    public static TResult SelectToObject<TResult>(string query, Func<IDataRecord, TResult> rowMap, QueryOptions options, params SqlParameter[] queryParameters) where TResult : class
    {
      return ExecuteCommandFromOptions(query, command => ExecuteCommandToObject(command, rowMap), options, queryParameters);
    }

    public static TResult SelectValueScalar<TResult>(string query, QueryOptions options, params SqlParameter[] queryParameters) where TResult : struct
    {
      return ExecuteCommandFromOptions(query, command => ExecuteCommandToValueScalar<TResult>(command), options, queryParameters);
    }

    public static TResult SelectReferenceScalar<TResult>(string query, QueryOptions options, params SqlParameter[] queryParameters) where TResult : class
    {
      return ExecuteCommandFromOptions(query, command => ExecuteCommandToReferenceScalar<TResult>(command), options, queryParameters);
    }

    public static void ExecuteNonQuery(string query, QueryOptions options, params SqlParameter[] queryParameters)
    {
      ExecuteCommandFromOptions(query, command => command.ExecuteNonQuery(), options, queryParameters);
    }

    #endregion

    #region Implementation

    private static IEnumerable<TResult> ExecuteCommandToObjects<TResult>(SqlCommand command, Func<IDataRecord, TResult> rowMap)
    {
      var resultList = new List<TResult>();
      using (var reader = command.ExecuteReader())
      {
        while (reader.Read())
        {
          resultList.Add(rowMap(reader));
        }
      }
      return resultList;
    }

    private static TResult ExecuteCommandToObject<TResult>(SqlCommand command, Func<IDataRecord, TResult> rowMap) where TResult : class
    {
      using (var reader = command.ExecuteReader())
      {
        if (reader.Read())
        {
          return rowMap(reader);
        }
        return null;
      }
    }

    private static TResult ExecuteCommandToValueScalar<TResult>(SqlCommand command) where TResult : struct
    {
      var rawResult = command.ExecuteScalar();
      try
      {
        return (TResult)rawResult;
      }
      catch (Exception ex)
      {
        throw new FieldReadCastException("Failed to cast execute scalar result", ex);
      }
    }

    private static TResult ExecuteCommandToReferenceScalar<TResult>(SqlCommand command) where TResult : class
    {
      return command.ExecuteScalar() as TResult;
    }

    private static TResult ExecuteCommandFromDefaults<TResult>(string query, Func<SqlCommand, TResult> commandAction,
      params SqlParameter[] queryParameters)
    {
      return ExecuteCommand(query, commandAction, ConnectionString, CommandType, CommandTimeout, queryParameters);
    }

    private static TResult ExecuteCommandFromOptions<TResult>(string query, Func<SqlCommand, TResult> commandAction,
      QueryOptions options, params SqlParameter[] queryParameters)
    {
      return ExecuteCommand(query, commandAction, options.ConnectionString ?? ConnectionString, 
        options.CommandType.HasValue ? options.CommandType.Value : CommandType,
        options.CommandTimeout.HasValue ? options.CommandTimeout.Value : CommandTimeout, 
        queryParameters);
    }

    private static TResult ExecuteCommand<TResult>(string query, Func<SqlCommand, TResult> commandAction, 
      string connectionString, CommandType commandType, TimeSpan commandTimeout, params SqlParameter[] queryParameters)
    {
      using (var connection = new SqlConnection(ConnectionString))
      {
        connection.Open();
        using (var command = new SqlCommand(query, connection))
        {
          command.CommandType = CommandType;
          command.CommandTimeout = (int)CommandTimeout.TotalSeconds;
          return commandAction(command);
        }
      }
    }

    private static void ExecuteCommandFromDefaults(string query, Action<SqlCommand> commandAction, 
      params SqlParameter[] queryParameters)
    {
      ExecuteCommand(query, commandAction, ConnectionString, CommandType, CommandTimeout, queryParameters);
    }

    private static void ExecuteCommandFromOptions<TResult>(string query, Action<SqlCommand> commandAction,
      QueryOptions options, params SqlParameter[] queryParameters)
    {
      ExecuteCommand(query, commandAction, options.ConnectionString ?? ConnectionString,
        options.CommandType.HasValue ? options.CommandType.Value : CommandType,
        options.CommandTimeout.HasValue ? options.CommandTimeout.Value : CommandTimeout,
        queryParameters);
    }

    private static void ExecuteCommand(string query, Action<SqlCommand> commandAction,
      string connectionString, CommandType commandType, TimeSpan commandTimeout, params SqlParameter[] queryParameters)
    {
      using (var connection = new SqlConnection(ConnectionString))
      {
        connection.Open();
        using (var command = new SqlCommand(query, connection))
        {
          command.CommandType = CommandType;
          command.CommandTimeout = (int)CommandTimeout.TotalSeconds;
          commandAction(command);
        }
      }
    }

    #endregion
  }
}