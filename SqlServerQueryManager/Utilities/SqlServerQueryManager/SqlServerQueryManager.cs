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

    public static CommandType CommandType { get; set; } = CommandType.StoredProcedure;

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

    /// <summary>
    /// Executes <paramref name="query"/> with parameters <paramref name="queryParameters"/> that returns a single 
    /// SELECT result and convert each row to an object of type <typeparamref name="TResult"/> using the 
    /// <paramref name="rowMap"/> function. The current options set on  will be used.
    /// </summary>
    /// <typeparam name="TResult">Type of the individual result items returned.</typeparam>
    /// <param name="query">Query to execute. This must be a query with single SELECT result.</param>
    /// <param name="rowMap">
    /// Function that takes an arguement of type IDataRecord and returns <typeparamref name="TResult"/>.
    /// </param>
    /// <param name="queryParameters">
    /// Optional list of SqlParameters. Any parameters with null values will have the value replaced with DBNull.Value.
    /// </param>
    /// <returns>List of <typeparamref name="TResult"/></returns>
    public static IList<TResult> SelectToObjects<TResult>(string query, Func<IDataRecord, TResult> rowMap, 
      params SqlParameter[] queryParameters)
    {
      return ExecuteCommandFromDefaults(query, command => ExecuteCommandToObjects(command, rowMap), queryParameters);
    }

    /// <summary>
    /// Executes <paramref name="query"/> with parameters <paramref name="queryParameters"/> that returns a single 
    /// SELECT result with a single row and converts the row to an object of type <typeparamref name="TResult"/> 
    /// using the <paramref name="rowMap"/> function. The current options set on  will be used.
    /// </summary>
    /// <typeparam name="TResult">Type of the individual result items returned.</typeparam>
    /// <param name="query">Query to execute. This must be a query with single SELECT result with a single row.</param>
    /// <param name="rowMap">
    /// Function that takes an arguement of type IDataRecord and returns <typeparamref name="TResult"/>.
    /// </param>
    /// <param name="queryParameters">
    /// Optional list of SqlParameters. Any parameters with null values will have the value replaced with DBNull.Value.
    /// </param>
    /// <returns>Single <typeparamref name="TResult"/> or null if no rows returned.</returns>
    public static TResult SelectToObject<TResult>(string query, Func<IDataRecord, TResult> rowMap, params SqlParameter[] queryParameters) where TResult : class
    {
      return ExecuteCommandFromDefaults(query, command => ExecuteCommandToObject(command, rowMap), queryParameters);
    }

    /// <summary>
    /// Executes <paramref name="query"/> with parameters <paramref name="queryParameters"/> that returns a single 
    /// SELECT result with a single row with a single column and casts the value of type <typeparamref name="TResult"/>. 
    /// The current options set on  will be used.
    /// </summary>
    /// <typeparam name="TResult">Type of value to be returned. Must be a value type.</typeparam>
    /// <param name="query">
    /// Query to execute. This must be a query with single SELECT result with a single row and a single column.
    /// </param>
    /// <param name="queryParameters">
    /// Optional list of SqlParameters. Any parameters with null values will have the value replaced with DBNull.Value.
    /// </param>
    /// <returns>Value of type <typeparamref name="TResult"/> or default if null or no rows returned.</returns>
    public static TResult SelectValueScalar<TResult>(string query, params SqlParameter[] queryParameters) where TResult : struct
    {
      return ExecuteCommandFromDefaults(query, ExecuteCommandToValueScalar<TResult>, queryParameters);
    }

    /// <summary>
    /// Executes <paramref name="query"/> with parameters <paramref name="queryParameters"/> that returns a single 
    /// SELECT result with a single row with a single column and casts the value of type <typeparamref name="TResult"/>. 
    /// The current options set on  will be used.
    /// </summary>
    /// <typeparam name="TResult">Type of value to be returned. Must be a value type.</typeparam>
    /// <param name="query">
    /// Query to execute. This must be a query with single SELECT result with a single row and a single column.
    /// </param>
    /// <param name="queryParameters">
    /// Optional list of SqlParameters. Any parameters with null values will have the value replaced with DBNull.Value.
    /// </param>
    /// <returns>Value of type <typeparamref name="TResult"/> or default if null or no rows returned.</returns>
    public static TResult SelectReferenceScalar<TResult>(string query, params SqlParameter[] queryParameters) where TResult : class
    {
      return ExecuteCommandFromDefaults(query, ExecuteCommandToReferenceScalar<TResult>, queryParameters);
    }

    /// <summary>
    /// Executes <paramref name="query"/> with parameters <paramref name="queryParameters"/> that returns no data. 
    /// The current options set on  will be used.
    /// </summary>
    /// <param name="query">Query to execute. Any data returned will be ignored.</param>
    /// <param name="queryParameters">
    /// Optional list of SqlParameters. Any parameters with null values will have the value replaced with DBNull.Value.
    /// </param>
    public static void ExecuteNonQuery(string query, params SqlParameter[] queryParameters)
    {
      ExecuteCommandFromDefaults(query, command => command.ExecuteNonQuery(), queryParameters);
    }

    /// <summary>
    /// Executes <paramref name="query"/> with parameters <paramref name="queryParameters"/> that returns a single 
    /// SELECT result and convert each row to an object of type <typeparamref name="TResult"/> using the 
    /// <paramref name="rowMap"/> function. Any options passed in <paramref name="options"/> will overide values 
    /// current options for this execution only.
    /// </summary>
    /// <typeparam name="TResult">Type of the individual result items returned.</typeparam>
    /// <param name="query">Query to execute. This must be a query with single SELECT result.</param>
    /// <param name="rowMap">
    /// Function that takes an arguement of type IDataRecord and returns <typeparamref name="TResult"/>.
    /// </param>
    /// <param name="options">
    /// Container for execution options that used to override the current options for this execution.
    /// </param>
    /// <param name="queryParameters">
    /// Optional list of SqlParameters. Any parameters with null values will have the value replaced with DBNull.Value.
    /// </param>
    /// <returns>List of <typeparamref name="TResult"/></returns>
    public static IList<TResult> SelectToObjects<TResult>(string query, Func<IDataRecord, TResult> rowMap, QueryOptions options, params SqlParameter[] queryParameters)
    {
      return ExecuteCommandFromOptions(query, command => ExecuteCommandToObjects(command, rowMap), options, queryParameters);
    }

    /// <summary>
    /// Executes <paramref name="query"/> with parameters <paramref name="queryParameters"/> that returns a single 
    /// SELECT result with a single row and converts the row to an object of type <typeparamref name="TResult"/> 
    /// using the <paramref name="rowMap"/> function. Any options passed in <paramref name="options"/> will 
    /// overide values current options for this execution only.
    /// </summary>
    /// <typeparam name="TResult">Type of the individual result items returned.</typeparam>
    /// <param name="query">
    /// Query to execute. This must be a query with single SELECT result with a single row.
    /// </param>
    /// <param name="rowMap">
    /// Function that takes an arguement of type IDataRecord and returns <typeparamref name="TResult"/>.
    /// </param>
    /// <param name="options">
    /// Container for execution options that used to override the current options for this execution.
    /// </param>
    /// <param name="queryParameters">
    /// Optional list of SqlParameters. Any parameters with null values will have the value replaced with DBNull.Value.
    /// </param>
    /// <returns>Single <typeparamref name="TResult"/> or null if no rows returned.</returns>
    public static TResult SelectToObject<TResult>(string query, Func<IDataRecord, TResult> rowMap, QueryOptions options, params SqlParameter[] queryParameters) where TResult : class
    {
      return ExecuteCommandFromOptions(query, command => ExecuteCommandToObject(command, rowMap), options, queryParameters);
    }

    /// <summary>
    /// Executes <paramref name="query"/> with parameters <paramref name="queryParameters"/> that returns a single 
    /// SELECT result with a single row with a single column and casts the value of type <typeparamref name="TResult"/>. 
    /// Any options passed in <paramref name="options"/> will overide values current options for this execution only.
    /// </summary>
    /// <typeparam name="TResult">Type of value to be returned. Must be a reference type.</typeparam>
    /// <param name="query">
    /// Query to execute. This must be a query with single SELECT result with a single row and a single column.
    /// </param>
    /// <param name="options">
    /// Container for execution options that used to override the current options for this execution.
    /// </param>
    /// <param name="queryParameters">Optional list of SqlParameters. Any parameters with null values will have the 
    /// value replaced with DBNull.Value.</param>
    /// <returns>
    /// Value of type <typeparamref name="TResult"/> or default if null or no rows returned.
    /// </returns>
    public static TResult SelectValueScalar<TResult>(string query, QueryOptions options, params SqlParameter[] queryParameters) where TResult : struct
    {
      return ExecuteCommandFromOptions(query, ExecuteCommandToValueScalar<TResult>, options, queryParameters);
    }

    /// <summary>
    /// Executes <paramref name="query"/> with parameters <paramref name="queryParameters"/> that returns a single SELECT 
    /// result with a single row with a single column and casts the value of type <typeparamref name="TResult"/>. 
    /// Any options passed in <paramref name="options"/> will overide values current options for this execution only.
    /// </summary>
    /// <typeparam name="TResult">Type of value to be returned. Must be a reference type.</typeparam>
    /// <param name="query">
    /// Query to execute. This must be a query with single SELECT result with a single row and a single column.
    /// </param>
    /// <param name="options">
    /// Container for execution options that used to override the current options for this execution.
    /// </param>
    /// <param name="queryParameters">Optional list of SqlParameters. 
    /// Any parameters with null values will have the value replaced with DBNull.Value.</param>
    /// <returns>
    /// Value of type <typeparamref name="TResult"/> or default if null or no rows returned.
    /// </returns>
    public static TResult SelectReferenceScalar<TResult>(string query, QueryOptions options, params SqlParameter[] queryParameters) where TResult : class
    {
      return ExecuteCommandFromOptions(query, ExecuteCommandToReferenceScalar<TResult>, options, queryParameters);
    }

    /// <summary>
    /// Executes <paramref name="query"/> with parameters <paramref name="queryParameters"/> that returns no data. 
    /// Any options passed in <paramref name="options"/> will overide values current options for this execution only.
    /// </summary>
    /// <param name="query">Query to execute. Any data returned will be ignored.</param>
    /// <param name="options">
    /// Container for execution options that used to override the current options for this execution.
    /// </param>
    /// <param name="queryParameters">
    /// Optional list of SqlParameters. Any parameters with null values will have the value replaced with DBNull.Value.
    /// </param>
    public static void ExecuteNonQuery(string query, QueryOptions options, params SqlParameter[] queryParameters)
    {
      ExecuteCommandFromOptions(query, command => command.ExecuteNonQuery(), options, queryParameters);
    }

    #endregion

    #region Implementation

    private static IList<TResult> ExecuteCommandToObjects<TResult>(SqlCommand command, Func<IDataRecord, TResult> rowMap)
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
        options.CommandType ?? CommandType,
        options.CommandTimeout ?? CommandTimeout, 
        queryParameters);
    }

    private static TResult ExecuteCommand<TResult>(string query, Func<SqlCommand, TResult> commandAction, 
      string connectionString, CommandType commandType, TimeSpan commandTimeout, params SqlParameter[] queryParameters)
    {
      using (var connection = new SqlConnection(connectionString))
      {
        connection.Open();
        using (var command = new SqlCommand(query, connection))
        {
          command.CommandType = commandType;
          command.CommandTimeout = (int)commandTimeout.TotalSeconds;
          queryParameters = SubstituteDbNullForNullParameterValues(queryParameters);
          command.Parameters.AddRange(queryParameters);
          return commandAction(command);
        }
      }
    }

    private static void ExecuteCommandFromDefaults(string query, Action<SqlCommand> commandAction, 
      params SqlParameter[] queryParameters)
    {
      ExecuteCommand(query, commandAction, ConnectionString, CommandType, CommandTimeout, queryParameters);
    }

    private static void ExecuteCommand(string query, Action<SqlCommand> commandAction,
      string connectionString, CommandType commandType, TimeSpan commandTimeout, params SqlParameter[] queryParameters)
    {
      using (var connection = new SqlConnection(connectionString))
      {
        connection.Open();
        using (var command = new SqlCommand(query, connection))
        {
          command.CommandType = commandType;
          command.CommandTimeout = (int)commandTimeout.TotalSeconds;
          queryParameters = SubstituteDbNullForNullParameterValues(queryParameters);
          command.Parameters.AddRange(queryParameters);
          commandAction(command);
        }
      }
    }

    private static SqlParameter[] SubstituteDbNullForNullParameterValues(SqlParameter[] queryParameters)
    {
      foreach (var queryParameter in queryParameters)
      {
        if (queryParameter.Value == null)
        {
          queryParameter.Value = DBNull.Value;
        }
      }
      return queryParameters;
    }

    #endregion
  }
}