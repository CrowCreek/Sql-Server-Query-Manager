/*
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE.txt', which is part of this source code package.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;

namespace CrowCreek.Utilities.SqlServer
{
  public class SqlServerQueryManager
  {
    #region Sync Methods
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
    /// Container for execution options that used to override the current options for this execution.<para>To pass default options use <see cref="QueryOptions.Default"/></para>
    /// </param>
    /// <param name="queryParameters">
    /// Optional list of SqlParameters. Any parameters with null values will have the value replaced with DBNull.Value.
    /// </param>
    /// <returns>List of <typeparamref name="TResult"/></returns>
    public static IList<TResult> SelectToObjects<TResult>(string query, Func<DbDataReader, TResult> rowMap, QueryOptions options, params SqlParameter[] queryParameters)
    {
      return ExecuteCommand(query, command => ExecuteCommandToObjects(command, rowMap), options, queryParameters);
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
    /// Container for execution options that used to override the current options for this execution.<para>To pass default options use <see cref="QueryOptions.Default"/></para>
    /// </param>
    /// <param name="queryParameters">
    /// Optional list of SqlParameters. Any parameters with null values will have the value replaced with DBNull.Value.
    /// </param>
    /// <returns>Single <typeparamref name="TResult"/> or null if no rows returned.</returns>
    public static TResult SelectToObject<TResult>(string query, Func<DbDataReader, TResult> rowMap, QueryOptions options, params SqlParameter[] queryParameters) where TResult : class
    {
      return ExecuteCommand(query, command => ExecuteCommandToObject(command, rowMap), options, queryParameters);
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
    /// Container for execution options that used to override the current options for this execution.<para>To pass default options use <see cref="QueryOptions.Default"/></para>
    /// </param>
    /// <param name="queryParameters">Optional list of SqlParameters. Any parameters with null values will have the 
    /// value replaced with DBNull.Value.</param>
    /// <returns>
    /// Value of type <typeparamref name="TResult"/> or default if null or no rows returned.
    /// </returns>
    public static TResult SelectValueScalar<TResult>(string query, QueryOptions options, params SqlParameter[] queryParameters) where TResult : struct
    {
      return ExecuteCommand(query, ExecuteCommandToValueScalar<TResult>, options, queryParameters);
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
    /// Container for execution options that used to override the current options for this execution.<para>To pass default options use <see cref="QueryOptions.Default"/></para>
    /// </param>
    /// <param name="queryParameters">Optional list of SqlParameters. 
    /// Any parameters with null values will have the value replaced with DBNull.Value.</param>
    /// <returns>
    /// Value of type <typeparamref name="TResult"/> or default if null or no rows returned.
    /// </returns>
    public static TResult SelectReferenceScalar<TResult>(string query, QueryOptions options, params SqlParameter[] queryParameters) where TResult : class
    {
      return ExecuteCommand(query, ExecuteCommandToReferenceScalar<TResult>, options, queryParameters);
    }

    /// <summary>
    /// Executes <paramref name="query"/> with parameters <paramref name="queryParameters"/> that returns no data. 
    /// Any options passed in <paramref name="options"/> will overide values current options for this execution only.
    /// </summary>
    /// <param name="query">Query to execute. Any data returned will be ignored.</param>
    /// <param name="options">
    /// Container for execution options that used to override the current options for this execution.<para>To pass default options use <see cref="QueryOptions.Default"/></para>
    /// </param>
    /// <param name="queryParameters">
    /// Optional list of SqlParameters. Any parameters with null values will have the value replaced with DBNull.Value.
    /// </param>
    public static void ExecuteNonQuery(string query, QueryOptions options, params SqlParameter[] queryParameters)
    {
      ExecuteCommand(query, command => command.ExecuteNonQuery(), options, queryParameters);
    }

    #endregion

    #region Implementation

    private static IList<TResult> ExecuteCommandToObjects<TResult>(SqlCommand command, Func<DbDataReader, TResult> rowMap)
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

    private static TResult ExecuteCommandToObject<TResult>(SqlCommand command, Func<DbDataReader, TResult> rowMap) where TResult : class
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

    private static TResult ExecuteCommand<TResult>(string query, Func<SqlCommand, TResult> commandAction, QueryOptions options, params SqlParameter[] queryParameters)
    {
      if (options == null) throw new ArgumentException("options cannot be null");
      using (var connection = new SqlConnection(options.ConnectionString))
      {
        connection.Open();
        using (var command = new SqlCommand(query, connection))
        {
          command.CommandType = options.CommandType;
          command.CommandTimeout = (int)options.CommandTimeout.TotalSeconds;
          queryParameters = SubstituteDbNullForNullParameterValues(queryParameters);
          command.Parameters.AddRange(queryParameters);
          return commandAction(command);
        }
      }
    }

    private static void ExecuteCommand(string query, Action<SqlCommand> commandAction, QueryOptions options, params SqlParameter[] queryParameters)
    {
      if (options == null) throw new ArgumentException("options cannot be null");
      using (var connection = new SqlConnection(options.ConnectionString))
      {
        connection.Open();
        using (var command = new SqlCommand(query, connection))
        {
          command.CommandType = options.CommandType;
          command.CommandTimeout = (int)options.CommandTimeout.TotalSeconds;
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