/*
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE.txt', which is part of this source code package.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;

namespace CrowCreek.Utilities.SqlServer
{
    public partial class SqlServerQueryManager
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
    public static Task<IEnumerable<TResult>> SelectToObjectsAsync<TResult>(string query, Func<DbDataReader, TResult> rowMap, QueryOptions options, params SqlParameter[] queryParameters)
    {
      return ExecuteCommandAsync(query, command => ExecuteCommandToObjectsAsync(command, rowMap), options, queryParameters);
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
    public static Task<TResult> SelectToObjectAsync<TResult>(string query, Func<DbDataReader, TResult> rowMap, QueryOptions options, params SqlParameter[] queryParameters) where TResult : class
    {
      return ExecuteCommandAsync(query, command => ExecuteCommandToObjectAsync(command, rowMap), options, queryParameters);
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
    public static Task<TResult> SelectValueScalarAsync<TResult>(string query, QueryOptions options, params SqlParameter[] queryParameters) where TResult : struct
    {
      return ExecuteCommandAsync(query, ExecuteCommandToScalarAsync<TResult>, options, queryParameters);
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
    public static Task<TResult> SelectReferenceScalarAsync<TResult>(string query, QueryOptions options, params SqlParameter[] queryParameters) where TResult : class
    {
      return ExecuteCommandAsync(query, ExecuteCommandToReferenceScalarAsync<TResult>, options, queryParameters);
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
    public static Task<int> ExecuteNonQueryAsync(string query, QueryOptions options, params SqlParameter[] queryParameters)
    {
      return ExecuteCommandAsync(query, command => command.ExecuteNonQueryAsync() , options, queryParameters);
    }

    #endregion

    #region Implementation

    private async static Task<IEnumerable<TResult>> ExecuteCommandToObjectsAsync<TResult>(SqlCommand command, Func<DbDataReader, TResult> rowMap)
    {
      var resultList = new List<TResult>();
      using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
      {
        while (await reader.ReadAsync().ConfigureAwait(false))
        {
          resultList.Add(rowMap(reader));
        }
      }
      return resultList;
    }

    private async static Task<TResult> ExecuteCommandToObjectAsync<TResult>(SqlCommand command, Func<DbDataReader, TResult> rowMap) where TResult : class
    {
      using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
      {
        if (await reader.ReadAsync().ConfigureAwait(false))
        {
          return rowMap(reader);
        }
        return null;
      }
    }

    private async static Task<TResult> ExecuteCommandToScalarAsync<TResult>(SqlCommand command)
    {
      var rawResult = await command.ExecuteScalarAsync().ConfigureAwait(false);
      try
      {
        return (TResult)rawResult;
      }
      catch (Exception ex)
      {
        throw new FieldReadCastException("Failed to cast execute scalar result", ex);
      }
    }

    private async static Task<TResult> ExecuteCommandToReferenceScalarAsync<TResult>(SqlCommand command) where TResult : class
    {
      return await command.ExecuteScalarAsync().ConfigureAwait(false) as TResult;
    }

    private async static Task<TResult> ExecuteCommandAsync<TResult>(string query, Func<SqlCommand, Task<TResult>> commandAction, QueryOptions options, params SqlParameter[] queryParameters)
    {
      if (options == null) throw new ArgumentException("options cannot be null");
      using (var connection = new SqlConnection(options.ConnectionString))
      {
        await connection.OpenAsync().ConfigureAwait(false);
        using (var command = new SqlCommand(query, connection))
        {
          command.CommandType = options.CommandType;
          command.CommandTimeout = (int)options.CommandTimeout.TotalSeconds;
          queryParameters = SubstituteDbNullForNullParameterValues(queryParameters);
          command.Parameters.AddRange(queryParameters);
          return await commandAction(command).ConfigureAwait(false);
        }
      }
    }
    #endregion
  }
}
