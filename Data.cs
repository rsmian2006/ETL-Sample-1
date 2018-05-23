using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;

namespace LiquidusData
{
    public sealed class Data
    {
        string connectionString;
        SqlCommand sqlCmd; 

        /// <summary>
        /// A library for updating or returning records from the database.
        /// </summary>
        public Data()
        {
            connectionString = "";
            sqlCmd = new SqlCommand();
        }
        
        /// <summary>
        /// A library for updating or returning records from the database.
        /// </summary>
        /// <param name="ConnectionString">The connections string for the database.</param>
        public Data(string ConnectionString)
        {
            connectionString = ConnectionString;
            sqlCmd = new SqlCommand();
        }


        /// <summary>
        /// Adds a parameter to the query command.
        /// </summary>
        /// <param name="ParameterName">The parameter name.</param>
        /// <param name="value">The parameter's value.</param>
        public void AddParameter(string ParameterName, object value)
        {
            SqlParameter param = new SqlParameter();

            param.ParameterName = ParameterName;
            param.Value = value;
            sqlCmd.Parameters.Add(param);

        }

        /// <summary>
        /// Adds a parameter to the query command, and allows you to specify the direction.
        /// </summary>
        /// <param name="ParameterName">The parameter name.</param>
        /// <param name="value">The parameter's value.</param>
        /// <param name="Direction"></param>
        /// <remarks></remarks>

        public void AddParameter(string ParameterName, object value, ParameterDirection Direction)
        {
            SqlParameter param = new SqlParameter();

            param.ParameterName = ParameterName;
            param.Value = value;
            param.Direction = Direction;
            sqlCmd.Parameters.Add(param);

        }
        /// <summary>
        /// Wipes out all parameters in query command.
        /// </summary>
        public void ClearParameter()
        {
            if ((sqlCmd.Parameters == null) == false)
            {
                sqlCmd.Parameters.Clear();
            }
        }

        /// <summary>
        /// Removes a parameter from the sql command.
        /// </summary>
        /// <param name="ParameterName">The parameter to remove.</param>
        public void ClearParameter(string ParameterName)
        {
            if ((sqlCmd.Parameters == null) == false)
            {
                SqlParameter param = sqlCmd.Parameters[ParameterName];
                sqlCmd.Parameters.Remove(param);
            }
        }

        /// <summary>
        /// Creates a data table
        /// </summary>
        /// <param name="SQLStatement">The stored procedure you are calling.</param>
        /// <returns>A shiny new data table.</returns>
        /// <remarks></remarks>
        public DataTable GetDataTable(string SQLStatement)
        {
            DataTable dt = default(DataTable);

            using (SqlConnection cn = new SqlConnection(connectionString))
            {
                cn.Open();
                sqlCmd.Connection = cn;
                sqlCmd.CommandText = SQLStatement;

                sqlCmd.CommandType = CommandType.StoredProcedure;
                sqlCmd.CommandTimeout = 0;

                using (DataSet QueryData = new DataSet())
                {
                    using (SqlDataAdapter QueryAdapter = new SqlDataAdapter(sqlCmd))
                    {
                        try
                        {
                            QueryAdapter.Fill(QueryData);
                        }
                        catch (Exception ex)
                        {
                            this.WriteError(ex);
                        }
                    }

                    try
                    {
                        dt = QueryData.Tables[0];
                    }
                    catch (Exception ex)
                    {
                        this.WriteError(ex);
                    }
                }                                
                cn.Close();
            }
            
            return dt;
        }

        /// <summary>
        /// Executes a non-query statement.  
        /// </summary>
        /// <param name="SQLStatement">The stored procedure to run.</param>
        /// <returns>A boolean stating if the statement was successful or not.</returns>        
        public bool ExecuteNonQuery(string SQLStatement)
        {
            bool SuccessfulExecution = true;

            using (SqlConnection cn = new SqlConnection(connectionString))
            {
                cn.Open();
                sqlCmd.Connection = cn;
                sqlCmd.CommandText = SQLStatement;
                sqlCmd.CommandType = CommandType.StoredProcedure;
                sqlCmd.CommandTimeout = 0;

                using (SqlDataAdapter QueryAdapter = new SqlDataAdapter(sqlCmd))
                {
                    try
                    {
                        sqlCmd.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        Trace.WriteLine(String.Concat(SQLStatement, " --- ", connectionString));
                        Trace.WriteLine(ex);
                        this.WriteError(ex);
                        SuccessfulExecution = false;
                    }
                    finally
                    {
                        sqlCmd.Connection.Close();
                    }
                }
                cn.Close();
            }
            
            //tell the calling function if this was successful
            return SuccessfulExecution;

        }

        /// <summary>
        /// Executes a scalar command and returns the the first column of the first row in the result set returned by the query.
        /// </summary>
        /// <param name="SQLStatement">The stored procedure to run.</param>
        /// <returns>The first column of the first row in the result set returned by the query.</returns>
        public object ExecuteScalar(string SQLStatement)
        {

            object iIdentity = null;

            using (SqlConnection cn = new SqlConnection(connectionString))
            {
                cn.Open();
                sqlCmd.Connection = cn;
                sqlCmd.CommandText = SQLStatement;
                sqlCmd.CommandType = CommandType.StoredProcedure;
                sqlCmd.CommandTimeout = 0;

                using (SqlDataAdapter QueryAdapter = new SqlDataAdapter(sqlCmd))
                {
                    try
                    {
                        iIdentity = sqlCmd.ExecuteScalar();
                    }
                    catch (Exception ex)
                    {
                        this.WriteError(ex);
                        iIdentity = 0;
                    }
                    finally
                    {
                        sqlCmd.Connection.Close();
                    }
                }
                cn.Close();
            }

            return iIdentity;
        }

        /// <summary>
        /// Returns the schema of a table.
        /// </summary>
        /// <param name="tablename">The table to check.</param>        
        public DataTable GetDataTableSchema(string tablename)
        {

            DataTable tbl = new DataTable();
            string selectStatement = string.Concat("SELECT TOP 1 * FROM ", tablename);
            
            using (SqlConnection cn = new SqlConnection(connectionString))
            {
                cn.Open();
                var _with1 = sqlCmd;
                _with1.Connection = cn;
                _with1.CommandType = CommandType.Text;
                _with1.CommandText = selectStatement;
                _with1.CommandTimeout = 0;

                using (DataSet QueryData = new DataSet())
                {
                    using (SqlDataAdapter QueryAdapter = new SqlDataAdapter(sqlCmd))
                    {
                        try
                        {
                            QueryAdapter.Fill(QueryData);
                        }
                        catch (Exception ex)
                        {
                            this.WriteError(ex);
                        }
                    }

                    try
                    {
                        tbl = QueryData.Tables[0];
                    }
                    catch (Exception ex)
                    {
                        tbl = new DataTable();
                        this.WriteError(ex);
                    }
                }

                cn.Close();
            }
            return tbl;

        }

        /// <summary>
        /// Creates a data table
        /// </summary>
        /// <param name="SQLStatement">The stored procedure you are calling.</param>
        /// <returns>A shiny new data table.</returns>
        /// <remarks></remarks>
        public DataSet GetDataSet(string SQLStatement)
        {
            DataSet ds = default(DataSet);

            using (SqlConnection cn = new SqlConnection(connectionString))
            {
                cn.Open();
                sqlCmd.Connection = cn;
                sqlCmd.CommandText = SQLStatement;

                sqlCmd.CommandType = CommandType.StoredProcedure;
                sqlCmd.CommandTimeout = 0;

                using (ds = new DataSet())
                {
                    using (SqlDataAdapter QueryAdapter = new SqlDataAdapter(sqlCmd))
                    {
                        try
                        {
                            QueryAdapter.Fill(ds);
                        }
                        catch (Exception ex)
                        {
                            this.WriteError(new Exception(SQLStatement));
                            this.WriteError(ex);
                        }
                    }
                }
                cn.Close();
            }

            return ds;
        }

        /// <summary>
        /// Sends the exception message to the status handler.
        /// </summary>
        /// <param name="ex">The exception </param>
        private void WriteError(Exception ex)
        {
            Trace.WriteLine(ex);
        }

        //event handlers
        #region
        public delegate void WriteStatusHandler(string Status);
        public event WriteStatusHandler StatusUpdate;
        public void WriteStatus(string Status)
        {
            if (StatusUpdate != null)
                StatusUpdate(Status);
        }
        
        #endregion


    }
}
