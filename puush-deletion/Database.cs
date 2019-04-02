using System.Data;
using MySql.Data.MySqlClient;
using StatsdClient;

namespace puush_deletion
{
    internal static class Database
    {
        private static string connection_string;

        public static string ConnectionString
        {
            get { return connection_string; }
            set
            {
                if (connection_string == value) return;

                connection_string = value;
                HasDatabase = !string.IsNullOrEmpty(value);
                MySqlConnection.ClearAllPools();
            }
        }

        private static string connection_string_slave;

        public static string ConnectionStringSlave
        {
            get { return connection_string_slave; }
            set
            {
                if (connection_string_slave == value) return;

                connection_string_slave = value;
                MySqlConnection.ClearAllPools();
            }
        }

        public static bool HasDatabase;

        internal static MySqlConnection GetConnection(bool useSlave = false)
        {
            DogStatsd.Increment("database_queries");
            return new MySqlConnection(useSlave ? ConnectionStringSlave : ConnectionString);
        }

        internal static MySqlDataReader RunQuery(MySqlConnection m, string sqlString, params MySqlParameter[] parameters)
        {
            m.Open();
            MySqlCommand c = m.CreateCommand();
            if (parameters != null)
                c.Parameters.AddRange(parameters);
            c.CommandText = sqlString;
            c.CommandTimeout = 60000;
            return c.ExecuteReader(CommandBehavior.CloseConnection);
        }

        internal static MySqlDataReader RunQuery(string sqlString, params MySqlParameter[] parameters)
        {
            if (!HasDatabase) return null;
            return RunQuery(GetConnection(), sqlString, parameters);
        }

        internal static MySqlDataReader RunQuerySlave(string sqlString, params MySqlParameter[] parameters)
        {
            if (!HasDatabase) return null;
            return RunQuery(GetConnection(true), sqlString, parameters);
        }

        internal static object RunQueryOne(string sqlString, params MySqlParameter[] parameters)
        {
            if (!HasDatabase) return 0;

            using (MySqlConnection m = GetConnection())
            {
                m.Open();
                using (MySqlCommand c = m.CreateCommand())
                {
                    c.Parameters.AddRange(parameters);
                    c.CommandText = sqlString;
                    c.CommandTimeout = 36000;
                    return c.ExecuteScalar();
                }
            }
        }

        internal static int RunNonQuery(string sqlString, params MySqlParameter[] parameters)
        {
            if (!HasDatabase) return 0;

            using (MySqlConnection m = GetConnection())
            {
                m.Open();
                using (MySqlCommand c = m.CreateCommand())
                {
                    c.Parameters.AddRange(parameters);
                    c.CommandText = sqlString;
                    return c.ExecuteNonQuery();
                }
            }
        }
    }
}