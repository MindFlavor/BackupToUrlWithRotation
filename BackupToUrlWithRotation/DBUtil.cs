using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;

namespace BackupToUrlWithRotation
{
    class DBUtil
    {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(typeof(DBUtil));
        public const int COMMAND_TIMEOUT = 60 * 60 * 48; // 48 hours

        private string ConnectionString { get; set; }
        private SqlInfoMessageEventHandler SqlInfoMessageEventHandler { get;set;}

        public DBUtil(string connectionString, SqlInfoMessageEventHandler sqlInfoMessageEventHandler)
        {
            this.ConnectionString = connectionString;
            this.SqlInfoMessageEventHandler = sqlInfoMessageEventHandler;
        }

        private string GetTSQLScript(string scriptName)
        {
            log.DebugFormat("Retrieving tsql script {0:S}", scriptName);
            using (System.IO.StreamReader sr = new System.IO.StreamReader(System.Reflection.Assembly.GetExecutingAssembly().GetManifestResourceStream("BackupToUrlWithRotation.tsql." + scriptName)))
            {
                return sr.ReadToEnd();
            }
        }

        public SqlConnection OpenConnection()
        {
            log.Debug("OpenConnection called");
            SqlConnectionStringBuilder scsb = new SqlConnectionStringBuilder(ConnectionString);
            scsb.ApplicationName = System.Reflection.Assembly.GetExecutingAssembly().GetName().Name;

            log.DebugFormat("Connection string is {0:S}", scsb.ConnectionString);
            SqlConnection conn = new SqlConnection(scsb.ConnectionString);
            conn.Open();
            log.Debug("Connection opened successfully");

            return conn;
        }

        protected void SendNonQuery(string stmt)
        {
            var pts = new System.Threading.ParameterizedThreadStart(_SendNonQuery);
            System.Threading.Thread t = new System.Threading.Thread(pts);
            t.Start(stmt);
            t.Join();
        }

        protected void _SendNonQuery(object param)
        {
            string stmt = (string)param;

            using (SqlConnection conn = OpenConnection())
            {
                if (SqlInfoMessageEventHandler != null)
                    conn.InfoMessage += SqlInfoMessageEventHandler;
                log.DebugFormat("Sending statement {0:S}", stmt);
                using (SqlCommand cmd = new SqlCommand(stmt, conn))
                {
                    cmd.CommandTimeout = COMMAND_TIMEOUT;
                    cmd.ExecuteScalar();
                }
            }
        }

        public void CreateCredential(string credential, string identity, string secret)
        {
            log.DebugFormat("CreateCredential({0:S}, {1:S}, {2:S})", credential, identity, secret);
            string stmt = string.Format(GetTSQLScript("create_credential.sql"), credential, identity, secret);
            SendNonQuery(stmt);
        }

        public void DropCredential(string credential)
        {
            log.DebugFormat("DropCredential({0:S})", credential);
            string stmt = string.Format(GetTSQLScript("drop_credential.sql"), credential);
            SendNonQuery(stmt);
        }

        public void BackupDatabaseToUrl(string database, string url, string credential)
        {
            log.DebugFormat("BackupDatabaseToUrl({0:S}, {1:S}, {2:S})", database, url, credential);
            string stmt = string.Format(GetTSQLScript("backup_to_url.sql"), database, url, credential);
            SendNonQuery(stmt);
        }

        public void BackupDifferentialToUrl(string database, string url, string credential)
        {
            log.DebugFormat("BackupDifferentialToUrl({0:S}, {1:S}, {2:S})", database, url, credential);
            string stmt = string.Format(GetTSQLScript("backup_differential_to_url.sql"), database, url, credential);
            SendNonQuery(stmt);
        }

        public void BackupLogToUrl(string database, string url, string credential)
        {
            log.DebugFormat("BackupLogToUrl({0:S}, {1:S}, {2:S})", database, url, credential);
            string stmt = string.Format(GetTSQLScript("backup_log_to_url.sql"), database, url, credential);
            SendNonQuery(stmt);
        }

        public List<string> ListDatabases(bool logOnly)
        {
            log.DebugFormat("ListDatabases({0:S})", logOnly.ToString());
            string stmt;
            if (logOnly)
            {
                stmt = string.Format(GetTSQLScript("list_databases_log.sql"));
            }
            else
            {
                stmt = string.Format(GetTSQLScript("list_databases_full_backup.sql"));
            }

            List<string> lDBs = new List<string>();
            using (SqlConnection conn = OpenConnection())
            {
                log.DebugFormat("Sending statement {0:S}", stmt);
                using (SqlCommand cmd = new SqlCommand(stmt, conn))
                {
                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            lDBs.Add(reader.GetString(0));
                        }
                    }
                }
            }

            return lDBs;
        }
    }
}
