using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage;
using System.Globalization;

namespace BackupToUrlWithRotation
{
    class Program
    {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(typeof(Program));
        private static Configuration config;
        private static DBUtil dbUtil;
        private static System.Text.RegularExpressions.Regex regex = null;

        public const string DATE_FORMAT = "yyyyMMdd";
        public const string TIME_FORMAT = "HHmmss";

        public const string M_BACKUP_PERFORMED_BY = "BackupPerformedBy";
        public const string M_BACKUP_START_TIME = "BackupStartTime";
        public const string M_BACKUP_END_TIME = "BackupEndTime";
        public const string M_BACKUP_TOOL_EXECUTED_BY = "BackupToolExecutedBy";
        public const string M_BACKUP_DATABASE = "BackupDatabaseName";

        public const string M_DATETIME_FORMAT = "MM/dd/yyyy HH:mm:ss.fffzzz";

        public static string FQ_PROGRAM_NAME = System.Reflection.Assembly.GetExecutingAssembly().GetName().Name + ".exe v" + System.Reflection.Assembly.GetExecutingAssembly().GetName().Version.ToString();

        static int Main(string[] args)
        {
            return Process(args);
        }

        static int Process(string[] args)
        {
            Console.WriteLine(FQ_PROGRAM_NAME);
            log4net.Config.XmlConfigurator.Configure(System.Reflection.Assembly.GetExecutingAssembly().GetManifestResourceStream("BackupToUrlWithRotation.log4net.xml"));

            log.Debug("Parsing command line");
            try
            {
                config = Configuration.ParseFromCommandLine(args);
            }
            catch (Exception e)
            {
                log.ErrorFormat("Syntax error: {0:S}", e.Message);
                Configuration.Usage();

                return -1;
            }
            log.Debug("Parsing command line completed");

            log.DebugFormat("config == {0:S}", config.ToString());

            if (!string.IsNullOrEmpty(config.Regex))
            {
                log.DebugFormat("Parsing RegEx {0:S}", config.Regex);
                regex = new System.Text.RegularExpressions.Regex(config.Regex);
            }

            System.Data.SqlClient.SqlConnectionStringBuilder sb = new System.Data.SqlClient.SqlConnectionStringBuilder();
            sb.DataSource = config.DataSource;
            if (config.UserIntegrated)
                sb.IntegratedSecurity = true;
            else
            {
                sb.IntegratedSecurity = false;
                sb.UserID = config.Username;
                sb.Password = config.Password;
            }

            #region Azure connection setup
            Microsoft.WindowsAzure.Storage.Auth.StorageCredentials sc = new Microsoft.WindowsAzure.Storage.Auth.StorageCredentials(config.StorageAccount, config.Secret);
            CloudStorageAccount csa = new CloudStorageAccount(sc, true);
            var client = csa.CreateCloudBlobClient();
            var container = client.GetContainerReference(config.Container);
            log.DebugFormat("Creating container {0:S} if not existent", config.Container);
            container.CreateIfNotExists();
            #endregion

            dbUtil = new DBUtil(sb.ConnectionString, SQLInfoHandler);

            #region CREDENTIAL creation
            string tempCred = Guid.NewGuid().ToString();

            log.InfoFormat("Creating temp credential: {0:S}", tempCred);
            dbUtil.CreateCredential(tempCred, config.StorageAccount, config.Secret);
            #endregion

            try
            {
                #region List databases to backup
                // now that we have the container, start with the backups
                log.DebugFormat("Getting database to backup list");
                var lDBs = dbUtil.ListDatabases();
                log.DebugFormat("{0:N0} databases available", lDBs.Count); 

                if (config.BackupType == BackupType.Log) // full recovery mode only
                    lDBs = lDBs.Where(x => x.recovery_model_desc == "FULL").ToList();
                if (!config.IncludeReadOnly) // exclude read only
                    lDBs = lDBs.Where(x => !x.is_read_only).ToList();
                if (regex != null)
                {
                    log.InfoFormat("{0:N0} databases available", lDBs.Count);
                    lDBs = lDBs.Where(x => regex.IsMatch(x.Name)).ToList();
                }
                log.InfoFormat("{0:N0} databases to backup", lDBs.Count);
                #endregion

                #region Backup to URL
                foreach (var db in lDBs)
                {
                    log.InfoFormat("Performing backup of {0:S}", db);

                    DateTime dt = DateTime.Now;
                    string strBackupName = string.Format("{0:S}_{1:S}_{2:S}.{3:S}",
                        dt.ToString(DATE_FORMAT),
                        dt.ToString(TIME_FORMAT),
                        db.Name,
                        config.BackupType.ToString());
                    log.InfoFormat("Backup will be called {0:S}", strBackupName);

                    string backupUrl = string.Format("{0:S}{1:S}/{2:S}",
                        client.StorageUri.PrimaryUri.ToString(),
                        config.Container,
                        strBackupName);
                    log.DebugFormat("Backup URI {0:S}", backupUrl);

                    switch (config.BackupType)
                    {
                        case BackupType.Full:
                            dbUtil.BackupDatabaseToUrl(db, backupUrl, tempCred);
                            break;
                        case BackupType.Differential:
                            dbUtil.BackupDifferentialToUrl(db, backupUrl, tempCred);
                            break;
                        case BackupType.Log:
                            dbUtil.BackupLogToUrl(db, backupUrl, tempCred);
                            break;
                    }

                    #region Add "our" metadata to the backup
                    var cBackup = container.GetBlobReference(strBackupName);
                    {
                        log.DebugFormat("Setting {0:S} metadata {1:S} to {2:S}", cBackup.Name, M_BACKUP_PERFORMED_BY, FQ_PROGRAM_NAME);
                        cBackup.Metadata.Add(M_BACKUP_PERFORMED_BY, FQ_PROGRAM_NAME);
                    }

                    {
                        string sBackupStartTime = dt.ToString(M_DATETIME_FORMAT, CultureInfo.InvariantCulture);
                        log.DebugFormat("Setting {0:S} metadata {1:S} to {2:S}", cBackup.Name, M_BACKUP_START_TIME, sBackupStartTime);
                        cBackup.Metadata.Add(M_BACKUP_START_TIME, sBackupStartTime);
                    }

                    {
                        string sBackupEndTime = DateTime.Now.ToString(M_DATETIME_FORMAT, CultureInfo.InvariantCulture);
                        log.DebugFormat("Setting {0:S} metadata {1:S} to {2:S}", cBackup.Name, M_BACKUP_END_TIME, sBackupEndTime);
                        cBackup.Metadata.Add(M_BACKUP_END_TIME, sBackupEndTime);
                    }

                    {
                        string bteb = System.Security.Principal.WindowsIdentity.GetCurrent().Name;
                        log.DebugFormat("Setting {0:S} metadata {1:S} to {2:S}", cBackup.Name, M_BACKUP_TOOL_EXECUTED_BY, bteb);
                        cBackup.Metadata.Add(M_BACKUP_TOOL_EXECUTED_BY, bteb);
                    }

                    {
                        log.DebugFormat("Setting {0:S} metadata {1:S} to {2:S}", cBackup.Name, M_BACKUP_DATABASE, db.Name);
                        cBackup.Metadata.Add(M_BACKUP_DATABASE, db.Name);
                    }

                    log.InfoFormat("Updating blob \"{0:S}\" metadata", cBackup.Name);
                    cBackup.SetMetadata();
                    #endregion
                }
                #endregion
            }
            finally
            {
                // try to drop credential if possibile
                log.InfoFormat("Dropping temp credential: {0:S}", tempCred);
                dbUtil.DropCredential(tempCred);
            }



            #region Old backups deletion
            if (config.RetentionDays != -1)
            {
                foreach (var blob_uri in container.ListBlobs())
                {
                    CloudBlob cblob = new CloudBlob(blob_uri.Uri);

                    var blob = container.GetBlobReference(cblob.Name);
                    log.DebugFormat("Fetching {0:S} metadata", blob.Name);
                    blob.FetchAttributes();

                    // try to parse to our format
                    log.DebugFormat("Parsing {0:S} metadata", blob.Name);

                    TimeSpan delta;
                    try
                    {
                        if (blob.Metadata.ContainsKey(M_BACKUP_END_TIME))
                        {
                            string database_name;
                            if(!blob.Metadata.TryGetValue(M_BACKUP_DATABASE, out database_name))
                            {
                                log.ErrorFormat("Cannot find metadata key {0:S} skipping {1:S}", M_BACKUP_DATABASE, blob.Name);
                                continue;
                            }

                            if (regex != null)
                            {
                                if (!regex.IsMatch(database_name))
                                {
                                    log.InfoFormat("Ignoring backup {0:S} because if of database {1:S} which doesn't match with the regular expression", blob.Name, database_name);
                                    continue;
                                }
                            }

                            log.InfoFormat("Backup {0:S} key {1:S} has value {2:S}", blob.Name, M_BACKUP_END_TIME, blob.Metadata[M_BACKUP_END_TIME]);
                            DateTime dEndBackup = DateTime.ParseExact(blob.Metadata[M_BACKUP_END_TIME], M_DATETIME_FORMAT, CultureInfo.InvariantCulture);
                            log.InfoFormat("Backup {0:S} was taken {1:S}", blob.Name, dEndBackup.ToString());
                            delta = DateTime.Now - dEndBackup;

                            if (delta.TotalDays > config.RetentionDays)
                            {
                                log.WarnFormat("Backup {0:S} was taken {1:N0} days ago (retention is {2:N0} days). It will be deleted", blob.Name, delta.TotalDays, config.RetentionDays);
                                try
                                {
                                    blob.Delete();
                                    log.DebugFormat("Backup {0:S} deleted", blob.Name);
                                }
                                catch (Exception e)
                                {
                                    log.ErrorFormat("Failed to delete blob {0:S}: {1:S}", blob.Name, e.Message);
                                }
                            } else
                            {
                                log.InfoFormat("Backup {0:S} is {1:N0} days old (retention is {2:N0} days). Skipped.", blob.Name, delta.TotalDays, config.RetentionDays);
                            }                           
                        }
                        else
                        {
                            log.InfoFormat("Backup {0:S} does not contain {1:S} metadata. Skipped.", blob.Name, M_BACKUP_END_TIME);
                        }
                    }
                    catch (Exception e)
                    {
                        log.ErrorFormat("Failed to parse time. Metadata has been tampered with?  {0:S}. Blob will be ignored", e.Message);
                        continue;
                    }
                }
            }
            else
            {
                log.WarnFormat("Backup deletion disabled because retention is set to {0:N0}.", config.RetentionDays.ToString());
            }
            #endregion


            log.InfoFormat("Program completed, exiting with 0");

            return 0;
        }

        private static void SQLInfoHandler(object sender, System.Data.SqlClient.SqlInfoMessageEventArgs e)
        {
            log.InfoFormat(e.Message);
        }
    }
}
