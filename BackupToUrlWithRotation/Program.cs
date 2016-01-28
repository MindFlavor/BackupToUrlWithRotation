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

        public const string DATE_FORMAT = "yyyyMMdd";
        public const string TIME_FORMAT = "HHmmss";

        static int Main(string[] args)
        {
            return Process(args);
        }

        static int Process(string[] args)
        {
            Console.WriteLine(System.Reflection.Assembly.GetExecutingAssembly().GetName().Name + ".exe v" + System.Reflection.Assembly.GetExecutingAssembly().GetName().Version);
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
            string tempCred = Guid.NewGuid().ToString();

            log.InfoFormat("Creating temp credential: {0:S}", tempCred);
            dbUtil.CreateCredential(tempCred, config.StorageAccount, config.Secret);

            try
            {
                #region List databases to backup
                // now that we have the container, start with the backups
                log.DebugFormat("Getting database to backup list");
                var lDBs = dbUtil.ListDatabases(config.BackupType == BackupType.Log);
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
                        db,
                        config.BackupType.ToString());

                    log.InfoFormat("Backup will be called {0:S}", strBackupName);

                    string backupUrl = string.Format("{0:S}{1:S}/{2:S}",
                        client.StorageUri.PrimaryUri.ToString(),
                        config.Container,
                        strBackupName);
                    log.DebugFormat("Full backup URI {0:S}", backupUrl);

                    //dbUtil.BackupDatabaseToUrl(db, backupUrl, tempCred);
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

                    // try to parse to our format
                    log.DebugFormat("Parsing {0:S}", blob.Name);

                    DateTime d;
                    if (!DateTime.TryParseExact(blob.Name.Substring(0, 8), DATE_FORMAT, CultureInfo.InvariantCulture, DateTimeStyles.None, out d))
                    {
                        log.InfoFormat("Ingoring blob {0:S} because doesn't seem to have a valid date", blob.Name);
                        continue;
                    }

                    DateTime t;

                    if (!DateTime.TryParseExact(blob.Name.Substring(9, 6), TIME_FORMAT, CultureInfo.InvariantCulture, DateTimeStyles.None, out t))
                    {
                        log.InfoFormat("Ingoring blob {0:S} because doesn't seem to have a valid time", blob.Name);
                        continue;
                    }

                    log.DebugFormat("{0:S} and {1:S}", d.ToString(), t.ToString());

                    DateTime dfinal = d.AddHours(t.Hour).AddMinutes(t.Minute).AddSeconds(t.Second);
                    log.InfoFormat("Backup {0:S} was taken {1:S}", blob.Name, dfinal.ToString());

                    var delta = DateTime.Now - dfinal;

                    if (true) //(delta.TotalDays > config.RetentionDays)
                    {
                        log.WarnFormat("Backup {0:S} was taken {1:S} days ago. It will be deleted", blob.Name, delta.TotalDays.ToString());
                        try
                        {
                            blob.Delete();
                        }
                        catch (Exception e)
                        {
                            log.ErrorFormat("Failed to delete blob {0:S}: {1:S}", blob.Name, e.Message);
                        }
                        log.DebugFormat("Backup {0:S} deleted", blob.Name);
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
