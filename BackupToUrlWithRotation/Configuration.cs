using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BackupToUrlWithRotation
{
    public enum BackupType
    {
        Full,
        Differential,
        Log,
        Unknown
    };

    public class Configuration
    {
        public BackupType BackupType { get; set; }
        public string DataSource { get; set; }
        public bool UserIntegrated { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }
        public string StorageAccount { get; set; }
        public string Secret { get; set; }
        public string Container { get; set; }
        public int RetentionDays { get; set; }
        public bool Verbose { get; set; }

        public const string F_BACKUP_TYPE = "-t";
        public const string F_DATA_SOURCE = "-d";
        public const string F_INTEGRATED = "-i";
        public const string F_USERNAME_PASSWORD = "-u";
        public const string F_STORAGE_ACCOUNT = "-s";
        public const string F_RETENTION = "-r";
        public const string F_VERBOSE = "-v";

        public Configuration()
        {
            BackupType = BackupType.Unknown;
            Verbose = false;
            RetentionDays = int.MinValue;
        }

        public static Configuration ParseFromCommandLine(string[] args) 
        {
            Configuration c = new Configuration();

            #region look for F_BACKUP_TYPE
            for (int i = 0; i < args.Length; i++)
            {
                if (args[i] == F_BACKUP_TYPE)
                {
                    if (i + 1 >= args.Length)
                    {
                        throw new ArgumentException(string.Format("{0:S} flag must be followed by backup type", F_BACKUP_TYPE));
                    }

                    string sBt = args[i + 1];

                    BackupType bt;

                    if (!Enum.TryParse<BackupType>(sBt, out bt))
                    {
                        throw new ArgumentException(string.Format("Backup type can be either {0:S}, {1:S} or {2:S}. {3:S} is not supported.",
                            BackupType.Full, BackupType.Differential, BackupType.Log, sBt));
                    }

                    c.BackupType = bt;

                    break;
                }
            }
            #endregion

            #region look for F_DATA_SOURCE
            for (int i = 0; i < args.Length; i++)
            {
                if (args[i] == F_DATA_SOURCE)
                {
                    if (i + 1 >= args.Length)
                    {
                        throw new ArgumentException(string.Format("{0:S} flag must be followed data source", F_DATA_SOURCE));
                    }

                    c.DataSource = args[i + 1];

                    break;
                }
            }
            #endregion

            #region look for F_INTEGRATED or F_USERNAME_PASSWORD
            for (int i = 0; i < args.Length; i++)
            {
                if (args[i] == F_INTEGRATED)
                {
                    c.UserIntegrated = true;
                    break;
                }
                if (args[i] == F_USERNAME_PASSWORD)
                {
                    c.UserIntegrated = false;
                    if (i + 2 >= args.Length)
                    {
                        throw new ArgumentException(string.Format("{0:S} flag must be followed by user name and password", F_USERNAME_PASSWORD));
                    }

                    c.Username = args[i + 1];
                    c.Password = args[i + 2];

                    break;
                }
            }
            #endregion

            #region look for F_STORAGE_ACCOUNT
            for (int i = 0; i < args.Length; i++)
            {
                if (args[i] == F_STORAGE_ACCOUNT)
                {
                    if (i + 3 >= args.Length)
                    {
                        throw new ArgumentException(string.Format("{0:S} flag must be followed by storage account name, secret and container", F_STORAGE_ACCOUNT));
                    }

                    c.StorageAccount = args[i + 1];
                    c.Secret = args[i + 2];
                    c.Container = args[i + 3];

                    break;
                }
            }
            #endregion

            #region look for F_RETENTION flag
            for (int i = 0; i < args.Length; i++)
            {
                if (args[i] == F_RETENTION)
                {
                    if (i + 1 >= args.Length)
                    {
                        throw new ArgumentException(string.Format("{0:S} flag must be followed by retention days", F_RETENTION));
                    }

                    int v;
                    if (!int.TryParse(args[i + 1], out v))
                    {
                        throw new ArgumentException(string.Format("{0:S} flag must be followed by a valid number ({1:S} is not a valid number)", F_RETENTION, args[i + 1]));
                    }

                    c.RetentionDays = v;

                    break;
                }
            }
            #endregion

            if (c.BackupType == BackupType.Unknown)
                throw new ArgumentException("Please specify backup type");

            if (string.IsNullOrEmpty(c.DataSource))
                throw new ArgumentException("Please specify the data source (SQL Server to backup)");

            if (c.UserIntegrated == false && string.IsNullOrEmpty(c.Username) && string.IsNullOrEmpty(c.Password))
                throw new ArgumentException("Please specify authentication type (either integrated or mixed)");

            if (string.IsNullOrEmpty(c.StorageAccount) || string.IsNullOrEmpty(c.Secret) || string.IsNullOrEmpty(c.Container))
                throw new ArgumentException("Please specify the Azure parameters (storage account, container and secret)");

            if (c.RetentionDays == int.MinValue)
                throw new ArgumentException("Please specify the retention days (-1 for unlimited)");

            return c;
        }

        public static void Usage()
        {
            Console.WriteLine("Syntax:\n" + System.Reflection.Assembly.GetExecutingAssembly().GetName().Name +
                string.Format(".exe {0:S} <{1:S}|{2:S}|{3:S}> {4:S} <data source> {5:S}|{6:S} <username> <password> {7:S} <storage account> <secret> <container name> {8:S} <retention days>",
                F_BACKUP_TYPE, BackupType.Full.ToString(), BackupType.Differential.ToString(), BackupType.Log.ToString(),
                F_DATA_SOURCE,
                F_INTEGRATED, F_USERNAME_PASSWORD,
                F_STORAGE_ACCOUNT,
                F_RETENTION
                ));
            Console.WriteLine("\nParameters:");
        }

        public override string ToString()
        {
            return string.Format("Configuration[BackupType={0:S}, DataSource={1:S}, UserIntegrated={2:S}, Username={3:S}, Password={4:S}, StorageAccount={5:S}, Secret={6:S}, Container={7:S}, RetentionDays={8:S}, Verbose={9:S}]",
                  BackupType.ToString(),
                  DataSource,
                  UserIntegrated.ToString(),
                  Username,
                  Password,
                  StorageAccount,
                  "xxxx",
                  Container,
                  RetentionDays.ToString(),
                  Verbose.ToString());
        }
    }
}
