using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BackupToUrlWithRotation
{
    public class Database
    {
        public string Name { get; set; }
        public int ID { get; set; }
        public bool is_read_only { get; set; }
        public string state_desc { get; set; }
        public string recovery_model_desc { get; set; }

        public override string ToString()
        {
            return string.Format("Database[Name={0:S}, ID={1:N0}, is_read_only={2:S}, state_desc={3:S}, recovery_model_desc={4:S}]",
                Name, ID, is_read_only.ToString(), state_desc, recovery_model_desc);
        }
    }
}
