using System.Data;

namespace puush_deletion
{
    public class PuushUpload
    {
        public readonly long UploadId;
        public readonly int UserId;
        public readonly byte Filestore;
        public readonly int Filesize;
        public readonly int Pool;
        public readonly string Path;

        public PuushUpload(string path)
        {
            Path = path;
        }
        
        public PuushUpload(IDataRecord dataRecord)
        {
            UploadId = (long)(ulong)dataRecord.GetValue(0);
            UserId = (int)(uint)dataRecord.GetValue(1);
            Filestore = dataRecord.GetByte(2);
            Filesize = (int)(uint)dataRecord.GetValue(3);
            Pool = dataRecord.GetInt32(4);
            Path = dataRecord.GetString(5);
        }

        public string FullPath => $"files/{Path}";
    }
}