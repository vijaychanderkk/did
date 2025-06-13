using Azure.Identity;
using Azure.Health.Deidentification;
using Azure;
using Azure.Storage.Blobs;
using Azure.Core;
using System.Threading.Tasks;

namespace DeidPoC
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            var inputFolder = @"C:\DeId\data\blobInput";
            var outputFolder = @"C:\DeId\data\blobOutput";

            var blobAccessSignature = "shareaccesssgnature";
            

            string serviceEndpoint = "deidendpoint";

            DeidentificationClient client = new(
                new Uri(serviceEndpoint),
                new AzureCliCredential(),
                new DeidentificationClientOptions()
            );

            Uri storageAccountContainerUri = new("bloburl");

            // uploading BLOBs
            var containerClient = new BlobContainerClient(
                blobContainerUri: storageAccountContainerUri,
                credential: new AzureSasCredential(blobAccessSignature)
            );
            
            var jobId = Guid.NewGuid().ToString();
            var inputfiles = Directory.GetFiles(inputFolder, "*.xml", SearchOption.TopDirectoryOnly);
            var allBlobs = new List<Tuple<string, string>>(); // full file name, blob name
            foreach (var fullfilename in inputfiles)
            {
                //var filename = Path.GetFileName(fullfilename);
                var blobname = Path.GetFileNameWithoutExtension(fullfilename);
                blobname = $"{jobId}/{blobname}";
                allBlobs.Add(new Tuple<string, string>(fullfilename, blobname));
            }


            // upload BLOBs here
            foreach (var blobInfo in allBlobs)
            {
                var targetBlobName = $"input/{blobInfo.Item2}";
                var blobClient = containerClient.GetBlobClient(targetBlobName);
                blobClient.Upload(blobInfo.Item1);
            }

            DeidentificationJob job = new(
                new SourceStorageLocation(storageAccountContainerUri, $"input/{jobId}"),
                new TargetStorageLocation(storageAccountContainerUri, "output/")
                );

            
            //job = client.CreateJob(WaitUntil.Started, jobId, job).Value;
            var operation = await client.CreateJobAsync(
                    WaitUntil.Completed,
                    jobId, 
                    job
                );

            var completedJob = operation.Value;
            Console.WriteLine($"Job Status: {completedJob.Status}");

            foreach (var blobInfo in allBlobs)
            {
                var targetBlobName = "output/" + blobInfo.Item2;
                var blobClient = containerClient.GetBlobClient(blobName: targetBlobName);
                var outFile = Path.Combine(outputFolder, blobInfo.Item2);
                outFile = Path.ChangeExtension(outFile, ".xml");
                blobClient.DownloadTo(path: outFile);
            }
        } 
    }
}
