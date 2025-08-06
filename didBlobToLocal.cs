using Azure.Identity;
using Azure.Health.Deidentification;
using Azure;
using Azure.Storage.Blobs;
using Azure.Core;
using System.Threading.Tasks;
using System.Text;
using System.Linq;

namespace DeidPoC
{
    internal class Program
    {
        private static string logFilePath;
        private static BlobClient logBlobClient;
        private static StringBuilder logBuffer = new StringBuilder();

        static async Task Main(string[] args)
        {
            // Get max parallel jobs from user input
            Console.WriteLine("Enter maximum number of parallel jobs (e.g., 5): ");
            string maxParallelInput = Console.ReadLine();
            int maxParallelJobs = 3; // Default value
            
            if (!string.IsNullOrWhiteSpace(maxParallelInput) && int.TryParse(maxParallelInput, out int parsedParallel))
            {
                maxParallelJobs = Math.Max(1, Math.Min(parsedParallel, 10)); // Limit between 1-10
            }

            // Get folder range from user input
            Console.WriteLine("Enter folder range (e.g., 100-110): ");
            string folderRangeInput = Console.ReadLine();
            
            var (startFolder, endFolder) = ParseFolderRange(folderRangeInput);
            if (startFolder == -1 || endFolder == -1)
            {
                Console.WriteLine("Invalid folder range format. Please use format: 100-110");
                return;
            }

            Uri storageAccountContainerUri = new("<<storageURL>>/deidlob");
            string serviceEndpoint = "<<serviceendpoint>>";

            var timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
            logFilePath = $"logs/deid_log_{timestamp}.txt";

            try
            {
                // Initialize blob client for logging
                var storageAccountUri = new Uri($"{storageAccountContainerUri.Scheme}://{storageAccountContainerUri.Host}");
                BlobServiceClient blobServiceClient = new(storageAccountUri, new AzureCliCredential());
                var containerClient = blobServiceClient.GetBlobContainerClient("deidlob");
                logBlobClient = containerClient.GetBlobClient(logFilePath);

                DeidentificationClient client = new(
                    new Uri(serviceEndpoint),
                    new AzureCliCredential(),
                    new DeidentificationClientOptions()
                );

                await LogMessage("=== DeID Batch Process Started ===");
                await LogMessage($"Processing folders: {startFolder} to {endFolder}");
                await LogMessage($"Maximum parallel jobs: {maxParallelJobs}");

                // Create list of folder numbers to process
                var foldersToProcess = Enumerable.Range(startFolder, endFolder - startFolder + 1).ToList();
                
                // Process folders in fire-and-forget mode
                await ProcessFoldersInParallel(client, foldersToProcess, maxParallelJobs, storageAccountContainerUri);

                await LogMessage("=== All Jobs Submitted - Application Exiting ===");
                await LogMessage($"Monitor job status via Azure portal or create a separate monitoring application");
                await LogMessage($"Output locations: folder{{number}}-output/ in deidlob container");
                
                // Final log upload
                await UploadLogToBlob();
                
                Console.WriteLine("\nAll jobs have been submitted successfully!");
                Console.WriteLine("You can now close this console window.");
                Console.WriteLine($"Check the log file: logs/deid_log_{timestamp}.txt in blob storage for job IDs.");
            }
            catch (Exception ex)
            {
                await LogMessage($"CRITICAL ERROR: {ex.Message}");
                await UploadLogToBlob();
                throw;
            }
        }

        private static async Task ProcessFoldersInParallel(DeidentificationClient client, List<int> foldersToProcess, int maxParallelJobs, Uri storageAccountContainerUri)
        {
            var semaphore = new SemaphoreSlim(maxParallelJobs, maxParallelJobs);
            var allTasks = new List<Task>();

            foreach (int folderNum in foldersToProcess)
            {
                var task = StartJobForFolderAsync(client, folderNum, storageAccountContainerUri, semaphore);
                allTasks.Add(task);
            }

            await LogMessage($"Starting {allTasks.Count} jobs in parallel...");
            await Task.WhenAll(allTasks);
            await LogMessage("All jobs have been submitted successfully");
        }

        private static async Task StartJobForFolderAsync(DeidentificationClient client, int folderNum, Uri storageAccountContainerUri, SemaphoreSlim semaphore)
        {
            await semaphore.WaitAsync();
            
            try
            {
                string inputFolder = $"all_data/folder{folderNum}";
                string tempBlobOutputPath = $"folder{folderNum}-output/";
                var jobId = Guid.NewGuid().ToString().Substring(0, 25);

                DeidentificationJob job = new(
                    new SourceStorageLocation(storageAccountContainerUri, inputFolder),
                    new TargetStorageLocation(storageAccountContainerUri, tempBlobOutputPath)
                );

                // Start the job (fire-and-forget)
                var operation = await client.DeidentifyDocumentsAsync(
                    WaitUntil.Started,
                    jobId,
                    job
                );

                await LogMessage($"Job {jobId} STARTED: {inputFolder} -> {tempBlobOutputPath}");
            }
            catch (Exception ex)
            {
                await LogMessage($"Folder{folderNum} FAILED TO START: {ex.Message}");
            }
            finally
            {
                semaphore.Release();
            }
        }

        private static (int startFolder, int endFolder) ParseFolderRange(string input)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(input) || !input.Contains("-"))
                    return (-1, -1);

                var parts = input.Split('-');
                if (parts.Length != 2)
                    return (-1, -1);

                int start = int.Parse(parts[0].Trim());
                int end = int.Parse(parts[1].Trim());

                if (start > end || start < 0 || end < 0)
                    return (-1, -1);

                return (start, end);
            }
            catch
            {
                return (-1, -1);
            }
        }

        private static async Task LogMessage(string message)
        {
            var timestampedMessage = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - {message}";
            Console.WriteLine(timestampedMessage);
            
            lock (logBuffer)
            {
                logBuffer.AppendLine(timestampedMessage);
            }

            // Upload log more frequently in fire-and-forget mode
            if (logBuffer.Length > 500 || message.Contains("STARTED") || message.Contains("FAILED TO START") || message.Contains("==="))
            {
                await UploadLogToBlob();
            }
        }

        private static async Task UploadLogToBlob()
        {
            try
            {
                string logContent;
                lock (logBuffer)
                {
                    if (logBuffer.Length == 0) return;
                    logContent = logBuffer.ToString();
                    logBuffer.Clear();
                }

                using (var stream = new MemoryStream(Encoding.UTF8.GetBytes(logContent)))
                {
                    await logBlobClient.UploadAsync(stream, overwrite: true);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to upload log to blob: {ex.Message}");
            }
        }
    }
}
