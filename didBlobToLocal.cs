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
        private static string trackingLogPath;
        private static BlobClient logBlobClient;
        private static BlobClient trackingBlobClient;
        private static StringBuilder logBuffer = new StringBuilder();
        private static StringBuilder trackingBuffer = new StringBuilder();

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
            trackingLogPath = $"logs/job_tracking_{timestamp}.txt";

            try
            {
                // Initialize blob clients for logging
                var storageAccountUri = new Uri($"{storageAccountContainerUri.Scheme}://{storageAccountContainerUri.Host}");
                BlobServiceClient blobServiceClient = new(storageAccountUri, new AzureCliCredential());
                var containerClient = blobServiceClient.GetBlobContainerClient("deidlob");
                logBlobClient = containerClient.GetBlobClient(logFilePath);
                trackingBlobClient = containerClient.GetBlobClient(trackingLogPath);

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
                
                // Log output locations for each folder
                for (int folderNum = startFolder; folderNum <= endFolder; folderNum++)
                {
                    await LogMessage($"folder{folderNum} output location: folder{folderNum}-output/");
                }
                
                // Force final log uploads with multiple retries
                await UploadLogToBlob();
                await UploadTrackingToBlob();
                await Task.Delay(1000);
                await UploadLogToBlob(); // Retry to ensure upload
                await UploadTrackingToBlob(); // Retry to ensure upload
                
                Console.WriteLine("\nAll jobs have been submitted successfully!");
                Console.WriteLine("You can now close this console window.");
                Console.WriteLine($"Check these log files in blob storage:");
                Console.WriteLine($"- Main log: logs/deid_log_{timestamp}.txt");
                Console.WriteLine($"- Job tracking: logs/job_tracking_{timestamp}.txt (CSV format)");
                
                // Wait a moment to ensure all logs are written
                await Task.Delay(3000);
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
                
                // Create tracking entry for easy job-to-folder mapping
                await LogTrackingInfo($"folder{folderNum},{jobId},{inputFolder},{tempBlobOutputPath},STARTED,{DateTime.Now:yyyy-MM-dd HH:mm:ss}");
            }
            catch (Exception ex)
            {
                await LogMessage($"Folder{folderNum} FAILED TO START: {ex.Message}");
                await LogTrackingInfo($"folder{folderNum},ERROR,,,FAILED_TO_START,{DateTime.Now:yyyy-MM-dd HH:mm:ss}");
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

            // Upload log immediately for all important messages in fire-and-forget mode
            if (message.Contains("STARTED") || message.Contains("TRACKING") || message.Contains("FAILED TO START") || message.Contains("===") || message.Contains("Processing folders"))
            {
                await UploadLogToBlob();
            }
        }

        private static async Task LogTrackingInfo(string trackingData)
        {
            lock (trackingBuffer)
            {
                trackingBuffer.AppendLine(trackingData);
            }
            
            // Upload tracking data immediately
            await UploadTrackingToBlob();
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

        private static async Task UploadTrackingToBlob()
        {
            try
            {
                string trackingContent;
                lock (trackingBuffer)
                {
                    if (trackingBuffer.Length == 0)
                    {
                        // Create header if this is the first upload
                        trackingBuffer.AppendLine("FolderName,JobId,SourcePath,OutputPath,Status,Timestamp");
                    }
                    trackingContent = trackingBuffer.ToString();
                    trackingBuffer.Clear();
                }

                using (var stream = new MemoryStream(Encoding.UTF8.GetBytes(trackingContent)))
                {
                    await trackingBlobClient.UploadAsync(stream, overwrite: true);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to upload tracking log to blob: {ex.Message}");
            }
        }
    }
}
