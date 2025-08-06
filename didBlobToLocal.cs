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
                
                // Process folders in parallel batches
                await ProcessFoldersInParallel(client, foldersToProcess, maxParallelJobs, storageAccountContainerUri);

                await LogMessage("=== All DeID Processes Completed ===");
                
                // Final log upload
                await UploadLogToBlob();
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
                var task = ProcessSingleFolderAsync(client, folderNum, storageAccountContainerUri, semaphore);
                allTasks.Add(task);
            }

            await LogMessage($"Started {allTasks.Count} parallel jobs");
            await Task.WhenAll(allTasks);
            await LogMessage("All parallel jobs completed");
        }

        private static async Task ProcessSingleFolderAsync(DeidentificationClient client, int folderNum, Uri storageAccountContainerUri, SemaphoreSlim semaphore)
        {
            await semaphore.WaitAsync();
            
            try
            {
                string inputFolder = $"all_data/folder{folderNum}";
                string tempBlobOutputPath = $"folder{folderNum}-output/";
                var jobId = Guid.NewGuid().ToString().Substring(0, 25);

                await LogMessage($"Job {jobId}: {inputFolder} -> {tempBlobOutputPath}");

                DeidentificationJob job = new(
                    new SourceStorageLocation(storageAccountContainerUri, inputFolder),
                    new TargetStorageLocation(storageAccountContainerUri, tempBlobOutputPath)
                );

                // Start the job 
                var operation = await client.DeidentifyDocumentsAsync(
                    WaitUntil.Started,
                    jobId,
                    job
                );

                // Poll for job completion with timeout
                var startTime = DateTime.Now;
                var maxWaitTime = TimeSpan.FromHours(2);
                var pollInterval = TimeSpan.FromSeconds(30);

                DeidentificationJob completedJob = null;
                bool jobCompleted = false;

                while (!jobCompleted && DateTime.Now - startTime < maxWaitTime)
                {
                    try
                    {
                        var currentJob = await client.GetJobAsync(jobId);
                        completedJob = currentJob.Value;

                        if (completedJob.Status.ToString() == "Succeeded" ||
                            completedJob.Status.ToString() == "Failed" ||
                            completedJob.Status.ToString() == "Canceled")
                        {
                            jobCompleted = true;
                            break;
                        }

                        await Task.Delay(pollInterval);
                    }
                    catch (Exception ex)
                    {
                        await LogMessage($"Job {jobId} polling error: {ex.Message}");
                        await Task.Delay(pollInterval);
                    }
                }

                if (!jobCompleted)
                {
                    await LogMessage($"Job {jobId} TIMEOUT after {maxWaitTime.TotalMinutes}min - Status: {completedJob?.Status}");
                    return;
                }

                if (completedJob.Status.ToString() == "Succeeded")
                {
                    await LogMessage($"Job {jobId} SUCCESS - Elapsed: {DateTime.Now - startTime:mm\\:ss}");
                }
                else
                {
                    string errorInfo = completedJob.Error != null ? $" - {completedJob.Error.Code}: {completedJob.Error.Message}" : "";
                    await LogMessage($"Job {jobId} FAILED - Status: {completedJob.Status}{errorInfo}");
                }
            }
            catch (Exception ex)
            {
                await LogMessage($"Folder{folderNum} EXCEPTION: {ex.Message}");
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

            // Upload log periodically (every 10 messages) or on important events
            if (logBuffer.Length > 1000 || message.Contains("SUCCESS") || message.Contains("FAILED") || message.Contains("==="))
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
