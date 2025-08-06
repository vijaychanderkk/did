using Azure.Identity;
using Azure.Health.Deidentification;
using Azure;
using Azure.Storage.Blobs;
using Azure.Core;
using System.Threading.Tasks;
using System.Text;

namespace DeidPoC
{
    internal class Program
    {
        private static string logFilePath;

        static async Task Main(string[] args)
        {
            // Get folder range from user input
            Console.WriteLine("Enter folder range (e.g., 100-110): ");
            string folderRangeInput = Console.ReadLine();
            
            var (startFolder, endFolder) = ParseFolderRange(folderRangeInput);
            if (startFolder == -1 || endFolder == -1)
            {
                Console.WriteLine("Invalid folder range format. Please use format: 100-110");
                return;
            }

            // Get network drive path from user input
            Console.WriteLine("Enter network drive path for output (e.g., \\\\server\\share\\output): ");
            string networkDrivePath = Console.ReadLine();
            
            if (string.IsNullOrWhiteSpace(networkDrivePath))
            {
                Console.WriteLine("Network drive path cannot be empty.");
                return;
            }

            // Ensure network drive path exists
            if (!Directory.Exists(networkDrivePath))
            {
                Console.WriteLine($"Network drive path does not exist: {networkDrivePath}");
                return;
            }

            Uri storageAccountContainerUri = new("<<storageURL>>");
            string serviceEndpoint = "<<serviceendpoint>>";

            var timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
            logFilePath = Path.Combine("C:\\temp\\", $"deid_log_{timestamp}.txt");

            try
            {
                DeidentificationClient client = new(
                    new Uri(serviceEndpoint),
                    new AzureCliCredential(),
                    new DeidentificationClientOptions()
                );

                await LogMessage("--- DeID Process Started ---");
                await LogMessage($"Log File: {logFilePath}");
                await LogMessage($"Processing folders: {startFolder} to {endFolder}");
                await LogMessage($"Output will be saved to: {networkDrivePath}");

                // Process each folder in the range
                for (int folderNum = startFolder; folderNum <= endFolder; folderNum++)
                {
                    string inputFolder = $"folder{folderNum}";
                    string outputFolderName = $"folder{folderNum}-output";
                    string networkOutputPath = Path.Combine(networkDrivePath, outputFolderName);

                    await LogMessage($"\n--- Processing {inputFolder} ---");
                    
                    // Create output directory on network drive
                    Directory.CreateDirectory(networkOutputPath);
                    await LogMessage($"Created output directory: {networkOutputPath}");

                    // Use simple output path similar to original
                    string tempBlobOutputPath = $"{outputFolderName}/";
                    
                    var jobId = Guid.NewGuid().ToString().Substring(0, 25);
                    await LogMessage($"Job ID: {jobId}");

                    await LogMessage("Creating deidentification job...");
                    DeidentificationJob job = new(
                        new SourceStorageLocation(storageAccountContainerUri, inputFolder),
                        new TargetStorageLocation(storageAccountContainerUri, tempBlobOutputPath)
                    );

                    await LogMessage($"Starting deidentification job for {inputFolder}...");

                    // Start the job 
                    var operation = await client.DeidentifyDocumentsAsync(
                        WaitUntil.Started,
                        jobId,
                        job
                    );

                    await LogMessage($"Job creation request submitted. Polling for status...");

                    // Poll for job completion with timeout
                    var startTime = DateTime.Now;
                    var maxWaitTime = TimeSpan.FromHours(1);
                    var pollInterval = TimeSpan.FromSeconds(60);

                    DeidentificationJob completedJob = null;
                    bool jobCompleted = false;

                    while (!jobCompleted && DateTime.Now - startTime < maxWaitTime)
                    {
                        try
                        {
                            // Get current job status
                            var currentJob = await client.GetJobAsync(jobId);
                            completedJob = currentJob.Value;

                            await LogMessage($"Job status: {completedJob.Status} (Elapsed: {DateTime.Now - startTime:mm\\:ss})");

                            if (completedJob.Status.ToString() == "Succeeded" ||
                                completedJob.Status.ToString() == "Failed" ||
                                completedJob.Status.ToString() == "Canceled")
                            {
                                jobCompleted = true;
                                break;
                            }

                            // Wait before next poll
                            await LogMessage($"Job still running... waiting {pollInterval.TotalSeconds} seconds before next check");
                            await Task.Delay(pollInterval);
                        }
                        catch (Exception ex)
                        {
                            await LogMessage($"Error checking job status: {ex.Message}");
                            await Task.Delay(pollInterval);
                        }
                    }

                    if (!jobCompleted)
                    {
                        await LogMessage($"Job timed out after {maxWaitTime.TotalMinutes} minutes. Current status: {completedJob?.Status}");
                        continue; // Continue with next folder
                    }

                    if (completedJob.Status.ToString() == "Succeeded")
                    {
                        await LogMessage($"Job completed successfully. Copying files to network drive...");
                        
                        // Copy files from blob storage to network drive
                        await CopyBlobToNetworkDrive(storageAccountContainerUri, tempBlobOutputPath, networkOutputPath);
                        
                        await LogMessage($"Files copied to: {networkOutputPath}");
                        
                        // Optional: Clean up temporary blob storage
                        await LogMessage($"Cleaning up temporary blob storage: {tempBlobOutputPath}");
                        await CleanupTempBlobStorage(storageAccountContainerUri, tempBlobOutputPath);
                    }
                    else
                    {
                        await LogMessage($"Job failed with status: {completedJob.Status}");
                    }

                    await LogMessage($"--- Completed processing {inputFolder} ---\n");
                }

                await LogMessage("--- All DeID Processes Completed ---");
            }
            catch (Exception ex)
            {
                await LogMessage($"ERROR: {ex.Message}");
                await LogMessage($"StackTrace: {ex.StackTrace}");
                throw;
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

        private static async Task CopyBlobToNetworkDrive(Uri containerUri, string blobPath, string networkPath)
        {
            try
            {
                // Extract container name from URI more safely
                string containerName = containerUri.AbsolutePath.Split('/', StringSplitOptions.RemoveEmptyEntries).LastOrDefault();
                if (string.IsNullOrEmpty(containerName))
                {
                    await LogMessage("Warning: Could not determine container name, using default connection");
                }

                // Create blob service client - use the base URI without container path
                var baseUri = new Uri($"{containerUri.Scheme}://{containerUri.Host}");
                BlobServiceClient blobServiceClient = new(baseUri, new AzureCliCredential());
                
                // Get container client using the extracted container name or from URI
                BlobContainerClient containerClient;
                if (!string.IsNullOrEmpty(containerName))
                {
                    containerClient = blobServiceClient.GetBlobContainerClient(containerName);
                }
                else
                {
                    // Fallback to original method
                    containerClient = blobServiceClient.GetBlobContainerClient(containerUri.Segments.Last().TrimEnd('/'));
                }

                await LogMessage($"Listing blobs in: {blobPath}");

                // List all blobs in the output path
                await foreach (var blobItem in containerClient.GetBlobsAsync(prefix: blobPath))
                {
                    try
                    {
                        var blobClient = containerClient.GetBlobClient(blobItem.Name);
                        
                        // Create relative path for network drive
                        string relativePath = blobItem.Name.Substring(blobPath.Length).TrimStart('/');
                        string networkFilePath = Path.Combine(networkPath, relativePath);
                        
                        // Create directory structure if needed
                        string networkFileDir = Path.GetDirectoryName(networkFilePath);
                        if (!string.IsNullOrEmpty(networkFileDir))
                        {
                            Directory.CreateDirectory(networkFileDir);
                        }

                        await LogMessage($"Copying: {blobItem.Name} -> {networkFilePath}");

                        // Download blob to network drive
                        using (var fileStream = File.Create(networkFilePath))
                        {
                            await blobClient.DownloadToAsync(fileStream);
                        }

                        await LogMessage($"Successfully copied: {relativePath}");
                    }
                    catch (Exception ex)
                    {
                        await LogMessage($"Error copying blob {blobItem.Name}: {ex.Message}");
                    }
                }
            }
            catch (Exception ex)
            {
                await LogMessage($"Error in CopyBlobToNetworkDrive: {ex.Message}");
                throw;
            }
        }

        private static async Task CleanupTempBlobStorage(Uri containerUri, string blobPath)
        {
            try
            {
                // Extract container name from URI more safely
                string containerName = containerUri.AbsolutePath.Split('/', StringSplitOptions.RemoveEmptyEntries).LastOrDefault();
                
                // Create blob service client - use the base URI without container path
                var baseUri = new Uri($"{containerUri.Scheme}://{containerUri.Host}");
                BlobServiceClient blobServiceClient = new(baseUri, new AzureCliCredential());
                
                // Get container client
                BlobContainerClient containerClient;
                if (!string.IsNullOrEmpty(containerName))
                {
                    containerClient = blobServiceClient.GetBlobContainerClient(containerName);
                }
                else
                {
                    containerClient = blobServiceClient.GetBlobContainerClient(containerUri.Segments.Last().TrimEnd('/'));
                }

                await LogMessage($"Cleaning up temporary blobs in: {blobPath}");

                // List and delete all blobs in the temp path
                await foreach (var blobItem in containerClient.GetBlobsAsync(prefix: blobPath))
                {
                    try
                    {
                        var blobClient = containerClient.GetBlobClient(blobItem.Name);
                        await blobClient.DeleteIfExistsAsync();
                        await LogMessage($"Deleted temporary blob: {blobItem.Name}");
                    }
                    catch (Exception ex)
                    {
                        await LogMessage($"Error deleting blob {blobItem.Name}: {ex.Message}");
                    }
                }
            }
            catch (Exception ex)
            {
                await LogMessage($"Error in CleanupTempBlobStorage: {ex.Message}");
            }
        }

        private static async Task LogMessage(string message)
        {
            var timestampedMessage = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff} - {message}";
            Console.WriteLine(timestampedMessage);
            try
            {
                await File.AppendAllTextAsync(logFilePath, timestampedMessage + Environment.NewLine);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to write to log file: {ex.Message}");
            }
        }
    }
}
