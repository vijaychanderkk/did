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
        private static string logFilePath;

        static async Task Main(string[] args)
        {
            var inputFolder = @"C:\DeId\data\blobInput";
            var outputFolder = @"C:\DeId\data\blobOutput";
            var blobAccessSignature = "shareaccesssgnature";
            string serviceEndpoint = "deidendpoint";

            // Initialize logging
            var timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
            logFilePath = Path.Combine(outputFolder, $"deid_log_{timestamp}.txt");
            
            // Ensure output directory exists
            Directory.CreateDirectory(outputFolder);

            try
            {
                await LogMessage("=== DeID Process Started ===");
                await LogMessage($"Input Folder: {inputFolder}");
                await LogMessage($"Output Folder: {outputFolder}");
                await LogMessage($"Log File: {logFilePath}");

                // Initialize DeID client
                await LogMessage("Initializing DeID client...");
                DeidentificationClient client = new(
                    new Uri(serviceEndpoint),
                    new AzureCliCredential(),
                    new DeidentificationClientOptions()
                );
                await LogMessage("DeID client initialized successfully");

                Uri storageAccountContainerUri = new("bloburl");

                // Initialize blob container client
                await LogMessage("Initializing blob container client...");
                var containerClient = new BlobContainerClient(
                    blobContainerUri: storageAccountContainerUri,
                    credential: new AzureSasCredential(blobAccessSignature)
                );
                await LogMessage("Blob container client initialized successfully");

                var jobId = Guid.NewGuid().ToString();
                await LogMessage($"Generated Job ID: {jobId}");

                // Get input files
                await LogMessage($"Scanning for XML files in: {inputFolder}");
                var inputfiles = Directory.GetFiles(inputFolder, "*.xml", SearchOption.TopDirectoryOnly);
                await LogMessage($"Found {inputfiles.Length} XML files to process");

                if (inputfiles.Length == 0)
                {
                    await LogMessage("No XML files found in input folder. Exiting.");
                    return;
                }

                var allBlobs = new List<Tuple<string, string>>(); // full file name, blob name
                foreach (var fullfilename in inputfiles)
                {
                    var fileName = Path.GetFileName(fullfilename);
                    var blobname = Path.GetFileNameWithoutExtension(fullfilename);
                    blobname = $"{jobId}/{blobname}";
                    allBlobs.Add(new Tuple<string, string>(fullfilename, blobname));
                    await LogMessage($"Prepared for upload: {fileName} -> {blobname}");
                }

                // Upload files to blob storage
                await LogMessage("Starting file upload to blob storage...");
                int uploadCount = 0;
                foreach (var blobInfo in allBlobs)
                {
                    var targetBlobName = $"input/{blobInfo.Item2}";
                    var blobClient = containerClient.GetBlobClient(targetBlobName);
                    
                    await LogMessage($"Uploading {Path.GetFileName(blobInfo.Item1)} to {targetBlobName}...");
                    await blobClient.UploadAsync(blobInfo.Item1, overwrite: true);
                    uploadCount++;
                    await LogMessage($"Upload successful ({uploadCount}/{allBlobs.Count})");
                }
                await LogMessage("All files uploaded successfully");

                // Create DeID job - input from input/{jobId}, output to output/
                await LogMessage("Creating deidentification job...");
                DeidentificationJob job = new(
                    new SourceStorageLocation(storageAccountContainerUri, $"input/{jobId}"),
                    new TargetStorageLocation(storageAccountContainerUri, "output/")
                );

                await LogMessage($"Starting deidentification job with ID: {jobId}");
                
                // Start the job but don't wait for completion immediately
                var operation = await client.CreateJobAsync(
                    WaitUntil.Started, // Changed from Completed to Started
                    jobId,
                    job
                );

                await LogMessage($"Job creation request submitted. Polling for status...");
                
                // Poll for job completion with timeout
                var startTime = DateTime.Now;
                var maxWaitTime = TimeSpan.FromMinutes(15); // 15 minute timeout
                var pollInterval = TimeSpan.FromSeconds(10); // Check every 10 seconds
                
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
                    return;
                }
                
                await LogMessage($"Job completed with final status: {completedJob.Status}");

                if (completedJob.Status.ToString() == "Succeeded")
                {
                    await LogMessage("Job succeeded. Starting file download...");
                    
                    // Download processed files - service creates output/input/{jobId}/ structure
                    int downloadCount = 0;
                    foreach (var blobInfo in allBlobs)
                    {
                        // Service preserves input structure: output/input/{jobId}/{filename}
                        var sourceBlobName = $"output/input/{blobInfo.Item2}";
                        var blobClient = containerClient.GetBlobClient(sourceBlobName);
                        
                        // Create local output file path
                        var localFileName = Path.GetFileName(blobInfo.Item2);
                        var outFile = Path.Combine(outputFolder, localFileName);
                        outFile = Path.ChangeExtension(outFile, ".xml");
                        
                        await LogMessage($"Downloading {sourceBlobName} to {outFile}...");
                        
                        try
                        {
                            // Check if blob exists before downloading
                            var blobExists = await blobClient.ExistsAsync();
                            if (blobExists.Value)
                            {
                                await blobClient.DownloadToAsync(outFile);
                                downloadCount++;
                                await LogMessage($"Download successful ({downloadCount}/{allBlobs.Count}): {Path.GetFileName(outFile)}");
                                
                                // Verify file was downloaded and has content
                                var fileInfo = new FileInfo(outFile);
                                if (fileInfo.Exists && fileInfo.Length > 0)
                                {
                                    await LogMessage($"File verification passed: {fileInfo.Length} bytes");
                                }
                                else
                                {
                                    await LogMessage($"WARNING: Downloaded file is empty or missing: {outFile}");
                                }
                            }
                            else
                            {
                                await LogMessage($"ERROR: Blob not found: {sourceBlobName}");
                                
                                // List all files in output/ to debug the actual structure
                                await LogMessage("Listing all files in output/ folder to debug structure:");
                                try
                                {
                                    await foreach (var blobItem in containerClient.GetBlobsAsync(prefix: "output/"))
                                    {
                                        await LogMessage($"Found blob: {blobItem.Name}");
                                    }
                                }
                                catch (Exception listEx)
                                {
                                    await LogMessage($"Error listing blobs: {listEx.Message}");
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            await LogMessage($"ERROR downloading {sourceBlobName}: {ex.Message}");
                        }
                    }
                    
                    await LogMessage($"Download process completed. {downloadCount}/{allBlobs.Count} files downloaded successfully");
                }
                else
                {
                    await LogMessage($"Job failed with status: {completedJob.Status}");
                    if (!string.IsNullOrEmpty(completedJob.Error?.Message))
                    {
                        await LogMessage($"Error details: {completedJob.Error.Message}");
                    }
                }

                await LogMessage("=== DeID Process Completed ===");
            }
            catch (Exception ex)
            {
                await LogMessage($"FATAL ERROR: {ex.Message}");
                await LogMessage($"Stack Trace: {ex.StackTrace}");
                throw;
            }
        }

        private static async Task LogMessage(string message)
        {
            var timestampedMessage = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff} - {message}";
            
            // Log to console
            Console.WriteLine(timestampedMessage);
            
            // Log to file
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
