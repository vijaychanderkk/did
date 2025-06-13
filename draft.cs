using Azure.Identity;
using Azure.Health.Deidentification;
using Azure;
using Azure.Storage.Blobs;
using Azure.Core;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.Extensibility;

namespace DeidPoC
{
    internal class Program
    {
        private static ILogger<Program> _logger;
        private static TelemetryClient _telemetryClient;

        static async Task Main(string[] args)
        {
            // Configure Application Insights and logging
            var host = CreateHostBuilder(args).Build();
            _logger = host.Services.GetRequiredService<ILogger<Program>>();
            _telemetryClient = host.Services.GetRequiredService<TelemetryClient>();

            try
            {
                _logger.LogInformation("Starting Azure Deidentification process");
                await RunDeidentificationProcess();
                _logger.LogInformation("Azure Deidentification process completed successfully");
                
                // Ensure all telemetry is sent before application exits
                _telemetryClient.Flush();
                await Task.Delay(5000); // Wait for telemetry to be sent
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Fatal error occurred during deidentification process");
                _telemetryClient.TrackException(ex);
                _telemetryClient.Flush();
                await Task.Delay(5000);
                Environment.Exit(1);
            }
        }

        static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureLogging(logging =>
                {
                    logging.ClearProviders();
                    logging.AddApplicationInsights();
                    logging.AddConsole(); // Keep console for local debugging
                })
                .ConfigureServices((context, services) =>
                {
                    // Add Application Insights
                    services.AddApplicationInsightsTelemetryWorkerService(options =>
                    {
                        options.ConnectionString = context.Configuration["ApplicationInsights:ConnectionString"];
                    });
                });

        static async Task RunDeidentificationProcess()
        {
            var inputFolder = @"C:\DeId\data\blobInput";
            var outputFolder = @"C:\DeId\data\blobOutput";
            var blobAccessSignature = "shareaccesssgnature";
            string serviceEndpoint = "deidendpoint";

            _logger.LogInformation("Configuration loaded - InputFolder: {InputFolder}, OutputFolder: {OutputFolder}", 
                inputFolder, outputFolder);

            // Track custom metrics
            _telemetryClient.TrackEvent("DeidentificationProcessStarted", new Dictionary<string, string>
            {
                ["InputFolder"] = inputFolder,
                ["OutputFolder"] = outputFolder
            });

            // Validate directories
            if (!Directory.Exists(inputFolder))
            {
                _logger.LogError("Input folder does not exist: {InputFolder}", inputFolder);
                _telemetryClient.TrackEvent("ValidationError", new Dictionary<string, string>
                {
                    ["ErrorType"] = "InputFolderNotFound",
                    ["InputFolder"] = inputFolder
                });
                throw new DirectoryNotFoundException($"Input folder not found: {inputFolder}");
            }

            if (!Directory.Exists(outputFolder))
            {
                _logger.LogInformation("Output folder does not exist, creating: {OutputFolder}", outputFolder);
                Directory.CreateDirectory(outputFolder);
            }

            var overallStopwatch = System.Diagnostics.Stopwatch.StartNew();

            try
            {
                _logger.LogInformation("Initializing Azure Deidentification client with endpoint: {ServiceEndpoint}", serviceEndpoint);
                
                DeidentificationClient client = new(
                    new Uri(serviceEndpoint),
                    new AzureCliCredential(),
                    new DeidentificationClientOptions()
                );

                Uri storageAccountContainerUri = new("bloburl");
                _logger.LogInformation("Storage container URI configured: {StorageUri}", storageAccountContainerUri);

                // Initialize blob container client
                var containerClient = new BlobContainerClient(
                    blobContainerUri: storageAccountContainerUri,
                    credential: new AzureSasCredential(blobAccessSignature)
                );

                var jobId = Guid.NewGuid().ToString();
                _logger.LogInformation("Generated job ID: {JobId}", jobId);

                // Get input files
                var inputfiles = Directory.GetFiles(inputFolder, "*.xml", SearchOption.TopDirectoryOnly);
                _logger.LogInformation("Found {FileCount} XML files to process", inputfiles.Length);

                if (inputfiles.Length == 0)
                {
                    _logger.LogWarning("No XML files found in input folder: {InputFolder}", inputFolder);
                    _telemetryClient.TrackEvent("NoFilesFound", new Dictionary<string, string>
                    {
                        ["InputFolder"] = inputFolder
                    });
                    return;
                }

                // Track job metrics
                _telemetryClient.TrackMetric("FilesToProcess", inputfiles.Length);
                _telemetryClient.TrackEvent("JobStarted", new Dictionary<string, string>
                {
                    ["JobId"] = jobId,
                    ["FileCount"] = inputfiles.Length.ToString()
                });

                var allBlobs = new List<Tuple<string, string>>();
                foreach (var fullfilename in inputfiles)
                {
                    var blobname = Path.GetFileNameWithoutExtension(fullfilename);
                    blobname = $"{jobId}/{blobname}";
                    allBlobs.Add(new Tuple<string, string>(fullfilename, blobname));
                    _logger.LogDebug("Prepared blob mapping - File: {FileName}, Blob: {BlobName}", 
                        Path.GetFileName(fullfilename), blobname);
                }

                // Upload files to blob storage
                _logger.LogInformation("Starting blob upload process for {BlobCount} files", allBlobs.Count);
                var uploadStopwatch = System.Diagnostics.Stopwatch.StartNew();
                var uploadTasks = new List<Task>();
                
                foreach (var blobInfo in allBlobs)
                {
                    uploadTasks.Add(UploadBlobAsync(containerClient, blobInfo, jobId));
                }

                await Task.WhenAll(uploadTasks);
                uploadStopwatch.Stop();
                
                _logger.LogInformation("All files uploaded to blob storage successfully in {Duration}ms", uploadStopwatch.ElapsedMilliseconds);
                _telemetryClient.TrackMetric("UploadDurationMs", uploadStopwatch.ElapsedMilliseconds);
                _telemetryClient.TrackEvent("UploadCompleted", new Dictionary<string, string>
                {
                    ["JobId"] = jobId,
                    ["FileCount"] = allBlobs.Count.ToString(),
                    ["DurationMs"] = uploadStopwatch.ElapsedMilliseconds.ToString()
                });

                // Create and execute deidentification job
                _logger.LogInformation("Creating deidentification job with ID: {JobId}", jobId);
                
                DeidentificationJob job = new(
                    new SourceStorageLocation(storageAccountContainerUri, $"input/{jobId}"),
                    new TargetStorageLocation(storageAccountContainerUri, "output/")
                );

                var processingStopwatch = System.Diagnostics.Stopwatch.StartNew();
                
                var operation = await client.CreateJobAsync(
                    WaitUntil.Completed,
                    jobId, 
                    job
                );

                processingStopwatch.Stop();
                var completedJob = operation.Value;
                
                _logger.LogInformation("Deidentification job completed - Status: {Status}, Duration: {Duration}ms", 
                    completedJob.Status, processingStopwatch.ElapsedMilliseconds);

                // Track processing metrics
                _telemetryClient.TrackMetric("ProcessingDurationMs", processingStopwatch.ElapsedMilliseconds);
                _telemetryClient.TrackEvent("ProcessingCompleted", new Dictionary<string, string>
                {
                    ["JobId"] = jobId,
                    ["Status"] = completedJob.Status.ToString(),
                    ["DurationMs"] = processingStopwatch.ElapsedMilliseconds.ToString()
                });

                if (completedJob.Status == DeidentificationJobStatus.Succeeded)
                {
                    // Download processed files
                    _logger.LogInformation("Starting download of processed files");
                    var downloadStopwatch = System.Diagnostics.Stopwatch.StartNew();
                    var downloadTasks = new List<Task>();
                    
                    foreach (var blobInfo in allBlobs)
                    {
                        downloadTasks.Add(DownloadBlobAsync(containerClient, blobInfo, outputFolder));
                    }

                    await Task.WhenAll(downloadTasks);
                    downloadStopwatch.Stop();
                    
                    _logger.LogInformation("All processed files downloaded successfully to: {OutputFolder} in {Duration}ms", 
                        outputFolder, downloadStopwatch.ElapsedMilliseconds);
                    
                    _telemetryClient.TrackMetric("DownloadDurationMs", downloadStopwatch.ElapsedMilliseconds);
                    _telemetryClient.TrackEvent("DownloadCompleted", new Dictionary<string, string>
                    {
                        ["JobId"] = jobId,
                        ["FileCount"] = allBlobs.Count.ToString(),
                        ["DurationMs"] = downloadStopwatch.ElapsedMilliseconds.ToString()
                    });
                }
                else
                {
                    _logger.LogError("Deidentification job failed with status: {Status}", completedJob.Status);
                    
                    _telemetryClient.TrackEvent("JobFailed", new Dictionary<string, string>
                    {
                        ["JobId"] = jobId,
                        ["Status"] = completedJob.Status.ToString()
                    });
                    
                    // Log job details if available
                    if (completedJob.Error != null)
                    {
                        _logger.LogError("Job error details: {ErrorCode} - {ErrorMessage}", 
                            completedJob.Error.Code, completedJob.Error.Message);
                        
                        _telemetryClient.TrackEvent("JobError", new Dictionary<string, string>
                        {
                            ["JobId"] = jobId,
                            ["ErrorCode"] = completedJob.Error.Code,
                            ["ErrorMessage"] = completedJob.Error.Message
                        });
                    }
                }

                overallStopwatch.Stop();
                _telemetryClient.TrackMetric("TotalProcessingDurationMs", overallStopwatch.ElapsedMilliseconds);
                _telemetryClient.TrackEvent("ProcessCompleted", new Dictionary<string, string>
                {
                    ["JobId"] = jobId,
                    ["Success"] = (completedJob.Status == DeidentificationJobStatus.Succeeded).ToString(),
                    ["TotalDurationMs"] = overallStopwatch.ElapsedMilliseconds.ToString(),
                    ["FileCount"] = allBlobs.Count.ToString()
                });
            }
            catch (RequestFailedException ex)
            {
                _logger.LogError(ex, "Azure service request failed - Status: {Status}, ErrorCode: {ErrorCode}, Message: {Message}", 
                    ex.Status, ex.ErrorCode, ex.Message);
                
                _telemetryClient.TrackException(ex, new Dictionary<string, string>
                {
                    ["ErrorType"] = "AzureServiceRequest",
                    ["Status"] = ex.Status.ToString(),
                    ["ErrorCode"] = ex.ErrorCode
                });
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unexpected error during deidentification process");
                _telemetryClient.TrackException(ex, new Dictionary<string, string>
                {
                    ["ErrorType"] = "UnexpectedError"
                });
                throw;
            }
        }

        private static async Task UploadBlobAsync(BlobContainerClient containerClient, Tuple<string, string> blobInfo, string jobId)
        {
            var uploadStopwatch = System.Diagnostics.Stopwatch.StartNew();
            try
            {
                var targetBlobName = $"input/{blobInfo.Item2}";
                var blobClient = containerClient.GetBlobClient(targetBlobName);
                var fileName = Path.GetFileName(blobInfo.Item1);
                
                _logger.LogDebug("Uploading file {FileName} to blob {BlobName}", fileName, targetBlobName);
                
                var uploadResponse = await blobClient.UploadAsync(blobInfo.Item1, overwrite: true);
                uploadStopwatch.Stop();
                
                _logger.LogDebug("Successfully uploaded {FileName} - ETag: {ETag}, Duration: {Duration}ms", 
                    fileName, uploadResponse.Value.ETag, uploadStopwatch.ElapsedMilliseconds);
                
                _telemetryClient.TrackEvent("FileUploaded", new Dictionary<string, string>
                {
                    ["FileName"] = fileName,
                    ["BlobName"] = targetBlobName,
                    ["JobId"] = jobId,
                    ["Success"] = "true"
                });
                _telemetryClient.TrackMetric("FileUploadDurationMs", uploadStopwatch.ElapsedMilliseconds);
            }
            catch (Exception ex)
            {
                uploadStopwatch.Stop();
                _logger.LogError(ex, "Failed to upload file {FileName} to blob storage", 
                    Path.GetFileName(blobInfo.Item1));
                
                _telemetryClient.TrackException(ex, new Dictionary<string, string>
                {
                    ["Operation"] = "FileUpload",
                    ["FileName"] = Path.GetFileName(blobInfo.Item1),
                    ["JobId"] = jobId
                });
                _telemetryClient.TrackEvent("FileUploaded", new Dictionary<string, string>
                {
                    ["FileName"] = Path.GetFileName(blobInfo.Item1),
                    ["JobId"] = jobId,
                    ["Success"] = "false"
                });
                throw;
            }
        }

        private static async Task DownloadBlobAsync(BlobContainerClient containerClient, Tuple<string, string> blobInfo, string outputFolder)
        {
            var downloadStopwatch = System.Diagnostics.Stopwatch.StartNew();
            try
            {
                var targetBlobName = "output/" + blobInfo.Item2;
                var blobClient = containerClient.GetBlobClient(blobName: targetBlobName);
                var outFile = Path.Combine(outputFolder, blobInfo.Item2);
                outFile = Path.ChangeExtension(outFile, ".xml");
                
                _logger.LogDebug("Downloading blob {BlobName} to {OutputFile}", targetBlobName, outFile);
                
                var downloadResponse = await blobClient.DownloadToAsync(path: outFile);
                downloadStopwatch.Stop();
                
                _logger.LogDebug("Successfully downloaded {BlobName} to {OutputFile}, Duration: {Duration}ms", 
                    targetBlobName, Path.GetFileName(outFile), downloadStopwatch.ElapsedMilliseconds);
                
                _telemetryClient.TrackEvent("FileDownloaded", new Dictionary<string, string>
                {
                    ["BlobName"] = targetBlobName,
                    ["OutputFile"] = Path.GetFileName(outFile),
                    ["Success"] = "true"
                });
                _telemetryClient.TrackMetric("FileDownloadDurationMs", downloadStopwatch.ElapsedMilliseconds);
            }
            catch (Exception ex)
            {
                downloadStopwatch.Stop();
                _logger.LogError(ex, "Failed to download blob {BlobName}", 
                    "output/" + blobInfo.Item2);
                
                _telemetryClient.TrackException(ex, new Dictionary<string, string>
                {
                    ["Operation"] = "FileDownload",
                    ["BlobName"] = "output/" + blobInfo.Item2
                });
                _telemetryClient.TrackEvent("FileDownloaded", new Dictionary<string, string>
                {
                    ["BlobName"] = "output/" + blobInfo.Item2,
                    ["Success"] = "false"
                });
                throw;
            }
        }
    }
}
