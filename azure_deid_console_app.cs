using Azure;
using Azure.Health.Deidentification;
using Azure.Identity;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Text;

namespace AzureHealthcareDeidentification
{
    public class Program
    {
        private static async Task Main(string[] args)
        {
            // Build configuration
            var configuration = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();

            // Setup DI container
            var host = Host.CreateDefaultBuilder(args)
                .ConfigureServices((context, services) =>
                {
                    services.AddSingleton<IConfiguration>(configuration);
                    services.AddScoped<AzureDeidentificationService>();
                    services.AddLogging(builder => builder.AddConsole());
                })
                .Build();

            var logger = host.Services.GetRequiredService<ILogger<Program>>();
            var deidentificationService = host.Services.GetRequiredService<AzureDeidentificationService>();

            try
            {
                logger.LogInformation("Starting Azure Healthcare De-identification Console Application");

                // Show menu
                await ShowMenuAsync(deidentificationService, logger);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "An error occurred during execution");
            }
        }

        private static async Task ShowMenuAsync(AzureDeidentificationService service, ILogger logger)
        {
            while (true)
            {
                Console.Clear();
                Console.WriteLine("=== Azure Healthcare De-identification Console ===");
                Console.WriteLine("1. Upload local files to blob storage");
                Console.WriteLine("2. De-identify single text (synchronous)");
                Console.WriteLine("3. Tag single text (synchronous)");
                Console.WriteLine("4. Create de-identification batch job (asynchronous)");
                Console.WriteLine("5. Check job status");
                Console.WriteLine("6. Download processed files");
                Console.WriteLine("7. List jobs");
                Console.WriteLine("8. Exit");
                Console.Write("Select an option (1-8): ");

                var choice = Console.ReadLine();

                try
                {
                    switch (choice)
                    {
                        case "1":
                            await service.UploadLocalFilesToBlobAsync();
                            break;
                        case "2":
                            await service.DeidentifyTextAsync();
                            break;
                        case "3":
                            await service.TagTextAsync();
                            break;
                        case "4":
                            await service.CreateBatchJobAsync();
                            break;
                        case "5":
                            await service.CheckJobStatusAsync();
                            break;
                        case "6":
                            await service.DownloadProcessedFilesAsync();
                            break;
                        case "7":
                            await service.ListJobsAsync();
                            break;
                        case "8":
                            logger.LogInformation("Exiting application");
                            return;
                        default:
                            Console.WriteLine("Invalid option. Press any key to continue...");
                            Console.ReadKey();
                            break;
                    }
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Error executing option {Choice}", choice);
                    Console.WriteLine($"Error: {ex.Message}");
                    Console.WriteLine("Press any key to continue...");
                    Console.ReadKey();
                }
            }
        }
    }

    public class AzureDeidentificationService
    {
        private readonly IConfiguration _configuration;
        private readonly ILogger<AzureDeidentificationService> _logger;
        private readonly DeidentificationClient _deidentificationClient;
        private readonly BlobServiceClient _blobServiceClient;
        private readonly string _containerName;
        private readonly string _inputPrefix;
        private readonly string _outputPrefix;

        public AzureDeidentificationService(IConfiguration configuration, ILogger<AzureDeidentificationService> logger)
        {
            _configuration = configuration;
            _logger = logger;

            // Initialize clients
            var serviceEndpoint = _configuration["AzureHealthDeidentification:ServiceEndpoint"];
            var storageConnectionString = _configuration["AzureStorage:ConnectionString"];
            
            _containerName = _configuration["AzureStorage:ContainerName"];
            _inputPrefix = _configuration["AzureStorage:InputPrefix"];
            _outputPrefix = _configuration["AzureStorage:OutputPrefix"];

            _deidentificationClient = new DeidentificationClient(
                new Uri(serviceEndpoint),
                new DefaultAzureCredential()
            );

            _blobServiceClient = new BlobServiceClient(storageConnectionString);
        }

        public async Task UploadLocalFilesToBlobAsync()
        {
            Console.Write("Enter local directory path containing files to upload: ");
            var localPath = Console.ReadLine();

            if (string.IsNullOrEmpty(localPath) || !Directory.Exists(localPath))
            {
                Console.WriteLine("Invalid directory path.");
                Console.ReadKey();
                return;
            }

            var containerClient = _blobServiceClient.GetBlobContainerClient(_containerName);
            await containerClient.CreateIfNotExistsAsync(PublicAccessType.None);

            var files = Directory.GetFiles(localPath, "*", SearchOption.AllDirectories);
            var uploadTasks = new List<Task>();

            foreach (var filePath in files)
            {
                var fileName = Path.GetFileName(filePath);
                var blobName = $"{_inputPrefix}{fileName}";
                
                uploadTasks.Add(UploadFileAsync(containerClient, filePath, blobName));
            }

            await Task.WhenAll(uploadTasks);
            Console.WriteLine($"Successfully uploaded {files.Length} files to blob storage.");
            Console.ReadKey();
        }

        private async Task UploadFileAsync(BlobContainerClient containerClient, string filePath, string blobName)
        {
            var blobClient = containerClient.GetBlobClient(blobName);
            
            using var fileStream = File.OpenRead(filePath);
            await blobClient.UploadAsync(fileStream, overwrite: true);
            
            _logger.LogInformation("Uploaded {FileName} to {BlobName}", Path.GetFileName(filePath), blobName);
        }

        public async Task DeidentifyTextAsync()
        {
            Console.WriteLine("Enter text to de-identify (or press Enter for sample text):");
            var inputText = Console.ReadLine();
            
            if (string.IsNullOrEmpty(inputText))
            {
                inputText = "Patient John Doe, SSN: 123-45-6789, was born on 01/01/1980. Contact at john.doe@email.com or 555-123-4567.";
                Console.WriteLine($"Using sample text: {inputText}");
            }

            _logger.LogInformation("De-identifying text synchronously");

            var content = new DeidentificationContent(inputText);
            var result = await _deidentificationClient.DeidentifyAsync(content);

            Console.WriteLine("\n=== DE-IDENTIFICATION RESULT ===");
            Console.WriteLine($"Original: {inputText}");
            Console.WriteLine($"De-identified: {result.Value.OutputText}");
            
            Console.WriteLine("\nPress any key to continue...");
            Console.ReadKey();
        }

        public async Task TagTextAsync()
        {
            Console.WriteLine("Enter text to tag PHI entities (or press Enter for sample text):");
            var inputText = Console.ReadLine();
            
            if (string.IsNullOrEmpty(inputText))
            {
                inputText = "Patient John Doe, SSN: 123-45-6789, was born on 01/01/1980. Contact at john.doe@email.com or 555-123-4567.";
                Console.WriteLine($"Using sample text: {inputText}");
            }

            _logger.LogInformation("Tagging PHI entities in text");

            var content = new DeidentificationContent(inputText)
            {
                Operation = OperationType.Tag
            };
            
            var result = await _deidentificationClient.DeidentifyAsync(content);

            Console.WriteLine("\n=== PHI TAGGING RESULT ===");
            Console.WriteLine($"Original: {inputText}");
            Console.WriteLine($"Tagged: {result.Value.OutputText}");
            
            if (result.Value.Entities != null && result.Value.Entities.Count > 0)
            {
                Console.WriteLine("\nDetected PHI Entities:");
                foreach (var entity in result.Value.Entities)
                {
                    Console.WriteLine($"- {entity.Category}: '{inputText.Substring(entity.Offset, entity.Length)}' at position {entity.Offset}-{entity.Offset + entity.Length}");
                }
            }

            Console.WriteLine("\nPress any key to continue...");
            Console.ReadKey();
        }

        public async Task CreateBatchJobAsync()
        {
            Console.Write($"Enter job name (or press Enter for auto-generated): ");
            var jobName = Console.ReadLine();
            
            if (string.IsNullOrEmpty(jobName))
            {
                jobName = $"deid-job-{DateTime.Now:yyyyMMdd-HHmmss}";
            }

            Console.Write($"Enter input prefix (current: {_inputPrefix}): ");
            var inputPrefix = Console.ReadLine();
            if (string.IsNullOrEmpty(inputPrefix))
            {
                inputPrefix = _inputPrefix;
            }

            Console.Write($"Enter output prefix (current: {_outputPrefix}): ");
            var outputPrefix = Console.ReadLine();
            if (string.IsNullOrEmpty(outputPrefix))
            {
                outputPrefix = _outputPrefix;
            }

            _logger.LogInformation("Creating de-identification batch job: {JobName}", jobName);

            var storageAccountUri = new Uri(_blobServiceClient.Uri, _containerName);
            
            var job = new DeidentificationJob(
                new SourceStorageLocation(storageAccountUri, inputPrefix),
                new TargetStorageLocation(storageAccountUri, outputPrefix)
            );

            try
            {
                var response = await _deidentificationClient.CreateJobAsync(WaitUntil.Started, jobName, job);
                var createdJob = response.Value;

                Console.WriteLine("\n=== JOB CREATED ===");
                Console.WriteLine($"Job Name: {createdJob.Name}");
                Console.WriteLine($"Status: {createdJob.Status}");
                Console.WriteLine($"Created Time: {createdJob.CreatedDateTime}");
                Console.WriteLine($"Source: {createdJob.SourceLocation.Location}/{createdJob.SourceLocation.Prefix}");
                Console.WriteLine($"Target: {createdJob.TargetLocation.Location}/{createdJob.TargetLocation.Prefix}");
            }
            catch (RequestFailedException ex)
            {
                Console.WriteLine($"Failed to create job: {ex.Message}");
                _logger.LogError(ex, "Failed to create de-identification job");
            }

            Console.WriteLine("\nPress any key to continue...");
            Console.ReadKey();
        }

        public async Task CheckJobStatusAsync()
        {
            Console.Write("Enter job name to check status: ");
            var jobName = Console.ReadLine();

            if (string.IsNullOrEmpty(jobName))
            {
                Console.WriteLine("Job name is required.");
                Console.ReadKey();
                return;
            }

            try
            {
                var response = await _deidentificationClient.GetJobAsync(jobName);
                var job = response.Value;

                Console.WriteLine("\n=== JOB STATUS ===");
                Console.WriteLine($"Job Name: {job.Name}");
                Console.WriteLine($"Status: {job.Status}");
                Console.WriteLine($"Created Time: {job.CreatedDateTime}");
                Console.WriteLine($"Last Updated: {job.LastUpdatedDateTime}");
                Console.WriteLine($"Source: {job.SourceLocation.Location}/{job.SourceLocation.Prefix}");
                Console.WriteLine($"Target: {job.TargetLocation.Location}/{job.TargetLocation.Prefix}");

                if (job.Summary != null)
                {
                    Console.WriteLine($"\nSummary:");
                    Console.WriteLine($"- Total Files: {job.Summary.Total}");
                    Console.WriteLine($"- Successful: {job.Summary.Successful}");
                    Console.WriteLine($"- Failed: {job.Summary.Failed}");
                    Console.WriteLine($"- Canceled: {job.Summary.Canceled}");
                }

                if (job.Status == JobStatus.Failed || job.Status == JobStatus.PartiallyCompleted)
                {
                    Console.WriteLine("\nChecking for document-level errors...");
                    await ListJobDocumentsAsync(jobName);
                }
            }
            catch (RequestFailedException ex)
            {
                Console.WriteLine($"Job not found or error occurred: {ex.Message}");
                _logger.LogError(ex, "Failed to get job status for {JobName}", jobName);
            }

            Console.WriteLine("\nPress any key to continue...");
            Console.ReadKey();
        }

        public async Task ListJobsAsync()
        {
            _logger.LogInformation("Listing all de-identification jobs");

            try
            {
                var jobs = _deidentificationClient.GetJobsAsync();
                var jobList = new List<DeidentificationJob>();

                await foreach (var job in jobs)
                {
                    jobList.Add(job);
                }

                if (jobList.Count == 0)
                {
                    Console.WriteLine("No jobs found.");
                }
                else
                {
                    Console.WriteLine("\n=== ALL JOBS ===");
                    foreach (var job in jobList.OrderByDescending(j => j.CreatedDateTime))
                    {
                        Console.WriteLine($"Name: {job.Name} | Status: {job.Status} | Created: {job.CreatedDateTime}");
                    }
                }
            }
            catch (RequestFailedException ex)
            {
                Console.WriteLine($"Failed to list jobs: {ex.Message}");
                _logger.LogError(ex, "Failed to list jobs");
            }

            Console.WriteLine("\nPress any key to continue...");
            Console.ReadKey();
        }

        private async Task ListJobDocumentsAsync(string jobName)
        {
            try
            {
                var documents = _deidentificationClient.GetJobDocumentsAsync(jobName);
                await foreach (var document in documents)
                {
                    if (document.Status == OperationState.Failed)
                    {
                        Console.WriteLine($"Failed Document: {document.Input.Location} - Error: {document.Error?.Message}");
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to list job documents for {JobName}", jobName);
            }
        }

        public async Task DownloadProcessedFilesAsync()
        {
            Console.Write("Enter local directory to download processed files: ");
            var localPath = Console.ReadLine();

            if (string.IsNullOrEmpty(localPath))
            {
                Console.WriteLine("Local path is required.");
                Console.ReadKey();
                return;
            }

            Directory.CreateDirectory(localPath);

            var containerClient = _blobServiceClient.GetBlobContainerClient(_containerName);
            var blobs = containerClient.GetBlobsAsync(prefix: _outputPrefix);

            var downloadCount = 0;
            await foreach (var blob in blobs)
            {
                var blobClient = containerClient.GetBlobClient(blob.Name);
                var fileName = Path.GetFileName(blob.Name);
                var filePath = Path.Combine(localPath, fileName);

                using var fileStream = File.Create(filePath);
                await blobClient.DownloadToAsync(fileStream);
                
                downloadCount++;
                _logger.LogInformation("Downloaded {FileName} to {FilePath}", fileName, filePath);
            }

            Console.WriteLine($"Downloaded {downloadCount} processed files to {localPath}");
            Console.WriteLine("Press any key to continue...");
            Console.ReadKey();
        }
    }
}