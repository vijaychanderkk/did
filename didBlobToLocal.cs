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
            string folder = "sample-10k";
            string lastExecutedJob = null;

            
            //var blobAccessSignature = "<<sastoken>>";
            Uri storageAccountContainerUri = new("<<storageURL>>");
            string serviceEndpoint = "<<serviceendpoint>>";

           
            var timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
            logFilePath = Path.Combine("C:\\temp\\", $"deid_log_{timestamp}.txt");

            //Directory.CreateDirectory(outputFolder);

            try
            {
                DeidentificationClient client = new(
                    new Uri(serviceEndpoint),
                    new AzureCliCredential(),
                    new DeidentificationClientOptions()
                );

                Console.WriteLine("Available jobs:");
                var jobs = client.GetJobs();

                await LogMessage("--- DeID Process Started ---");
                await LogMessage($"Log File: {logFilePath}");

                var jobId = Guid.NewGuid().ToString().Substring(0, 25) + "-" + folder;
                await LogMessage($"Job ID: {jobId}");

                await LogMessage("Creating deidentification job...");
                DeidentificationJob job = new(
                    new SourceStorageLocation(storageAccountContainerUri, folder),
                    new TargetStorageLocation(storageAccountContainerUri, "sample-10k-output/")
                );

                await LogMessage($"Starting deidentification job with ID: {jobId}");

                // Start the job 
                var operation = await client.DeidentifyDocumentsAsync(
                    WaitUntil.Started,
                    jobId,
                    job
                );

                await LogMessage($"Job creation request submitted. Polling for status...");

                // Poll for job completion with timeout
                var startTime = DateTime.Now;
                var maxWaitTime = TimeSpan.FromHours(1); // 15 minute timeout
                var pollInterval = TimeSpan.FromSeconds(60); // Check every 10 seconds

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
                await LogMessage("--- DeID Process Completed ---");
            }
            catch (Exception ex)
            {
                await LogMessage($"ERROR: {ex.Message}");
                await LogMessage($"StackTrace: {ex.StackTrace}");
                throw;
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
