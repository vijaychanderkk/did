using Azure.Identity;
using Azure.Storage.Blobs;
using System.Threading.Tasks;
using System.Text;
using System.Linq;

namespace BlobToNetworkCopy
{
    internal class Program
    {
        private static string logFilePath;
        private static StringBuilder logBuffer = new StringBuilder();

        static async Task Main(string[] args)
        {
            // Get folder range from user input
            Console.WriteLine("Enter folder range to copy (e.g., 100-105): ");
            string folderRangeInput = Console.ReadLine();
            
            var (startFolder, endFolder) = ParseFolderRange(folderRangeInput);
            if (startFolder == -1 || endFolder == -1)
            {
                Console.WriteLine("Invalid folder range format. Please use format: 100-105");
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

            Uri storageAccountContainerUri = new("<<storageURL>>/deidlob");

            var timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
            logFilePath = Path.Combine(networkDrivePath, $"copy_log_{timestamp}.txt");

            try
            {
                await LogMessage("=== Blob to Network Drive Copy Started ===");
                await LogMessage($"Copying folders: {startFolder} to {endFolder}");
                await LogMessage($"Source: deidlob container (Azure Blob Storage)");
                await LogMessage($"Destination: {networkDrivePath}");

                // Process each folder in the range
                for (int folderNum = startFolder; folderNum <= endFolder; folderNum++)
                {
                    string sourceBlobPath = $"folder{folderNum}-output/";
                    string networkOutputPath = Path.Combine(networkDrivePath, $"folder{folderNum}-output");

                    await LogMessage($"\n--- Processing folder{folderNum} ---");
                    await LogMessage($"Source blob path: {sourceBlobPath}");
                    await LogMessage($"Destination path: {networkOutputPath}");
                    
                    // Create output directory on network drive
                    Directory.CreateDirectory(networkOutputPath);
                    await LogMessage($"Created directory: {networkOutputPath}");

                    // Copy files from blob storage to network drive
                    var copyResult = await CopyBlobToNetworkDrive(storageAccountContainerUri, sourceBlobPath, networkOutputPath);
                    
                    if (copyResult.Success)
                    {
                        await LogMessage($"Successfully copied {copyResult.FileCount} files ({copyResult.TotalSizeBytes} bytes)");
                        await LogMessage($"Files: {string.Join(", ", copyResult.FileNames)}");
                    }
                    else
                    {
                        await LogMessage($"Failed to copy folder{folderNum}: {copyResult.ErrorMessage}");
                    }

                    await LogMessage($"--- Completed folder{folderNum} ---");
                }

                await LogMessage("=== All Copy Operations Completed ===");
                
                // Save final log
                await SaveLogToFile();
                
                Console.WriteLine("\nCopy operations completed!");
                Console.WriteLine($"Check log file: {logFilePath}");
            }
            catch (Exception ex)
            {
                await LogMessage($"CRITICAL ERROR: {ex.Message}");
                await LogMessage($"StackTrace: {ex.StackTrace}");
                await SaveLogToFile();
                throw;
            }
        }

        private static async Task<CopyResult> CopyBlobToNetworkDrive(Uri containerUri, string blobPath, string networkPath)
        {
            var result = new CopyResult();
            
            try
            {
                // Create blob service client using the storage account base URL
                var storageAccountUri = new Uri($"{containerUri.Scheme}://{containerUri.Host}");
                BlobServiceClient blobServiceClient = new(storageAccountUri, new AzureCliCredential());
                var containerClient = blobServiceClient.GetBlobContainerClient("deidlob");

                await LogMessage($"Scanning blobs in: {blobPath}");

                // List all blobs in the output path
                var blobItems = new List<Azure.Storage.Blobs.Models.BlobItem>();
                await foreach (var blobItem in containerClient.GetBlobsAsync(prefix: blobPath))
                {
                    blobItems.Add(blobItem);
                }

                if (!blobItems.Any())
                {
                    result.ErrorMessage = "No files found in source path";
                    return result;
                }

                await LogMessage($"Found {blobItems.Count} files to copy");

                foreach (var blobItem in blobItems)
                {
                    try
                    {
                        var blobClient = containerClient.GetBlobClient(blobItem.Name);
                        
                        // Create relative path for network drive
                        string relativePath = blobItem.Name.Substring(blobPath.Length).TrimStart('/');
                        
                        // Skip if it's just a folder marker (empty file)
                        if (string.IsNullOrEmpty(relativePath))
                            continue;
                            
                        string networkFilePath = Path.Combine(networkPath, relativePath);
                        
                        // Create directory structure if needed
                        string networkFileDir = Path.GetDirectoryName(networkFilePath);
                        if (!string.IsNullOrEmpty(networkFileDir))
                        {
                            Directory.CreateDirectory(networkFileDir);
                        }

                        await LogMessage($"Copying: {relativePath}");

                        // Download blob to network drive
                        using (var fileStream = File.Create(networkFilePath))
                        {
                            await blobClient.DownloadToAsync(fileStream);
                        }

                        // Get file info
                        var fileInfo = new FileInfo(networkFilePath);
                        result.FileNames.Add(relativePath);
                        result.TotalSizeBytes += fileInfo.Length;
                        result.FileCount++;

                        await LogMessage($"Copied: {relativePath} ({fileInfo.Length} bytes)");
                    }
                    catch (Exception ex)
                    {
                        await LogMessage($"Error copying {blobItem.Name}: {ex.Message}");
                        result.ErrorFiles.Add($"{blobItem.Name}: {ex.Message}");
                    }
                }

                result.Success = result.FileCount > 0;
                if (result.ErrorFiles.Any())
                {
                    result.ErrorMessage = $"Some files failed: {string.Join("; ", result.ErrorFiles)}";
                }
            }
            catch (Exception ex)
            {
                result.ErrorMessage = $"Copy operation failed: {ex.Message}";
                await LogMessage($"Error in CopyBlobToNetworkDrive: {ex.Message}");
            }

            return result;
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
        }

        private static async Task SaveLogToFile()
        {
            try
            {
                string logContent;
                lock (logBuffer)
                {
                    logContent = logBuffer.ToString();
                }

                await File.WriteAllTextAsync(logFilePath, logContent);
                Console.WriteLine($"Log saved to: {logFilePath}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to save log file: {ex.Message}");
            }
        }
    }

    public class CopyResult
    {
        public bool Success { get; set; } = false;
        public int FileCount { get; set; } = 0;
        public long TotalSizeBytes { get; set; } = 0;
        public List<string> FileNames { get; set; } = new List<string>();
        public List<string> ErrorFiles { get; set; } = new List<string>();
        public string ErrorMessage { get; set; } = "";
    }
}
