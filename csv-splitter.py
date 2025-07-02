import csv
import os
import argparse
from pathlib import Path

def get_file_size_mb(file_path):
    """Get file size in MB."""
    return os.path.getsize(file_path) / (1024 * 1024)

def estimate_rows_per_chunk(input_file, target_size_mb):
    """
    Estimate how many rows should be in each chunk to achieve target size.
    
    Args:
        input_file (str): Path to input CSV file
        target_size_mb (float): Target chunk size in MB
    
    Returns:
        int: Estimated rows per chunk
    """
    total_size_mb = get_file_size_mb(input_file)
    
    # Count total rows (excluding header)
    with open(input_file, 'r', newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)  # Skip header
        total_rows = sum(1 for _ in reader)
    
    if total_rows == 0:
        return 1
    
    # Calculate approximate rows per MB
    rows_per_mb = total_rows / total_size_mb
    
    # Estimate rows needed for target size
    estimated_rows = int(rows_per_mb * target_size_mb)
    
    # Ensure at least 1 row per chunk
    return max(1, estimated_rows)

def split_csv_by_size(input_file, chunk_size_mb, output_dir=None, keep_header=True):
    """
    Split a large CSV file into smaller chunks based on file size.
    
    Args:
        input_file (str): Path to the input CSV file
        chunk_size_mb (float): Target size for each chunk in MB
        output_dir (str): Directory to save chunks (default: same as input file)
        keep_header (bool): Whether to include header in each chunk
    
    Returns:
        list: List of created chunk file paths
    """
    input_path = Path(input_file)
    
    if not input_path.exists():
        raise FileNotFoundError(f"Input file '{input_file}' not found")
    
    # Set output directory
    if output_dir is None:
        output_dir = input_path.parent
    else:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate output filename pattern
    base_name = input_path.stem
    extension = input_path.suffix
    
    # Estimate initial rows per chunk
    estimated_rows = estimate_rows_per_chunk(input_file, chunk_size_mb)
    print(f"Estimated ~{estimated_rows} rows per {chunk_size_mb}MB chunk")
    
    chunk_files = []
    chunk_number = 1
    target_size_bytes = chunk_size_mb * 1024 * 1024  # Convert MB to bytes
    
    with open(input_file, 'r', newline='', encoding='utf-8') as infile:
        reader = csv.reader(infile)
        
        # Read header
        try:
            header = next(reader)
        except StopIteration:
            raise ValueError("CSV file is empty")
        
        current_chunk = []
        
        for row in reader:
            current_chunk.append(row)
            
            # Check if we should write the chunk (every 100 rows to avoid frequent checks)
            if len(current_chunk) % 100 == 0:
                # Create temporary chunk to check size
                temp_chunk_path = output_dir / f"temp_chunk{extension}"
                write_chunk(temp_chunk_path, header if keep_header else None, current_chunk)
                current_size = os.path.getsize(temp_chunk_path)
                
                # If chunk exceeds target size, write it (but keep at least 100 rows)
                if current_size >= target_size_bytes and len(current_chunk) >= 100:
                    chunk_filename = f"{base_name}_chunk_{chunk_number:03d}{extension}"
                    chunk_path = output_dir / chunk_filename
                    
                    # Rename temp file to final chunk name
                    temp_chunk_path.rename(chunk_path)
                    chunk_files.append(str(chunk_path))
                    
                    chunk_size_mb_actual = current_size / (1024 * 1024)
                    print(f"Created chunk {chunk_number}: {chunk_filename} ({len(current_chunk)} rows, {chunk_size_mb_actual:.2f}MB)")
                    
                    # Reset for next chunk
                    current_chunk = []
                    chunk_number += 1
                else:
                    # Remove temp file if we're not done with this chunk
                    temp_chunk_path.unlink()
        
        # Write remaining rows if any
        if current_chunk:
            chunk_filename = f"{base_name}_chunk_{chunk_number:03d}{extension}"
            chunk_path = output_dir / chunk_filename
            
            write_chunk(chunk_path, header if keep_header else None, current_chunk)
            chunk_files.append(str(chunk_path))
            
            final_size_mb = get_file_size_mb(chunk_path)
            print(f"Created chunk {chunk_number}: {chunk_filename} ({len(current_chunk)} rows, {final_size_mb:.2f}MB)")
    
    # Clean up any remaining temp files
    temp_files = list(output_dir.glob("temp_chunk*"))
    for temp_file in temp_files:
        temp_file.unlink()
    
    total_chunks = len(chunk_files)
    total_output_size = sum(get_file_size_mb(f) for f in chunk_files)
    print(f"\nSplit complete! Created {total_chunks} chunk files totaling {total_output_size:.2f}MB.")
    
    return chunk_files

def write_chunk(output_path, header, rows):
    """Write a chunk of data to CSV file."""
    with open(output_path, 'w', newline='', encoding='utf-8') as outfile:
        writer = csv.writer(outfile)
        
        if header:
            writer.writerow(header)
        
        writer.writerows(rows)

def main():
    parser = argparse.ArgumentParser(description='Split a large CSV file into smaller chunks by file size')
    parser.add_argument('input_file', help='Path to the input CSV file')
    parser.add_argument('chunk_size_mb', type=float, help='Target size for each chunk in MB')
    parser.add_argument('-o', '--output-dir', help='Output directory for chunks')
    parser.add_argument('--no-header', action='store_true', 
                       help='Do not include header in each chunk')
    
    args = parser.parse_args()
    
    try:
        chunk_files = split_csv_by_size(
            input_file=args.input_file,
            chunk_size_mb=args.chunk_size_mb,
            output_dir=args.output_dir,
            keep_header=not args.no_header
        )
        
        print(f"\nChunk files created:")
        for chunk_file in chunk_files:
            file_size = get_file_size_mb(chunk_file)
            print(f"  - {chunk_file} ({file_size:.2f}MB)")
            
    except Exception as e:
        print(f"Error: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    # Example usage when run directly (uncomment to test)
    # split_csv_by_size('large_file.csv', chunk_size_mb=10.0)
    
    exit(main())
