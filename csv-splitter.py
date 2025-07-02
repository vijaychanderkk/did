import csv
import os
import argparse
from pathlib import Path

def split_csv_file(input_file, chunk_size, output_dir=None, keep_header=True):
    """
    Split a large CSV file into smaller chunks.
    
    Args:
        input_file (str): Path to the input CSV file
        chunk_size (int): Number of rows per chunk (excluding header)
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
    
    chunk_files = []
    chunk_number = 1
    
    with open(input_file, 'r', newline='', encoding='utf-8') as infile:
        reader = csv.reader(infile)
        
        # Read header
        try:
            header = next(reader)
        except StopIteration:
            raise ValueError("CSV file is empty")
        
        current_chunk = []
        row_count = 0
        
        for row in reader:
            current_chunk.append(row)
            row_count += 1
            
            # When chunk is full, write it to file
            if row_count == chunk_size:
                chunk_filename = f"{base_name}_chunk_{chunk_number:03d}{extension}"
                chunk_path = output_dir / chunk_filename
                
                write_chunk(chunk_path, header if keep_header else None, current_chunk)
                chunk_files.append(str(chunk_path))
                
                print(f"Created chunk {chunk_number}: {chunk_filename} ({row_count} rows)")
                
                # Reset for next chunk
                current_chunk = []
                row_count = 0
                chunk_number += 1
        
        # Write remaining rows if any
        if current_chunk:
            chunk_filename = f"{base_name}_chunk_{chunk_number:03d}{extension}"
            chunk_path = output_dir / chunk_filename
            
            write_chunk(chunk_path, header if keep_header else None, current_chunk)
            chunk_files.append(str(chunk_path))
            
            print(f"Created chunk {chunk_number}: {chunk_filename} ({len(current_chunk)} rows)")
    
    print(f"\nSplit complete! Created {len(chunk_files)} chunk files.")
    return chunk_files

def write_chunk(output_path, header, rows):
    """Write a chunk of data to CSV file."""
    with open(output_path, 'w', newline='', encoding='utf-8') as outfile:
        writer = csv.writer(outfile)
        
        if header:
            writer.writerow(header)
        
        writer.writerows(rows)

def main():
    parser = argparse.ArgumentParser(description='Split a large CSV file into smaller chunks')
    parser.add_argument('input_file', help='Path to the input CSV file')
    parser.add_argument('chunk_size', type=int, help='Number of rows per chunk')
    parser.add_argument('-o', '--output-dir', help='Output directory for chunks')
    parser.add_argument('--no-header', action='store_true', 
                       help='Do not include header in each chunk')
    
    args = parser.parse_args()
    
    try:
        chunk_files = split_csv_file(
            input_file=args.input_file,
            chunk_size=args.chunk_size,
            output_dir=args.output_dir,
            keep_header=not args.no_header
        )
        
        print(f"\nChunk files created:")
        for chunk_file in chunk_files:
            print(f"  - {chunk_file}")
            
    except Exception as e:
        print(f"Error: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    # Example usage when run directly (uncomment to test)
    # split_csv_file('large_file.csv', chunk_size=1000)
    
    exit(main())
