# Catalogue Processor

A high-performance tool for processing XML files containing audio metadata and matching them with entries in a manifest file.

## Problem Solved

This tool solves the problem of extracting audio metadata (ISRC, track title, artist) from XML files and matching it with audio files listed in a manifest file. The solution avoids the SQLite threading issues in the previous implementation and ensures efficient processing of large datasets.

## Key Features

- **Fast Parallel Processing**: Process thousands of XML files efficiently using thread pooling
- **Memory-First Approach**: Avoids database locking and threading issues by keeping data in memory during processing 
- **Flexible File Support**: Handles CSV, JSON, and Excel manifest files
- **Robust Error Handling**: Continues processing even if some files fail
- **Performance Monitoring**: Tracks and reports processing times and success rates

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/Jopgood/catalogue-processor.git
   cd catalogue-processor
   ```

2. Create a virtual environment (optional but recommended):
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### Basic Usage

Run the tool with the following command:

```bash
python -m src.main --manifest /path/to/manifest.csv --xml-dir /path/to/xml/files --output /path/to/output.csv
```

### Command-Line Arguments

- `--manifest`: Path to the manifest file (CSV, JSON, or Excel)
- `--xml-dir`: Directory containing XML files to process
- `--output`: Path for the output file (format determined by file extension)
- `--max-workers`: Maximum number of worker threads (default: number of CPU cores)
- `--limit`: Limit the number of XML files to process (useful for testing)

### Example Commands

Test with a small sample (250 files):
```bash
python -m src.main --manifest data/manifest.csv --xml-dir data/xml_files --output data/output.csv --limit 250
```

Process all files with custom thread count:
```bash
python -m src.main --manifest data/manifest.csv --xml-dir data/xml_files --output data/output.json --max-workers 8
```

Output to Excel format:
```bash
python -m src.main --manifest data/manifest.csv --xml-dir data/xml_files --output data/output.xlsx
```

## How It Works

1. **Manifest Loading**: The tool loads the manifest file into memory and builds an index for fast lookups
2. **XML Processing**: XML files are processed in parallel using a thread pool to extract metadata
3. **Metadata Matching**: The extracted metadata is matched with entries in the manifest
4. **Output Generation**: The combined data is written to the specified output file

## Customization

### XML Element Paths

The tool attempts to automatically find metadata elements in XML files using common tag patterns. If your XML structure is different, you can modify the extraction methods in `src/xml_processor.py`:

- `_extract_audio_filename`
- `_extract_isrc`
- `_extract_track_title` 
- `_extract_artist`

### Performance Tuning

For optimal performance:

1. Adjust the `--max-workers` parameter based on your CPU resources
2. Process files in batches using the `--limit` parameter if memory is a concern
3. Choose the appropriate output format based on file size (CSV for largest files)

## Troubleshooting

### Common Issues

- **XML Processing Errors**: Check the log file for specific errors related to XML parsing
- **Memory Issues**: Reduce the `--max-workers` parameter or process files in smaller batches
- **File Not Found Errors**: Ensure file paths are correct and accessible

### Logs

The tool creates a log file (`catalogue_process.log`) that contains detailed information about the processing. Check this file for troubleshooting.

## Requirements

- Python 3.7+
- Pandas
- tqdm (for progress bars)
- lxml (for XML processing)
- openpyxl (for Excel support)

## Comparison with Previous Implementation

The previous implementation had issues with SQLite database connections being used across different threads, causing errors like:

```
SQLite objects created in a thread can only be used in that same thread. The object was created in thread id 28324 and this is thread id 14800.
```

This new implementation avoids these issues by:

1. Not using SQLite during the processing phase
2. Keeping all data in memory until the final write
3. Using a more efficient threading model with proper resource isolation
4. Implementing a file indexing system for faster lookups

## Performance Considerations

- Testing with 250 files should complete quickly (seconds to minutes depending on hardware)
- For the full 43,000 files, processing time will depend on CPU, memory, and storage speed
- The memory usage scales primarily with the size of the manifest file, not the number of XML files