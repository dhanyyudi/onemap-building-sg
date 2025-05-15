# Singapore OneMap Building Data Workflow

This repository contains automated scripts to download, process, and maintain Singapore building data from OneMap API.

## Overview

The workflow handles the following tasks:
1. **Download** - Fetch building data from Singapore's OneMap API
2. **Compare** - Identify differences between the current dataset and a previous version
3. **Correct** - Fix duplicate postal codes and apply consistent naming conventions
4. **Report** - Generate statistics about the changes

The entire process is automated via GitHub Actions to run on the 15th of each month or manually as needed.

## Workflow Structure

```
.
├── .github/
│   └── workflows/
│       └── onemap_building_workflow.yml  # GitHub Actions workflow
├── data/
│   ├── onemap_04042025.csv              # Base dataset (existing)
│   ├── onemap_15052025.csv              # New dataset (will be generated)
│   ├── differences_onemap_*.csv         # Comparison results
│   └── correction_differences_*.csv     # Corrected differences
├── scripts/
│   ├── onemap_building_download.py      # Download script
│   ├── onemap_building_compare.py       # Comparison script
│   └── onemap_building_correct.py       # Correction script
└── README.md                           # This file
```

## Scripts

### 1. Download Script (`onemap_building_download.py`)

This script downloads building data from the OneMap API:
- Fetches data for all Singapore postal codes (range: 010000-829999)
- Extracts building information (block number, street, name, coordinates)
- Saves the raw data to a CSV file

```bash
python scripts/onemap_building_download.py --output_file "data/onemap_15052025.csv"
```

### 2. Comparison Script (`onemap_building_compare.py`)

This script compares the new dataset with a previous version:
- Identifies new buildings (not present in the previous dataset)
- Detects changes in building names or locations
- Adds a `change_type` column to indicate the type of change
- Generates a detailed comparison report

```bash
python scripts/onemap_building_compare.py \
  --previous_file "data/onemap_04042025.csv" \
  --current_file "data/onemap_15052025.csv" \
  --diff_output "data/differences_onemap_04042025-15052025.csv"
```

### 3. Correction Script (`onemap_building_correct.py`)

This script processes the differences file to ensure data quality:
- Removes duplicate postal codes by selecting the most likely "parent" building
- Classifies buildings as residential or non-residential
- Applies consistent naming conventions
- Formats addresses according to Singapore standards

```bash
python scripts/onemap_building_correct.py \
  --input_file "data/differences_onemap_04042025-15052025.csv" \
  --output_file "data/correction_differences_onemap_04042025-15052025.csv"
```

## Automated Workflow

The GitHub Actions workflow (`onemap_building_workflow.yml`) automates the entire process:
- Runs on the 15th of each month automatically
- Can be triggered manually via workflow dispatch
- Executes the download, comparison, and correction scripts in sequence
- Uploads the results as artifacts
- Commits the changes to the repository if needed

## Change Types

The comparison script identifies several types of changes:
- `new_building`: Building not present in the previous dataset
- `name_change`: Building name has changed
- `location_change`: Building location (lat/lon) has changed
- `name_and_location_change`: Both name and location have changed

## Output Files

The workflow generates these output files:
1. `onemap_DDMMYYYY.csv` - Full dataset from the latest download
2. `differences_onemap_*.csv` - Changes between the old and new datasets
3. `correction_differences_onemap_*.csv` - Corrected differences with proper naming

## Building Classification

Buildings are classified into two categories:
- **Residential** - Housing units, HDB blocks, private residential properties
- **Non-residential** - Commercial, educational, religious, government, and other non-housing buildings

Classification is based on analyzing building names, block numbers, and street names using pattern matching.

## Naming Conventions

The correction script applies these naming conventions:

### For Residential Buildings
- Format: `[Block Number] [Street Name]`
- Example: `123 Bedok North Road`

### For Non-Residential Buildings
- Format: `[Building Name]`
- Example: `Tampines Mall`
- For buildings with both block number and name: `[Building Name]`
- For buildings with no name: derived from street or location

### Address Format
- Standard format: `[Block Number] [Street Name], Singapore [Postal Code]`
- Non-residential with building name: `[Block Number] [Street Name] [Building Name], Singapore [Postal Code]`

## Deduplication Logic

To handle duplicate postal codes, the system uses a scoring method to identify the main "parent" building:

1. **Block Number Analysis**
   - Simple numeric blocks get higher scores
   - Single-digit blocks get the highest priority
   
2. **Building Name Analysis**
   - Names with parent building keywords (Mall, Plaza, Complex) get higher scores
   - Names with unit indicators (#01-23, Level 3) get lower scores
   
3. **Other Factors**
   - Data completeness
   - Street name length (shorter street names often indicate main buildings)

The record with the highest parent building score is selected as the main entry for a given postal code.

## Requirements

- Python 3.10+
- Required packages:
  - pandas
  - requests
  - tqdm
  - aiohttp
  - nest_asyncio
  - asyncio
  - logging

## Getting Started

1. Clone this repository
2. Install the required dependencies:
   ```bash
   pip install pandas requests tqdm aiohttp nest_asyncio
   ```
3. Place your existing OneMap data in the `data/` directory as `onemap_04042025.csv`
4. Run the workflow:
   ```bash
   # Run the entire workflow manually
   cd scripts
   python onemap_building_download.py
   python onemap_building_compare.py --previous_file "../data/onemap_04042025.csv" --current_file "../data/onemap_15052025.csv"
   python onemap_building_correct.py --input_file "../data/differences_onemap_04042025-15052025.csv" --output_file "../data/correction_differences_onemap_04042025-15052025.csv"
   ```

## Notes

- The OneMap API has rate limits. The download script implements concurrency control and retries to manage these limits.
- The full download process may take 1-2 hours depending on API responsiveness.
- Location changes are detected when the latitude or longitude differs by more than 0.0001 degrees (approximately 10 meters).

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- [OneMap Singapore](https://www.onemap.sg/) for providing the building data API
- The original notebook `SG Building Onemap Workflow-Update.ipynb` which provided the foundation for this automated workflow
