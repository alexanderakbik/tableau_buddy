# Tableau Buddy

A Python tool for analyzing Tableau workbooks (.twb or .twbx files) and generating detailed reports about their structure and dependencies.

## Features

- Analyzes both .twb and .twbx files
- Extracts and reports on:
  - Data Sources
  - Calculated Fields
  - Worksheets
  - Parameters
  - Dashboards
  - Actions
  - Hierarchies
- Identifies dependencies between calculations
- Tracks parameter usage across sheets and calculations
- Generates detailed CSV reports for easy analysis

## Requirements

- Python 3.x
- Required packages:
  ```
  pandas>=2.0.0
  lxml>=4.9.0
  ```

## Installation

1. Clone this repository:
   ```bash
   git clone [repository-url]
   cd tableau-workbook-analyzer
   ```

2. Create and activate a virtual environment (recommended):
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On Windows, use: venv\Scripts\activate
   ```

3. Install the required packages:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

Run the analyzer by providing the path to your Tableau workbook:

```bash
python analyze_tableau_workbook.py path/to/your/workbook.twbx
```

Or for .twb files:

```bash
python analyze_tableau_workbook.py path/to/your/workbook.twb
```

## Output Reports

The tool generates seven CSV reports in the `output_files` directory:

1. `output_files/data_sources_report.csv`
   - Data source names
   - Server information
   - Database details
   - Tables/fields used

2. `output_files/calculations_report.csv`
   - Calculation names
   - Formulas
   - Usage in sheets
   - Dependencies
   - Parameter references
   - Cross-source calculations

3. `output_files/sheets_report.csv`
   - Sheet names
   - Data sources used
   - Calculations used
   - Parameters used

4. `output_files/parameters_report.csv`
   - Parameter names
   - Data types
   - Current values
   - Range/allowed values
   - Usage in sheets and calculations

5. `output_files/dashboards_report.csv`
   - Dashboard names
   - Contained sheets

6. `output_files/actions_report.csv`
   - Action names
   - Types
   - Source and target information
   - Field mappings

7. `output_files/hierarchies_report.csv`
   - Hierarchy names
   - Associated data sources
   - Level definitions

The `output_files` directory will be created automatically if it doesn't exist.

## Logging

The tool provides detailed logging at the DEBUG level, helping you track:
- File extraction process
- XML parsing details
- Element discovery and processing
- Any errors or issues encountered

## Error Handling

The tool includes robust error handling for:
- Missing or invalid files
- XML parsing issues
- Namespace resolution
- Missing or malformed elements

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Author

Alexander Akbik