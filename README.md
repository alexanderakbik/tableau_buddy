# Tableau Workbook Analyzer

A Python tool for analyzing Tableau workbooks (.twb/.twbx) and generating comprehensive reports and DBT project suggestions.

Current version: 1.0.0

## Features

- Extracts and analyzes Tableau workbook components:
  - Data sources and their connections
  - Calculated fields and their dependencies
  - Worksheets and their components
  - Dashboards and their layouts
  - Parameters and their usage
  - Actions and their configurations
  - Hierarchies and their levels

- Generates detailed reports in CSV format:
  - Data sources report
  - Calculations report
  - Sheets report
  - Parameters report
  - Dashboards report
  - Actions report
  - Hierarchies report
  - Workbook summary

- Provides architectural recommendations:
  - Backend offload suggestions
  - Calculation consolidation opportunities
  - Hierarchy management recommendations
  - Filter optimization strategies
  - Parameter usage improvements
  - Data source integration suggestions

- Generates a suggested DBT project structure:
  - Staging models for each data source
  - Intermediate models for calculations
  - Mart models based on dashboard groupings
  - Source definitions and configurations

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/tableau_buddy.git
cd tableau_buddy
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Verify installation:
```bash
python analyze_tableau_workbook.py --version
```

## Usage

Run the analyzer on a Tableau workbook:
```bash
python analyze_tableau_workbook.py "path/to/your/workbook.twbx"
```

The tool will generate the following outputs in the `output_files` directory:
- CSV reports for each component type
- Architecture recommendations in Markdown format
- A suggested DBT project structure

## Output Structure

The `output_files` directory contains:
- `data_sources_report.csv`: Details of all data sources
- `calculations_report.csv`: All calculated fields and their dependencies
- `sheets_report.csv`: Worksheet information and their components
- `parameters_report.csv`: Parameter details and usage
- `dashboards_report.csv`: Dashboard layouts and contained sheets
- `actions_report.csv`: Action configurations
- `hierarchies_report.csv`: Hierarchy definitions
- `workbook_summary.csv`: Overall workbook statistics
- `backend_offload_recommendations.csv`: Suggestions for moving calculations to backend
- `architecture_recommendations.md`: Detailed architectural improvement suggestions
- `suggested_dbt_project/`: Generated DBT project structure

## DBT Project Structure

The generated DBT project includes:
- `models/staging/`: Source-specific staging models
- `models/intermediate/`: Calculation-based intermediate models
- `models/marts/`: Dashboard-aligned mart models
- `models/staging/sources.yml`: Source definitions
- `dbt_project.yml`: Project configuration

## Requirements

- Python 3.8 or higher
- Dependencies listed in requirements.txt:
  - pandas>=2.0.0
  - lxml>=4.9.0
  - numpy>=1.24.0
  - python-dateutil>=2.8.2
  - pytz>=2023.3
  - six>=1.16.0
  - tzdata>=2023.3

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.