import os
from pathlib import Path
from typing import Dict, List, Optional
from analyze_tableau_workbook import TableauWorkbookAnalyzer
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TableauWorkbookDocumentation:
    def __init__(self, workbook_path: str):
        """Initialize the documentation generator with a Tableau workbook path."""
        self.workbook_path = Path(workbook_path)
        self.analyzer = TableauWorkbookAnalyzer(workbook_path)
        
    def generate_documentation(self, output_path: Optional[str] = None) -> str:
        """
        Generate comprehensive documentation for a Tableau workbook.
        
        Args:
            output_path: Optional path to save the documentation. If not provided,
                        will return the documentation as a string.
        
        Returns:
            str: The generated documentation if output_path is None
        """
        # Extract all workbook information
        self.analyzer.extract_xml()
        self.analyzer.extract_parameters()
        self.analyzer.extract_data_sources()
        self.analyzer.extract_calculations()
        self.analyzer.extract_sheets()
        self.analyzer.extract_dashboards()
        self.analyzer.extract_actions()
        self.analyzer.extract_hierarchies()
        
        # Generate documentation
        doc = []
        
        # Title and metadata
        doc.append("# Tableau Workbook Documentation")
        doc.append(f"\nGenerated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        doc.append(f"Workbook: {self.workbook_path.name}\n")
        
        # Overview section
        doc.append("## Overview")
        doc.append(f"- Total Sheets: {len(self.analyzer.sheets)}")
        doc.append(f"- Total Dashboards: {len(self.analyzer.dashboards)}")
        doc.append(f"- Total Data Sources: {len(self.analyzer.data_sources)}")
        doc.append(f"- Total Calculations: {len(self.analyzer.calculations)}")
        doc.append(f"- Total Parameters: {len(self.analyzer.parameters)}")
        doc.append(f"- Total Actions: {len(self.analyzer.actions)}")
        doc.append(f"- Total Hierarchies: {len(self.analyzer.hierarchies)}\n")
        
        # Data Sources section
        doc.append("## Data Sources")
        for ds_name, ds in self.analyzer.data_sources.items():
            doc.append(f"\n### {ds_name}")
            doc.append(f"- Server: {ds.server}")
            doc.append(f"- Database: {ds.database}")
            doc.append("- Tables:")
            for table in ds.tables:
                doc.append(f"  - {table}")
        
        # Parameters section
        doc.append("\n## Parameters")
        for param_name, param in self.analyzer.parameters.items():
            doc.append(f"\n### {param_name}")
            doc.append(f"- Data Type: {param.datatype}")
            doc.append(f"- Current Value: {param.current_value}")
            if param.range_min and param.range_max:
                doc.append(f"- Range: {param.range_min} to {param.range_max}")
            if param.allowed_values:
                doc.append("- Allowed Values:")
                for value in param.allowed_values:
                    doc.append(f"  - {value}")
            doc.append("- Used In:")
            for usage in param.used_in:
                doc.append(f"  - {usage}")
        
        # Calculations section
        doc.append("\n## Calculations")
        for calc_name, calc in self.analyzer.calculations.items():
            doc.append(f"\n### {calc_name}")
            doc.append(f"- Formula: {calc.formula}")
            doc.append(f"- Data Source: {calc.datasource}")
            if calc.dependencies:
                doc.append("- Dependencies:")
                for dep in calc.dependencies:
                    doc.append(f"  - {dep}")
            if calc.parameters_used:
                doc.append("- Parameters Used:")
                for param in calc.parameters_used:
                    doc.append(f"  - {param}")
        
        # Sheets section
        doc.append("\n## Sheets")
        for sheet_name, sheet in self.analyzer.sheets.items():
            doc.append(f"\n### {sheet_name}")
            doc.append("- Data Sources:")
            for ds in sheet.datasources:
                doc.append(f"  - {ds}")
            if sheet.calculations_used:
                doc.append("- Calculations Used:")
                for calc in sheet.calculations_used:
                    doc.append(f"  - {calc}")
            if sheet.parameters_used:
                doc.append("- Parameters Used:")
                for param in sheet.parameters_used:
                    doc.append(f"  - {param}")
            if sheet.filters_used:
                doc.append("- Filters:")
                for filter_info in sheet.filters_used:
                    doc.append(f"  - {filter_info}")
        
        # Dashboards section
        doc.append("\n## Dashboards")
        for dash_name, dash in self.analyzer.dashboards.items():
            doc.append(f"\n### {dash_name}")
            doc.append("- Sheets:")
            for sheet in dash.sheets:
                doc.append(f"  - {sheet}")
        
        # Actions section
        doc.append("\n## Actions")
        for action_name, action in self.analyzer.actions.items():
            doc.append(f"\n### {action_name}")
            doc.append(f"- Type: {action.type}")
            doc.append(f"- Source Sheet: {action.source_sheet}")
            doc.append(f"- Target Sheet: {action.target_sheet}")
            if action.source_field:
                doc.append(f"- Source Field: {action.source_field}")
            if action.target_field:
                doc.append(f"- Target Field: {action.target_field}")
            if action.url:
                doc.append(f"- URL: {action.url}")
            if action.target_dashboard:
                doc.append(f"- Target Dashboard: {action.target_dashboard}")
        
        # Hierarchies section
        doc.append("\n## Hierarchies")
        for hier_name, hier in self.analyzer.hierarchies.items():
            doc.append(f"\n### {hier_name}")
            doc.append(f"- Data Source: {hier.datasource}")
            doc.append("- Levels:")
            for level in hier.levels:
                doc.append(f"  - {level}")
        
        # Join all sections
        documentation = "\n".join(doc)
        
        # Save to file if output path is provided
        if output_path:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(documentation)
            logger.info(f"Documentation saved to {output_path}")
        
        return documentation

def main():
    """Main function to demonstrate usage."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Generate documentation for a Tableau workbook')
    parser.add_argument('workbook_path', help='Path to the Tableau workbook (.twb or .twbx)')
    parser.add_argument('--output', '-o', help='Output path for the documentation (optional)')
    
    args = parser.parse_args()
    
    try:
        doc_generator = TableauWorkbookDocumentation(args.workbook_path)
        
        # Use default output path if none provided
        output_path = args.output
        if not output_path:
            output_dir = Path("output_files")
            output_dir.mkdir(exist_ok=True)
            output_path = output_dir / "workbook_documentation.md"
        
        documentation = doc_generator.generate_documentation(output_path)
        print(f"\nDocumentation generated successfully!")
        print(f"Output saved to: {output_path}")
        
        # Print a summary to console
        print("\nSummary:")
        print(f"- Total Sheets: {len(doc_generator.analyzer.sheets)}")
        print(f"- Total Dashboards: {len(doc_generator.analyzer.dashboards)}")
        print(f"- Total Data Sources: {len(doc_generator.analyzer.data_sources)}")
        print(f"- Total Calculations: {len(doc_generator.analyzer.calculations)}")
        print(f"- Total Parameters: {len(doc_generator.analyzer.parameters)}")
        print(f"- Total Actions: {len(doc_generator.analyzer.actions)}")
        print(f"- Total Hierarchies: {len(doc_generator.analyzer.hierarchies)}")
        
    except Exception as e:
        logger.error(f"Failed to generate documentation: {e}")
        raise

if __name__ == "__main__":
    main() 