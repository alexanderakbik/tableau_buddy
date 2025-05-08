import os
import pandas as pd
from lxml import etree
import zipfile
from typing import Dict, List, Optional, Any, Set
from dataclasses import dataclass
import logging
import sys
from pathlib import Path
import re

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class DataSource:
    name: str
    server: str
    database: str
    tables: List[str]

@dataclass
class Parameter:
    name: str
    datatype: str
    current_value: str
    range_min: Optional[str]
    range_max: Optional[str]
    allowed_values: List[str]  # For list parameters
    used_in: List[str]
    used_in_calculations: List[str]  # Track which calculations use this parameter

@dataclass
class Calculation:
    name: str
    formula: str
    used_in: List[str]
    dependencies: Set[str]  # Other calculations this one depends on
    parameters_used: Set[str]  # Parameters used in this calculation
    datasource: str  # Data source this calculation belongs to

@dataclass
class Sheet:
    name: str
    datasources: List[str]  # Changed from single datasource to list
    calculations_used: List[str]
    parameters_used: List[str]

@dataclass
class Dashboard:
    name: str
    sheets: List[str]

@dataclass
class Action:
    name: str
    type: str
    source_sheet: str
    target_sheet: str
    source_field: str
    target_field: str
    url: Optional[str]  # For URL actions
    target_dashboard: Optional[str]  # For navigation actions

@dataclass
class Hierarchy:
    name: str
    levels: List[str]
    datasource: str

class TableauWorkbookAnalyzer:
    # Support multiple versions of Tableau namespaces
    TABLEAU_NAMESPACES = [
        "http://tableausoftware.com/xml/user",
        "http://www.tableausoftware.com/xml/user",
        "http://tableau.com/xml/user"
    ]
    
    def __init__(self, workbook_path: str):
        self.workbook_path = Path(workbook_path)
        self.xml_content: Optional[str] = None
        self.root: Optional[etree._Element] = None
        self.data_sources: Dict[str, DataSource] = {}
        self.calculations: Dict[str, Calculation] = {}
        self.sheets: Dict[str, Sheet] = {}
        self.parameters: Dict[str, Parameter] = {}
        self.dashboards: Dict[str, Dashboard] = {}
        self.actions: Dict[str, Action] = {}
        self.hierarchies: Dict[str, Hierarchy] = {}
        self.ns: Dict[str, str] = {}  # Will be set during XML parsing

    def find_parameter_references(self, text: str) -> Set[str]:
        """Find all parameter references in a text, handling both bracketed and unbracketed forms.
        
        Uses improved regex pattern matching to find parameter references in formulas,
        avoiding false positives by checking word boundaries.
        """
        refs = set()
        if not text or not self.parameters:
            return refs
            
        # Check all parameter references using regex with word boundaries
        for param_name in self.parameters.keys():
            # Check for bracketed form - exact match with brackets
            bracketed_pattern = fr'\[{re.escape(param_name)}\]'
            if re.search(bracketed_pattern, text):
                refs.add(param_name)
                continue
                
            # Check for unbracketed form with word boundaries to avoid partial matches
            unbracketed_pattern = fr'\b{re.escape(param_name)}\b'
            if re.search(unbracketed_pattern, text):
                # Avoid false positives by checking common function names or keywords
                # that might match parameter names accidentally
                false_positive_prefixes = ['IF(', 'WHEN ', 'THEN ', 'ELSE ', 'END ', 'AND ', 'OR ', 'NOT ']
                if not any(prefix + param_name in text for prefix in false_positive_prefixes):
                    refs.add(param_name)
                    
        return refs

    def extract_xml(self) -> None:
        """Extract XML content from TWBX or TWB file."""
        try:
            if self.workbook_path.suffix.lower() == '.twbx':
                with zipfile.ZipFile(self.workbook_path, 'r') as zip_ref:
                    # List all files in the archive for debugging
                    files = zip_ref.namelist()
                    logger.info(f"Files in TWBX: {files}")
                    
                    # Find the .twb file
                    twb_files = [f for f in files if f.endswith('.twb')]
                    if not twb_files:
                        raise ValueError("No .twb file found in TWBX package")
                    
                    twb_file = twb_files[0]
                    logger.info(f"Using TWB file: {twb_file}")
                    self.xml_content = zip_ref.read(twb_file).decode('utf-8')
                    logger.debug(f"XML Content:\n{self.xml_content[:1000]}...")  # Print first 1000 chars
            else:
                with open(self.workbook_path, 'r', encoding='utf-8') as f:
                    self.xml_content = f.read()
                    logger.debug(f"XML Content:\n{self.xml_content[:1000]}...")  # Print first 1000 chars
            
            # Parse XML and determine the correct namespace
            parser = etree.XMLParser(recover=True)
            self.root = etree.fromstring(self.xml_content.encode('utf-8'), parser=parser)
            
            # Print root element info for debugging
            logger.debug(f"Root element tag: {self.root.tag}")
            logger.debug(f"Root element nsmap: {self.root.nsmap if hasattr(self.root, 'nsmap') else 'No nsmap'}")
            
            # Find the correct namespace and prefix
            ns_found = False
            if hasattr(self.root, 'nsmap'):
                for prefix, uri in self.root.nsmap.items():
                    if uri in self.TABLEAU_NAMESPACES:
                        # Use the actual prefix from the XML
                        self.ns = {prefix if prefix else "t": uri}
                        ns_found = True
                        break
            
            if not ns_found:
                # Try to find namespace in the root element's tag
                root_tag = self.root.tag
                for ns in self.TABLEAU_NAMESPACES:
                    if ns in root_tag:
                        self.ns = {"t": ns}
                        ns_found = True
                        break
            
            if not ns_found:
                logger.warning("No Tableau namespace found, using default")
                self.ns = {"t": self.TABLEAU_NAMESPACES[0]}
            
            logger.info(f"Using namespace: {self.ns}")
            
            # Verify we can find basic elements
            ns_prefix = next(iter(self.ns.keys()))
            try:
                # Try with the detected namespace
                datasources = self.root.xpath(f'.//{ns_prefix}:datasource', namespaces=self.ns)
                worksheets = self.root.xpath(f'.//{ns_prefix}:worksheets/{ns_prefix}:worksheet', namespaces=self.ns)
                
                if not datasources and not worksheets:
                    # Try with 'user' namespace explicitly
                    self.ns = {'user': self.TABLEAU_NAMESPACES[0]}
                    datasources = self.root.xpath('.//user:datasource', namespaces=self.ns)
                    worksheets = self.root.xpath('.//user:worksheets/user:worksheet', namespaces=self.ns)
                
                logger.info(f"Found {len(datasources)} datasources and {len(worksheets)} worksheets")
                
                # Print first datasource and worksheet for debugging
                if datasources:
                    logger.debug(f"First datasource: {etree.tostring(datasources[0], pretty_print=True)}")
                if worksheets:
                    logger.debug(f"First worksheet: {etree.tostring(worksheets[0], pretty_print=True)}")
            except Exception as e:
                logger.warning(f"Initial XPath query failed: {e}")
                # Try alternative XPath without namespace
                datasources = self.root.xpath('.//datasource')
                worksheets = self.root.xpath('.//worksheets/worksheet')
                logger.info(f"Found {len(datasources)} datasources and {len(worksheets)} worksheets using fallback XPath")
            
        except Exception as e:
            logger.error(f"Failed to extract XML from workbook: {e}")
            raise

    def _xpath(self, element: etree._Element, path: str) -> List[etree._Element]:
        """Helper method to execute XPath with the correct namespace prefix."""
        try:
            # Try with the current namespace
            ns_prefix = next(iter(self.ns.keys()))
            path_with_ns = path.replace('t:', f'{ns_prefix}:')
            result = element.xpath(path_with_ns, namespaces=self.ns)
            if result:
                return result
            
            # Try with 'user' namespace if current namespace failed
            path_with_user = path.replace('t:', 'user:')
            self.ns = {'user': self.TABLEAU_NAMESPACES[0]}
            result = element.xpath(path_with_user, namespaces=self.ns)
            if result:
                return result
            
            # Try without namespace as last resort
            return element.xpath(path.replace('t:', ''))
        except Exception as e:
            logger.warning(f"XPath query failed: {e}")
            return []

    def _find(self, element: etree._Element, path: str) -> Optional[etree._Element]:
        """Helper method to find an element with the correct namespace prefix."""
        try:
            # Try with the current namespace
            ns_prefix = next(iter(self.ns.keys()))
            path_with_ns = path.replace('t:', f'{ns_prefix}:')
            result = element.find(path_with_ns, namespaces=self.ns)
            if result is not None:
                return result
            
            # Try with 'user' namespace if current namespace failed
            path_with_user = path.replace('t:', 'user:')
            self.ns = {'user': self.TABLEAU_NAMESPACES[0]}
            result = element.find(path_with_user, namespaces=self.ns)
            if result is not None:
                return result
            
            # Try without namespace as last resort
            return element.find(path.replace('t:', ''))
        except Exception as e:
            logger.warning(f"Find query failed: {e}")
            return None

    def extract_parameters(self) -> None:
        """Extract parameters and their usage."""
        try:
            parameters = self._xpath(self.root, './/t:parameter')
            for param in parameters:
                param_name = param.get('name', 'Unknown')
                datatype = param.get('datatype', '')
                current_value = param.get('current-value', '')
                
                range_elem = self._find(param, './/t:range')
                range_min = range_elem.get('min') if range_elem is not None else None
                range_max = range_elem.get('max') if range_elem is not None else None
                
                # Extract allowed values for list parameters
                allowed_values = []
                list_elem = self._find(param, './/t:list')
                if list_elem is not None:
                    members = self._xpath(list_elem, './/t:member')
                    allowed_values = [m.get('value', '') for m in members if m.get('value')]
                
                self.parameters[param_name] = Parameter(
                    name=param_name,
                    datatype=datatype,
                    current_value=current_value,
                    range_min=range_min,
                    range_max=range_max,
                    allowed_values=allowed_values,
                    used_in=[],
                    used_in_calculations=[]
                )
        except Exception as e:
            logger.error(f"Failed to extract parameters: {e}")
            raise
    
    def extract_data_sources(self) -> None:
        """Extract information about data sources."""
        try:
            # Handle both regular and federated datasources
            datasources = self._xpath(self.root, './/t:datasource')
            logger.info(f"Processing {len(datasources)} datasources")
            
            for ds in datasources:
                ds_name = ds.get('name', ds.get('caption', 'Unknown'))
                logger.debug(f"Processing datasource: {ds_name}")
                
                # Skip if we've already processed this datasource
                if ds_name in self.data_sources:
                    continue
                
                # Handle both direct and federated connections
                connection = None
                server = ''
                database = ''
                
                # Try federated connection first
                named_conns = self._xpath(ds, './/t:named-connection/t:connection')
                if named_conns:
                    connection = named_conns[0]
                    server = connection.get('server', '')
                    database = connection.get('filename', connection.get('database', ''))
                else:
                    # Try direct connection
                    connection = self._find(ds, './/t:connection')
                    if connection is not None:
                        server = connection.get('server', '')
                        database = connection.get('database', '')
                
                # Extract columns/fields
                tables = []
                
                # Try different column/field paths
                column_paths = [
                    './/t:column',
                    './/t:field',
                    './/t:metadata-record[@class="column"]'
                ]
                
                for path in column_paths:
                    columns = self._xpath(ds, path)
                    for column in columns:
                        column_name = column.get('name', column.get('caption', ''))
                        if column_name:
                            tables.append(column_name.strip('[]'))
                
                # Remove duplicates while preserving order
                tables = list(dict.fromkeys(tables))
                
                logger.debug(f"Found {len(tables)} columns in datasource {ds_name}")
                
                self.data_sources[ds_name] = DataSource(
                    name=ds_name,
                    server=server,
                    database=database,
                    tables=tables
                )
        except Exception as e:
            logger.error(f"Failed to extract data sources: {e}")
            raise
    
    def extract_calculations(self) -> None:
        """Extract calculated fields and their formulas."""
        try:
            # First pass: Create calculation objects
            for ds in self._xpath(self.root, './/t:datasource'):
                datasource_name = ds.get('name', ds.get('caption', 'Unknown'))
                for column in self._xpath(ds, './/t:column'):
                    calc = self._find(column, './/t:calculation')
                    if calc is not None:
                        calc_name = column.get('name', 'Unknown').strip('[]')
                        formula = calc.get('formula', '').strip()
                        
                        # Find parameter references in formula
                        param_refs = self.find_parameter_references(formula)

                        # Create calculation object
                        self.calculations[calc_name] = Calculation(
                            name=calc_name,
                            formula=formula,
                            used_in=[],  # Will be populated in extract_sheets
                            dependencies=set(),  # Will be populated in second pass
                            parameters_used=param_refs,
                            datasource=datasource_name
                        )
                        
                        # Update parameter's used_in_calculations list
                        for param_name in param_refs:
                            if param_name in self.parameters:
                                if calc_name not in self.parameters[param_name].used_in_calculations:
                                    self.parameters[param_name].used_in_calculations.append(calc_name)

            # Second pass: Find dependencies between calculations
            for calc_name, calc in self.calculations.items():
                # Check each formula for references to other calculations
                for other_calc_name in list(self.calculations.keys()):  # Use list to avoid dict size changes during iteration
                    if other_calc_name == calc_name:
                        continue
                        
                    # Check different reference patterns in formulas
                    # 1. Bracketed reference: "[OtherCalc]"
                    bracketed_pattern = fr'\[{re.escape(other_calc_name)}\]'
                    # 2. Unbracketed with dot notation (e.g., "DataSource.OtherCalc")
                    dotted_pattern = fr'\b{re.escape(other_calc_name)}\b'
                    
                    if (re.search(bracketed_pattern, calc.formula) or 
                        (re.search(dotted_pattern, calc.formula) and not any(kw + other_calc_name in calc.formula 
                                                                            for kw in ['IF(', 'WHEN ', 'THEN ', 'ELSE ']))):
                        # Add to dependencies
                        calc.dependencies.add(other_calc_name)
                        
                        # Propagate parameter references from dependent calculations
                        if other_calc_name in self.calculations:
                            # Get parameters from dependency
                            other_calc = self.calculations[other_calc_name]
                            # Update parameters_used with parameters from dependency
                            calc.parameters_used.update(other_calc.parameters_used)
                            
                            # Recursively get parameters from nested dependencies
                            self._propagate_parameters(calc, other_calc_name, visited=set())
        except Exception as e:
            logger.error(f"Failed to extract calculations: {e}")
            raise
            
    def _propagate_parameters(self, calc: Calculation, dependency_name: str, visited: Set[str]) -> None:
        """Recursively propagate parameter references from dependencies."""
        # Avoid circular dependencies
        if dependency_name in visited:
            return
            
        visited.add(dependency_name)
        
        # Get the dependency calculation
        if dependency_name not in self.calculations:
            return
            
        dependency = self.calculations[dependency_name]
        
        # Add parameters from this dependency
        calc.parameters_used.update(dependency.parameters_used)
        
        # Recursively process nested dependencies
        for nested_dep in dependency.dependencies:
            if nested_dep != calc.name:  # Avoid self-references
                self._propagate_parameters(calc, nested_dep, visited)
    
    def extract_sheets(self) -> None:
        """Extract information about worksheets and their dependencies."""
        try:
            worksheets = self._xpath(self.root, '//t:worksheets/t:worksheet')
            logger.info(f"Processing {len(worksheets)} worksheets")
            
            for ws in worksheets:
                sheet_name = ws.get('name', 'Unknown')
                logger.debug(f"Processing worksheet: {sheet_name}")
                
                # Find all datasources used in the sheet
                datasources = set()
                
                # Check datasources explicitly listed in the view
                ds_elements = self._xpath(ws, './/t:datasources/t:datasource')
                for ds in ds_elements:
                    ds_name = ds.get('name', ds.get('caption', ''))
                    if ds_name:
                        datasources.add(ds_name)
                
                # Find calculations and parameters used in the sheet
                calculations_used = []
                parameters_used = []
                
                # ENHANCE: Check more places calculations might be used in worksheets
                # 1. Check all fields in view-items (main visualization)
                self._check_fields_in_element(ws, './/t:view-item', 'column', sheet_name, 
                                          calculations_used, parameters_used, datasources)
                
                # 2. Check fields in shelves (columns/rows)
                self._check_fields_in_element(ws, './/t:shelf', 'name', sheet_name,
                                          calculations_used, parameters_used, datasources)
                
                # 3. Check fields in marks cards
                self._check_fields_in_element(ws, './/t:encodings/t:encoding', 'field', sheet_name,
                                          calculations_used, parameters_used, datasources)
                
                # 4. Check fields in filters
                self._check_fields_in_element(ws, './/t:filter', 'column', sheet_name,
                                          calculations_used, parameters_used, datasources)
                                          
                # 5. Check fields in tooltips
                self._check_fields_in_element(ws, './/t:tooltip-field', 'name', sheet_name,
                                          calculations_used, parameters_used, datasources)
                
                # Add parameters used in calculations (including indirect usage via calculation dependencies)
                # First collect all parameters directly referenced
                direct_params = set(parameters_used)
                
                # Then add parameters used indirectly through calculations
                for calc_name in calculations_used:
                    if calc_name in self.calculations:
                        calc = self.calculations[calc_name]
                        # Add all parameters used by this calculation
                        for param_name in calc.parameters_used:
                            if param_name in self.parameters and param_name not in parameters_used:
                                parameters_used.append(param_name)
                                # Also update the parameter.used_in list for bidirectional reference
                                if sheet_name not in self.parameters[param_name].used_in:
                                    self.parameters[param_name].used_in.append(sheet_name)
                
                # Remove duplicates and sort for consistency
                calculations_used = sorted(list(set(calculations_used)))
                parameters_used = sorted(list(set(parameters_used)))
                
                self.sheets[sheet_name] = Sheet(
                    name=sheet_name,
                    datasources=sorted(list(datasources)),
                    calculations_used=calculations_used,
                    parameters_used=parameters_used
                )
                
                logger.debug(f"Sheet {sheet_name} uses {len(calculations_used)} calculations, {len(parameters_used)} parameters, and {len(datasources)} datasources")
        except Exception as e:
            logger.error(f"Failed to extract sheets: {e}")
            raise
            
    def _check_fields_in_element(self, worksheet, xpath_expr, attr_name, sheet_name, 
                               calculations_used, parameters_used, datasources):
        """Helper to check for fields used in various worksheet elements.
        
        Args:
            worksheet: The worksheet element to search within
            xpath_expr: XPath to find elements that might use fields/calculations
            attr_name: Attribute name containing field references
            sheet_name: Name of the current sheet
            calculations_used: List to append found calculations to
            parameters_used: List to append found parameters to
            datasources: Set to add datasources to
        """
        elements = self._xpath(worksheet, xpath_expr)
        for element in elements:
            # Get the field name and clean it
            field_name = element.get(attr_name, '').strip('[]')
            if not field_name:
                continue
                
            # Check if it's a calculation and update bidirectional references
            if field_name in self.calculations:
                if field_name not in calculations_used:
                    calculations_used.append(field_name)
                
                # Update calculation's used_in list (bidirectional reference)
                if sheet_name not in self.calculations[field_name].used_in:
                    self.calculations[field_name].used_in.append(sheet_name)
                
                # Add datasource from the calculation
                calc = self.calculations[field_name]
                if calc.datasource:
                    datasources.add(calc.datasource)
                    
                # Also check if this calculation depends on other calculations
                # and add those to the list as well (indirect usage)
                self._add_dependent_calculations(field_name, sheet_name, calculations_used)
            
            # Check if it's a parameter and update bidirectional references
            elif field_name in self.parameters:
                if field_name not in parameters_used:
                    parameters_used.append(field_name)
                
                # Update parameter's used_in list (bidirectional reference)
                if sheet_name not in self.parameters[field_name].used_in:
                    self.parameters[field_name].used_in.append(sheet_name)
                    
    def _add_dependent_calculations(self, calc_name, sheet_name, calculations_used, visited=None):
        """Recursively add calculations that the given calculation depends on."""
        if visited is None:
            visited = set()
            
        # Avoid circular dependencies
        if calc_name in visited:
            return
            
        visited.add(calc_name)
        
        if calc_name not in self.calculations:
            return
            
        # Get dependencies of this calculation
        calc = self.calculations[calc_name]
        for dep_name in calc.dependencies:
            if dep_name not in calculations_used and dep_name in self.calculations:
                # Add to the list of calculations used in this sheet
                calculations_used.append(dep_name)
                
                # Update the calculation's used_in list (bidirectional reference)
                if sheet_name not in self.calculations[dep_name].used_in:
                    self.calculations[dep_name].used_in.append(sheet_name)
                    
                # Recursively add nested dependencies
                self._add_dependent_calculations(dep_name, sheet_name, calculations_used, visited)

    def extract_dashboards(self) -> None:
        """Extract dashboard information."""
        try:
            dashboards = self._xpath(self.root, '//t:dashboards/t:dashboard')
            logger.info(f"Processing {len(dashboards)} dashboards")
            
            for dashboard in dashboards:
                dashboard_name = dashboard.get('name', 'Unknown')
                sheets = []
                
                items = self._xpath(dashboard, './/t:dashboard-item')
                for item in items:
                    if item.get('type') == 'worksheet':
                        sheet_name = item.get('name', '')
                        if sheet_name:
                            sheets.append(sheet_name)
                
                self.dashboards[dashboard_name] = Dashboard(
                    name=dashboard_name,
                    sheets=sheets
                )
                
                logger.debug(f"Dashboard {dashboard_name} contains sheets: {sheets}")
        except Exception as e:
            logger.error(f"Failed to extract dashboards: {e}")
            raise

    def extract_actions(self) -> None:
        """Extract action information."""
        try:
            actions = self._xpath(self.root, './/t:action')
            for action in actions:
                action_name = action.get('name', 'Unknown')
                action_type = action.get('type', '')
                
                source_sheet = self._find(action, './/t:source-sheet')
                target_sheet = self._find(action, './/t:target-sheet')
                source_field = self._find(action, './/t:source-field')
                target_field = self._find(action, './/t:target-field')
                url = self._find(action, './/t:url')
                target_dashboard = self._find(action, './/t:target-dashboard')
                
                self.actions[action_name] = Action(
                    name=action_name,
                    type=action_type,
                    source_sheet=source_sheet.text if source_sheet is not None else '',
                    target_sheet=target_sheet.text if target_sheet is not None else '',
                    source_field=source_field.text if source_field is not None else '',
                    target_field=target_field.text if target_field is not None else '',
                    url=url.text if url is not None else None,
                    target_dashboard=target_dashboard.text if target_dashboard is not None else None
                )
        except Exception as e:
            logger.error(f"Failed to extract actions: {e}")
            raise

    def extract_hierarchies(self) -> None:
        """Extract hierarchy information."""
        try:
            hierarchies = self._xpath(self.root, './/t:hierarchy')
            for hierarchy in hierarchies:
                hierarchy_name = hierarchy.get('name', 'Unknown')
                levels = []
                
                # Extract levels
                level_elements = self._xpath(hierarchy, './/t:level')
                for level in level_elements:
                    level_name = level.get('name', '')
                    if level_name:
                        levels.append(level_name)
                
                # Find datasource by looking for the fields in the hierarchy
                datasource_name = ''
                for ds_name, ds in self.data_sources.items():
                    if any(level in ds.tables for level in levels):
                        datasource_name = ds_name
                        break
                
                self.hierarchies[hierarchy_name] = Hierarchy(
                    name=hierarchy_name,
                    levels=levels,
                    datasource=datasource_name
                )
        except Exception as e:
            logger.error(f"Failed to extract hierarchies: {e}")
            raise

class ReportGenerator:
    @staticmethod
    def generate_data_sources_report(data_sources: Dict[str, DataSource], output_path: str) -> None:
        """Generate CSV report for data sources."""
        ds_data = [
            {
                'Data Source Name': ds.name,
                'Server': ds.server,
                'Database': ds.database,
                'Tables': ', '.join(ds.tables)
            }
            for ds in data_sources.values()
        ]
        pd.DataFrame(ds_data).to_csv(output_path, index=False)
    
    @staticmethod
    def generate_calculations_report(calculations: Dict[str, Calculation], output_path: str) -> None:
        """Generate CSV report for calculations."""
        calc_data = []
        
        for calc_name, calc in calculations.items():
            # Ensure no duplicates in lists - convert to sets and back to lists for sorting
            used_in = sorted(list(set(calc.used_in))) if calc.used_in else []
            dependencies = sorted(list(set(calc.dependencies))) if calc.dependencies else []
            params_used = sorted(list(set(calc.parameters_used))) if calc.parameters_used else []
            
            # Determine if calculation references fields from other datasources
            cross_source = 'No'
            if any('.' in dep for dep in dependencies):
                cross_source = 'Yes'
                
            calc_data.append({
                'Calculation Name': calc.name,
                'Data Source': calc.datasource,
                'Formula': calc.formula,
                'Used In Sheets': ', '.join(used_in) if used_in else 'None',
                'Dependencies': ', '.join(dependencies) if dependencies else 'None',
                'Parameters Used': ', '.join(params_used) if params_used else 'None',
                'Cross-Source': cross_source,
                'Dependency Count': len(dependencies),
                'Sheet Usage Count': len(used_in)
            })
        
        # Sort calculations by dependency count (most complex first)
        calc_data.sort(key=lambda x: (-x['Dependency Count'], -x['Sheet Usage Count'], x['Calculation Name']))
        
        # Remove temporary columns used for sorting
        for item in calc_data:
            del item['Dependency Count']
            del item['Sheet Usage Count']
            
        pd.DataFrame(calc_data).to_csv(output_path, index=False)
    
    @staticmethod
    def generate_sheets_report(sheets: Dict[str, Sheet], output_path: str) -> None:
        """Generate CSV report for sheets."""
        sheet_data = []
        
        for sheet_name, sheet in sheets.items():
            # Remove duplicates and sort lists for consistency
            datasources = sorted(list(set(sheet.datasources)))
            calcs_used = sorted(list(set(sheet.calculations_used)))
            params_used = sorted(list(set(sheet.parameters_used))) if sheet.parameters_used else []
            
            sheet_data.append({
                'Sheet Name': sheet.name,
                'Data Sources': ', '.join(datasources) if datasources else 'None',
                'Calculations Used': ', '.join(calcs_used) if calcs_used else 'None',
                'Parameters Used': ', '.join(params_used) if params_used else 'None',
                'Calculation Count': len(calcs_used),
                'Parameter Count': len(params_used)
            })
        
        # Sort sheets by complexity (number of calculations and parameters)
        sheet_data.sort(key=lambda x: (-x['Calculation Count'], -x['Parameter Count'], x['Sheet Name']))
        
        # Remove temporary columns used for sorting
        for item in sheet_data:
            del item['Calculation Count']
            del item['Parameter Count']
        
        pd.DataFrame(sheet_data).to_csv(output_path, index=False)

    @staticmethod
    def generate_parameters_report(parameters: Dict[str, Parameter], output_path: str) -> None:
        """Generate CSV report for parameters."""
        param_data = []
        
        for param_name, param in parameters.items():
            # Remove duplicates and sort lists
            used_in = sorted(list(set(param.used_in))) if param.used_in else []
            used_in_calcs = sorted(list(set(param.used_in_calculations))) if param.used_in_calculations else []
            
            # Format range/values appropriately
            range_values = ''
            if param.range_min or param.range_max:
                range_values = f"{param.range_min or ''} to {param.range_max or ''}"
            elif param.allowed_values:
                range_values = ', '.join(param.allowed_values)
            else:
                range_values = 'N/A'
                
            param_data.append({
                'Parameter Name': param.name,
                'Data Type': param.datatype,
                'Current Value': param.current_value,
                'Range/Values': range_values,
                'Used In Sheets': ', '.join(used_in) if used_in else 'None',
                'Used In Calculations': ', '.join(used_in_calcs) if used_in_calcs else 'None',
                'Usage Count': len(used_in) + len(used_in_calcs)
            })
        
        # Sort parameters by usage (most used first)
        param_data.sort(key=lambda x: (-x['Usage Count'], x['Parameter Name']))
        
        # Remove temporary column used for sorting
        for item in param_data:
            del item['Usage Count']
            
        pd.DataFrame(param_data).to_csv(output_path, index=False)

    @staticmethod
    def generate_dashboards_report(dashboards: Dict[str, Dashboard], output_path: str) -> None:
        """Generate CSV report for dashboards."""
        dashboard_data = [
            {
                'Dashboard Name': dashboard.name,
                'Sheets': ', '.join(sorted(list(set(dashboard.sheets)))) if dashboard.sheets else 'None',
                'Sheet Count': len(dashboard.sheets)
            }
            for dashboard in dashboards.values()
        ]
        
        # Sort dashboards by complexity (number of sheets)
        dashboard_data.sort(key=lambda x: (-x['Sheet Count'], x['Dashboard Name']))
        
        # Remove temporary column used for sorting
        for item in dashboard_data:
            del item['Sheet Count']
            
        pd.DataFrame(dashboard_data).to_csv(output_path, index=False)

    @staticmethod
    def generate_actions_report(actions: Dict[str, Action], output_path: str) -> None:
        """Generate CSV report for actions."""
        action_data = []
        
        for action_name, action in actions.items():
            # Determine the action type properly
            action_type = action.type
            if not action_type:
                if action.url:
                    action_type = 'URL'
                elif action.target_dashboard:
                    action_type = 'Navigation'
                elif action.target_sheet and action.target_field:
                    action_type = 'Filter'
                else:
                    action_type = 'Unknown'
                    
            # Determine the target appropriately
            target = action.target_sheet or action.target_dashboard or action.url or ''
                    
            action_data.append({
                'Action Name': action.name,
                'Type': action_type,
                'Source Sheet': action.source_sheet,
                'Target': target,
                'Source Field': action.source_field,
                'Target Field': action.target_field
            })
            
        # Sort actions by type and name
        action_data.sort(key=lambda x: (x['Type'], x['Action Name']))
        
        pd.DataFrame(action_data).to_csv(output_path, index=False)

    @staticmethod
    def generate_hierarchies_report(hierarchies: Dict[str, Hierarchy], output_path: str) -> None:
        """Generate CSV report for hierarchies."""
        hierarchy_data = [
            {
                'Hierarchy Name': hierarchy.name,
                'Data Source': hierarchy.datasource,
                'Levels': ' → '.join(hierarchy.levels),
                'Level Count': len(hierarchy.levels)
            }
            for hierarchy in hierarchies.values()
        ]
        

def main():
    if len(sys.argv) != 2:
        logger.error("Usage: python analyze_tableau_workbook.py <path_to_workbook>")
        sys.exit(1)
    
    workbook_path = sys.argv[1]
    if not os.path.exists(workbook_path):
        logger.error(f"Workbook file '{workbook_path}' not found.")
        sys.exit(1)
    
    try:
        # Create output directory if it doesn't exist
        output_dir = "output_files"
        os.makedirs(output_dir, exist_ok=True)
        
        logger.info(f"Starting analysis of workbook: {workbook_path}")
        logger.info("Initializing Tableau Workbook Analyzer...")
        analyzer = TableauWorkbookAnalyzer(workbook_path)
        
        logger.info("Extracting XML content...")
        analyzer.extract_xml()
        
        logger.info("Extracting parameters...")
        analyzer.extract_parameters()
        logger.info(f"Found {len(analyzer.parameters)} parameters")
        
        logger.info("Extracting data sources...")
        analyzer.extract_data_sources()
        logger.info(f"Found {len(analyzer.data_sources)} data sources")
        
        logger.info("Extracting calculated fields...")
        analyzer.extract_calculations()
        logger.info(f"Found {len(analyzer.calculations)} calculated fields")
        
        logger.info("Extracting sheets and their dependencies...")
        analyzer.extract_sheets()
        logger.info(f"Found {len(analyzer.sheets)} worksheets")
        
        logger.info("Extracting dashboards...")
        analyzer.extract_dashboards()
        logger.info(f"Found {len(analyzer.dashboards)} dashboards")
        
        logger.info("Extracting actions...")
        analyzer.extract_actions()
        logger.info(f"Found {len(analyzer.actions)} actions")
        
        logger.info("Extracting hierarchies...")
        analyzer.extract_hierarchies()
        logger.info(f"Found {len(analyzer.hierarchies)} hierarchies")
        
        logger.info("Generating reports...")
        report_generator = ReportGenerator()
        
        # Generate all reports
        report_generator.generate_data_sources_report(
            analyzer.data_sources, os.path.join(output_dir, 'data_sources_report.csv'))
        
        report_generator.generate_calculations_report(
            analyzer.calculations, os.path.join(output_dir, 'calculations_report.csv'))
        
        report_generator.generate_sheets_report(
            analyzer.sheets, os.path.join(output_dir, 'sheets_report.csv'))
        
        report_generator.generate_parameters_report(
            analyzer.parameters, os.path.join(output_dir, 'parameters_report.csv'))
        
        report_generator.generate_dashboards_report(
            analyzer.dashboards, os.path.join(output_dir, 'dashboards_report.csv'))
        
        report_generator.generate_actions_report(
            analyzer.actions, os.path.join(output_dir, 'actions_report.csv'))
        
        report_generator.generate_hierarchies_report(
            analyzer.hierarchies, os.path.join(output_dir, 'hierarchies_report.csv'))
            
        # Generate summary report
        report_generator.generate_summary_report(
            analyzer, os.path.join(output_dir, 'workbook_summary.csv'))
        
        # Print success message
        logger.info("Analysis complete! Generated reports in 'output_files' directory:")
        logger.info("- data_sources_report.csv")
        logger.info("- calculations_report.csv")
        logger.info("- sheets_report.csv")
        logger.info("- parameters_report.csv")
        logger.info("- dashboards_report.csv")
        logger.info("- actions_report.csv")
        logger.info("- hierarchies_report.csv")
        logger.info("- workbook_summary.csv")
        
        print("\nAnalysis complete! Generated reports in 'output_files' directory.")
        print(f"Found {len(analyzer.calculations)} calculations, {len(analyzer.parameters)} parameters, and {len(analyzer.sheets)} sheets.")
        print("See the CSV files for detailed information about your Tableau workbook.")
        
    except Exception as e:
        logger.error(f"Analysis failed: {e}")
        logger.exception("Detailed error information:")
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()