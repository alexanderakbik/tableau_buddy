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

# Version information
__version__ = '1.0.0'

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Changed to INFO for less verbose default output
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
    # To store filter details for architectural recommendations
    filters_used: List[Dict[str, str]] 

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
            
        for param_name in self.parameters.keys():
            # Escape special characters in parameter name for regex
            escaped_param_name = re.escape(param_name)
            
            # Check for bracketed form - exact match with brackets
            bracketed_pattern = fr'\[{escaped_param_name}\]'
            if re.search(bracketed_pattern, text):
                refs.add(param_name)
                continue # Found, no need to check unbracketed
                
            # Check for unbracketed form with word boundaries to avoid partial matches
            # Ensure it's not part of a larger word or function call
            unbracketed_pattern = fr'(?<!\w){escaped_param_name}(?!\w)' 
            if re.search(unbracketed_pattern, text):
                # Avoid false positives by checking common function names or keywords
                # that might match parameter names accidentally (this check might be too broad)
                # A simpler word boundary check is often sufficient.
                # Example: If param is "Date", don't match "DATE("
                if not re.search(fr'\b[A-Z]+\({escaped_param_name}\)', text, re.IGNORECASE):
                     refs.add(param_name)
                    
        return refs

    def extract_xml(self) -> None:
        """Extract XML content from TWBX or TWB file."""
        try:
            if self.workbook_path.suffix.lower() == '.twbx':
                with zipfile.ZipFile(self.workbook_path, 'r') as zip_ref:
                    twb_files = [f for f in zip_ref.namelist() if f.endswith('.twb')]
                    if not twb_files:
                        raise ValueError("No .twb file found in TWBX package")
                    
                    twb_file = twb_files[0]
                    logger.info(f"Using TWB file: {twb_file}")
                    self.xml_content = zip_ref.read(twb_file).decode('utf-8')
            else:
                with open(self.workbook_path, 'r', encoding='utf-8') as f:
                    self.xml_content = f.read()
            
            parser = etree.XMLParser(recover=True, huge_tree=True) # Added huge_tree for large workbooks
            self.root = etree.fromstring(self.xml_content.encode('utf-8'), parser=parser)
            
            # Try to find the namespace in the root element's nsmap
            ns_found = False
            if hasattr(self.root, 'nsmap'):
                for prefix, uri in self.root.nsmap.items():
                    if uri in self.TABLEAU_NAMESPACES:
                        self.ns = {'t': uri}  # Always use 't' as the prefix
                        ns_found = True
                        break
            
            # If not found in nsmap, try to extract from root tag
            if not ns_found:
                root_tag = self.root.tag
                if '{' in root_tag:
                    ns_uri = root_tag[root_tag.find('{')+1:root_tag.find('}')]
                    if ns_uri in self.TABLEAU_NAMESPACES:
                        self.ns = {'t': ns_uri}
                        ns_found = True
            
            # If still not found, try default namespace
            if not ns_found:
                self.ns = {'t': self.TABLEAU_NAMESPACES[0]}
                logger.warning("No Tableau namespace found, using default.")
            
            logger.info(f"Using namespace: {self.ns}")
            
        except Exception as e:
            logger.error(f"Failed to extract XML from workbook: {e}")
            raise

    def _xpath(self, element: etree._Element, path: str) -> List[etree._Element]:
        """Helper method to execute XPath with the determined namespace."""
        try:
            # Try with namespace first
            results = element.xpath(path, namespaces=self.ns)
            
            if not results:
                # Try without namespace prefix
                path_no_ns = path.replace('t:', '')
                results = element.xpath(path_no_ns)
                
                if not results:
                    # Try with namespace but without prefix
                    path_with_ns = path.replace('t:', '')
                    results = element.xpath(path_with_ns, namespaces=self.ns)
            
            return results
        except Exception as e:
            logger.warning(f"XPath query failed for path '{path}' with namespace {self.ns}: {e}")
            return []

    def _find(self, element: etree._Element, path: str) -> Optional[etree._Element]:
        """Helper method to find an element with the correct namespace prefix."""
        results = self._xpath(element, path)
        return results[0] if results else None

    def extract_parameters(self) -> None:
        """Extract parameters and their usage."""
        try:
            parameters_xml = self._xpath(self.root, './/t:workbook/t:parameters/t:parameter')
            if not parameters_xml: # Fallback for older structures or different paths
                 parameters_xml = self._xpath(self.root, './/t:parameter')

            for param in parameters_xml:
                param_name = param.get('name', 'UnknownParameter').strip('[]')
                datatype = param.get('datatype', '')
                current_value_el = self._find(param, './/t:current-value')
                current_value = current_value_el.text if current_value_el is not None else param.get('value', '') # some versions use 'value'
                
                range_elem = self._find(param, './/t:range')
                range_min = range_elem.get('min') if range_elem is not None else None
                range_max = range_elem.get('max') if range_elem is not None else None
                
                allowed_values = []
                members = self._xpath(param, './/t:members/t:member') # Common path for allowed values
                if not members: # Alternative path
                    members = self._xpath(param, './/t:list/t:member')

                for member in members:
                    val = member.get('value')
                    if val is not None:
                         allowed_values.append(val)
                
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
            logger.error(f"Failed to extract parameters: {e}", exc_info=True)
            # Continue if possible, as parameters might not always be present
    
    def extract_data_sources(self) -> None:
        """Extract information about data sources."""
        try:
            datasources_xml = self._xpath(self.root, './/t:datasources/t:datasource')
            logger.info(f"Processing {len(datasources_xml)} datasources")
            
            for ds_xml in datasources_xml:
                ds_name = ds_xml.get('name', ds_xml.get('caption', f'UnknownDataSource_{len(self.data_sources)}')).strip('[]')
                
                if ds_name in self.data_sources: # Avoid reprocessing if names collide (e.g. 'Parameters')
                    logger.warning(f"Datasource '{ds_name}' already processed or name collision. Skipping.")
                    continue
                
                server = ''
                database = ''
                
                # Look for connection info
                connection_xml = self._find(ds_xml, './/t:connection')
                if connection_xml is None: # Try federated connection path
                    connection_xml = self._find(ds_xml, './/t:named-connections/t:named-connection/t:connection')

                if connection_xml is not None:
                    server = connection_xml.get('server', '')
                    database = connection_xml.get('filename', connection_xml.get('dbname', connection_xml.get('database', ''))) # dbname is common too

                tables_or_columns = set() # Use set to avoid duplicates initially
                # Extract columns/fields (these represent the 'tables' or queryable fields from the source)
                # This can be complex due to custom SQL, multiple tables, etc.
                # We will list all unique column names as a proxy for 'tables' or fields.
                
                # Path 1: column elements directly under datasource
                column_elements = self._xpath(ds_xml, './/t:column')
                for col in column_elements:
                    col_name = col.get('name', col.get('caption'))
                    if col_name:
                        tables_or_columns.add(col_name.strip('[]'))

                # Path 2: metadata-records (often more detailed)
                metadata_records = self._xpath(ds_xml, './/t:metadata-record[@class="column"]')
                for rec in metadata_records:
                    # Try to get the original name if available, otherwise the local name
                    original_name_el = self._find(rec, './/t:remote-name') # Original name in the database
                    local_name_el = self._find(rec, './/t:local-name') # Name used in Tableau
                    
                    col_name = None
                    if local_name_el is not None and local_name_el.text:
                        col_name = local_name_el.text
                    elif original_name_el is not None and original_name_el.text:
                        col_name = original_name_el.text
                    
                    if col_name:
                        tables_or_columns.add(col_name.strip('[]'))
                
                self.data_sources[ds_name] = DataSource(
                    name=ds_name,
                    server=server,
                    database=database,
                    tables=sorted(list(tables_or_columns))
                )
        except Exception as e:
            logger.error(f"Failed to extract data sources: {e}", exc_info=True)
            # Continue if possible
    
    def extract_calculations(self) -> None:
        """Extract calculated fields and their formulas."""
        try:
            # Calculations can be defined within datasources
            for ds_xml in self._xpath(self.root, './/t:datasources/t:datasource'):
                datasource_name = ds_xml.get('name', ds_xml.get('caption', 'Unknown')).strip('[]')
                
                # Find all columns that have a calculation child
                for column_xml in self._xpath(ds_xml, './/t:column[.//t:calculation]'):
                    calc_xml = self._find(column_xml, './/t:calculation')
                    if calc_xml is not None:
                        # Name is usually on the parent column, role='dimension' or 'measure'
                        calc_name = column_xml.get('name', column_xml.get('caption', f'UnknownCalc_{len(self.calculations)}')).strip('[]')
                        formula = calc_xml.get('formula', '').strip()
                        
                        # Ensure parameters are extracted first for find_parameter_references to work
                        if not self.parameters and self._xpath(self.root, './/t:workbook/t:parameters/t:parameter'):
                            logger.warning("Parameters not extracted before calculations. Extracting now.")
                            self.extract_parameters()

                        param_refs = self.find_parameter_references(formula)

                        self.calculations[calc_name] = Calculation(
                            name=calc_name,
                            formula=formula,
                            used_in=[],
                            dependencies=set(), 
                            parameters_used=param_refs,
                            datasource=datasource_name
                        )
                        
                        for param_name in param_refs:
                            if param_name in self.parameters:
                                if calc_name not in self.parameters[param_name].used_in_calculations:
                                    self.parameters[param_name].used_in_calculations.append(calc_name)

            # Second pass: Find dependencies between calculations
            for calc_name, calc_obj in self.calculations.items():
                for other_calc_name, other_calc_obj in self.calculations.items():
                    if other_calc_name == calc_name:
                        continue
                    
                    # Check for bracketed reference: "[OtherCalcName]"
                    # Need to escape for regex if other_calc_name has special chars
                    escaped_other_calc_name = re.escape(other_calc_name)
                    if re.search(fr'\[{escaped_other_calc_name}\]', calc_obj.formula):
                        calc_obj.dependencies.add(other_calc_name)
                        # Propagate parameters from dependency
                        calc_obj.parameters_used.update(other_calc_obj.parameters_used)
                        self._propagate_parameters_recursive(calc_obj, other_calc_name, set())


        except Exception as e:
            logger.error(f"Failed to extract calculations: {e}", exc_info=True)

    def _propagate_parameters_recursive(self, current_calc: Calculation, dependency_calc_name: str, visited: Set[str]):
        """Recursively propagate parameters from a dependency to the current calculation."""
        if dependency_calc_name in visited or dependency_calc_name not in self.calculations:
            return
        visited.add(dependency_calc_name)

        dependency_calc = self.calculations[dependency_calc_name]
        current_calc.parameters_used.update(dependency_calc.parameters_used)

        for nested_dependency_name in dependency_calc.dependencies:
            self._propagate_parameters_recursive(current_calc, nested_dependency_name, visited)
    
    def extract_sheets(self) -> None:
        """Extract information about worksheets and their dependencies."""
        try:
            worksheets_xml = self._xpath(self.root, '//t:worksheets/t:worksheet')
            logger.info(f"Processing {len(worksheets_xml)} worksheets")
            
            for ws_xml in worksheets_xml:
                sheet_name = ws_xml.get('name', f'UnknownSheet_{len(self.sheets)}')
                
                datasources_used_in_sheet = set()
                calculations_used_in_sheet = set()
                parameters_used_in_sheet = set()
                filters_in_sheet = [] # For architectural recommendations

                # Datasources explicitly linked to the sheet
                for ds_dep_xml in self._xpath(ws_xml, './/t:datasource-dependencies'):
                    ds_name = ds_dep_xml.get('datasource', '').strip('[]')
                    if ds_name and ds_name in self.data_sources:
                        datasources_used_in_sheet.add(ds_name)
                
                # Analyze fields used in various parts of the sheet (shelves, marks, filters, etc.)
                # This is a common pattern: find 'column' or 'field' references
                field_references_xml = self._xpath(ws_xml, './/t:view/.//t:slices/.//t:column | .//t:view/.//t:rows/.//t:column | .//t:view/.//t:cols/.//t:column | .//t:view/.//t:pages/.//t:column | .//t:view/.//t:lod/.//t:column | .//t:view/.//t:measures/.//t:measure | .//t:encodings/.//*[self::t:color or self::t:size or self::t:angle or self::t:shape or self::t:text or self::t:detail or self::t:tooltip]')
                # Also check for fields in specific encodings more directly if the above is too broad or misses some
                encoding_fields_xml = self._xpath(ws_xml, './/t:encodings//t:encoding[@field]')


                all_field_containers = field_references_xml + encoding_fields_xml

                for field_container_xml in all_field_containers:
                    field_name_attr = field_container_xml.get('column') or field_container_xml.get('field') or field_container_xml.get('name') # 'name' for measure-value
                    if not field_name_attr: # If it's a measure like <color column='[federated.xxxxxxxx].[Measures].[sum:Profit:qk]' />
                        # Try to parse the complex name
                        match = re.search(r'\[([^\]]+)\]\.\[([^\]]+)\]', field_name_attr if field_name_attr else "")
                        if match:
                            field_name_attr = match.group(2) # Get the actual field name part
                        elif field_container_xml.text and '[' in field_container_xml.text: # Sometimes it's in the text
                             field_name_attr = field_container_xml.text.strip('[]')


                    if field_name_attr:
                        field_name = field_name_attr.strip('[]')

                        # Handle names like '[TableauSQL].[Calculation_12345]'
                        if '.' in field_name and not field_name.startswith('Parameters.'):
                            # Attempt to split and take the last part if it looks like a qualified name
                            parts = field_name.split('.')
                            potential_field_name = parts[-1].strip('[]')
                            if potential_field_name in self.calculations or potential_field_name in self.parameters:
                                field_name = potential_field_name
                            # else keep original field_name if it's a direct datasource field

                        if field_name in self.calculations:
                            calculations_used_in_sheet.add(field_name)
                            calc_obj = self.calculations[field_name]
                            if sheet_name not in calc_obj.used_in:
                                calc_obj.used_in.append(sheet_name)
                            if calc_obj.datasource: # Add datasource of the calculation
                                datasources_used_in_sheet.add(calc_obj.datasource)
                            # Add parameters used by this calculation to the sheet's parameter list
                            for param_name in calc_obj.parameters_used:
                                parameters_used_in_sheet.add(param_name)
                                if param_name in self.parameters and sheet_name not in self.parameters[param_name].used_in:
                                    self.parameters[param_name].used_in.append(sheet_name)
                            # Add dependencies of this calculation as well
                            self._add_dependent_calculations_to_sheet(calc_obj, sheet_name, calculations_used_in_sheet, datasources_used_in_sheet, parameters_used_in_sheet, set())


                        elif field_name in self.parameters:
                            parameters_used_in_sheet.add(field_name)
                            if sheet_name not in self.parameters[field_name].used_in:
                                self.parameters[field_name].used_in.append(sheet_name)
                        
                        # If it's a direct field from a datasource, try to link its datasource
                        elif not field_name.startswith('Measure Names') and not field_name.startswith('Measure Values'):
                            for ds_name, ds_obj in self.data_sources.items():
                                if field_name in ds_obj.tables:
                                    datasources_used_in_sheet.add(ds_name)
                                    break
                
                # Extract filters for architectural recommendations
                filter_elements = self._xpath(ws_xml, './/t:filter')
                for filter_el in filter_elements:
                    filter_column = filter_el.get('column', '').strip('[]')
                    filter_class = filter_el.get('class', '') # e.g., 'categorical', 'quantitative'
                    if filter_column:
                        filters_in_sheet.append({'column': filter_column, 'class': filter_class})
                        # If filter column is a calculation or parameter, ensure it's tracked
                        if filter_column in self.calculations:
                            calculations_used_in_sheet.add(filter_column)
                            # ... (add to calc.used_in, add its params to sheet, etc. as above)
                        elif filter_column in self.parameters:
                            parameters_used_in_sheet.add(filter_column)
                            # ... (add to param.used_in as above)


                self.sheets[sheet_name] = Sheet(
                    name=sheet_name,
                    datasources=sorted(list(datasources_used_in_sheet)),
                    calculations_used=sorted(list(calculations_used_in_sheet)),
                    parameters_used=sorted(list(parameters_used_in_sheet)),
                    filters_used=filters_in_sheet
                )
        except Exception as e:
            logger.error(f"Failed to extract sheets: {e}", exc_info=True)

    def _add_dependent_calculations_to_sheet(self, calc: Calculation, sheet_name: str, 
                                           sheet_calcs: Set[str], sheet_ds: Set[str], 
                                           sheet_params: Set[str], visited: Set[str]):
        """Recursively add dependent calculations, their datasources, and parameters to the sheet's tracking sets."""
        if calc.name in visited:
            return
        visited.add(calc.name)

        for dep_name in calc.dependencies:
            if dep_name in self.calculations and dep_name not in sheet_calcs:
                sheet_calcs.add(dep_name)
                dep_calc = self.calculations[dep_name]
                if sheet_name not in dep_calc.used_in:
                    dep_calc.used_in.append(sheet_name)
                if dep_calc.datasource:
                    sheet_ds.add(dep_calc.datasource)
                for param_name in dep_calc.parameters_used:
                    sheet_params.add(param_name)
                    if param_name in self.parameters and sheet_name not in self.parameters[param_name].used_in:
                        self.parameters[param_name].used_in.append(sheet_name)
                # Recurse for nested dependencies
                self._add_dependent_calculations_to_sheet(dep_calc, sheet_name, sheet_calcs, sheet_ds, sheet_params, visited)


    def extract_dashboards(self) -> None:
        """Extract dashboard information."""
        try:
            dashboards_xml = self._xpath(self.root, '//t:dashboards/t:dashboard | //t:windows/t:window[@class="dashboard"]')
            logger.info(f"Processing {len(dashboards_xml)} dashboards")
            
            for db_xml in dashboards_xml:
                dashboard_name = db_xml.get('name', f'UnknownDashboard_{len(self.dashboards)}')
                sheets_in_dashboard = []
                
                # Sheets can be in zones or as dashboard-objects
                zone_sheets = self._xpath(db_xml, './/t:zones//t:zone[@name]') # name here is sheet name
                object_sheets = self._xpath(db_xml, './/t:dashboard-objects//t:dashboard-object[@type="worksheet"]//t:worksheet[@name]')


                for sheet_zone_xml in zone_sheets:
                    # Check if 'name' attribute is a valid sheet name
                    potential_sheet_name = sheet_zone_xml.get('name')
                    if potential_sheet_name and potential_sheet_name in self.sheets:
                         sheets_in_dashboard.append(potential_sheet_name)
                    # Sometimes the sheet name is in a child <worksheet name="Sheet 1"/>
                    elif self._find(sheet_zone_xml, './/t:worksheet[@name]'):
                        ws_el = self._find(sheet_zone_xml, './/t:worksheet[@name]')
                        if ws_el is not None:
                            sheets_in_dashboard.append(ws_el.get('name'))


                for sheet_object_xml in object_sheets:
                    sheet_name = sheet_object_xml.get('name')
                    if sheet_name and sheet_name in self.sheets: # Ensure it's a known sheet
                        sheets_in_dashboard.append(sheet_name)
                
                self.dashboards[dashboard_name] = Dashboard(
                    name=dashboard_name,
                    sheets=sorted(list(set(sheets_in_dashboard))) # Deduplicate and sort
                )
        except Exception as e:
            logger.error(f"Failed to extract dashboards: {e}", exc_info=True)

    def extract_actions(self) -> None:
        """Extract action information."""
        try:
            # Actions can be defined at workbook level or sometimes within dashboards/worksheets
            actions_xml = self._xpath(self.root, './/t:actions/t:action | .//t:workbook/t:actions/t:action')
            for action_xml in actions_xml:
                action_name = action_xml.get('name', f'UnknownAction_{len(self.actions)}')
                action_type = action_xml.get('caption', action_xml.get('type', '')) # caption often more descriptive
                
                # Source/Target information varies by action type (filter, highlight, URL)
                source_sheet_xml = self._find(action_xml, './/t:source') # General source
                target_sheet_xml = self._find(action_xml, './/t:target') # General target

                source_sheet_name = source_sheet_xml.get('worksheet') if source_sheet_xml is not None else ''
                target_sheet_name = target_sheet_xml.get('worksheet') if target_sheet_xml is not None else ''
                
                # For filter actions, fields are often in <action-target-filter>
                filter_info_xml = self._find(action_xml, './/t:action-target-filter')
                source_field = filter_info_xml.get('source-field') if filter_info_xml is not None else ''
                target_field = filter_info_xml.get('target-field') if filter_info_xml is not None else ''

                # For URL actions
                url_text = action_xml.get('command-url') # Often an attribute of <action> itself for URL actions
                if not url_text: # Or sometimes in a child
                    url_xml = self._find(action_xml, './/t:url')
                    if url_xml is not None:
                        url_text = url_xml.text

                # For navigation (Go to Sheet) actions
                target_dashboard_name = target_sheet_xml.get('dashboard') if target_sheet_xml is not None else ''
                if not target_sheet_name and not target_dashboard_name: # Sometimes target is directly an attribute
                    target_sheet_name = action_xml.get('target-worksheet', '')
                    target_dashboard_name = action_xml.get('target-dashboard', '')


                # Refine action type based on elements present
                if 'filter' in action_type.lower() or (source_field and target_field):
                    actual_type = 'Filter'
                elif 'url' in action_type.lower() or url_text:
                    actual_type = 'URL'
                elif 'highlight' in action_type.lower():
                    actual_type = 'Highlight'
                elif target_sheet_name or target_dashboard_name:
                    actual_type = 'Go to Sheet/Dashboard'
                else:
                    actual_type = action_type if action_type else 'Unknown'


                self.actions[action_name] = Action(
                    name=action_name,
                    type=actual_type,
                    source_sheet=source_sheet_name,
                    target_sheet=target_sheet_name,
                    source_field=source_field.strip('[]') if source_field else '',
                    target_field=target_field.strip('[]') if target_field else '',
                    url=url_text,
                    target_dashboard=target_dashboard_name
                )
        except Exception as e:
            logger.error(f"Failed to extract actions: {e}", exc_info=True)

    def extract_hierarchies(self) -> None:
        """Extract hierarchy information."""
        try:
            # Hierarchies are typically defined within a datasource
            for ds_xml in self._xpath(self.root, './/t:datasources/t:datasource'):
                ds_name = ds_xml.get('name', ds_xml.get('caption', 'Unknown')).strip('[]')
                for hierarchy_xml in self._xpath(ds_xml, './/t:hierarchy'):
                    hierarchy_name = hierarchy_xml.get('name', f'UnknownHierarchy_{len(self.hierarchies)}')
                    levels = []
                    
                    for level_xml in self._xpath(hierarchy_xml, './/t:level'):
                        level_name = level_xml.get('name', '').strip('[]')
                        if level_name:
                            levels.append(level_name)
                    
                    if levels: # Only add if hierarchy has levels
                        self.hierarchies[f"{ds_name}.{hierarchy_name}"] = Hierarchy( # Prefix with DS name for uniqueness
                            name=hierarchy_name,
                            levels=levels,
                            datasource=ds_name
                        )
        except Exception as e:
            logger.error(f"Failed to extract hierarchies: {e}", exc_info=True)

class ReportGenerator:
    @staticmethod
    def generate_data_sources_report(data_sources: Dict[str, DataSource], output_path: str) -> None:
        """Generate CSV report for data sources."""
        ds_data = [
            {
                'Data Source Name': ds.name,
                'Server': ds.server,
                'Database': ds.database,
                'Fields/Columns': ', '.join(ds.tables) if ds.tables else 'N/A' # Renamed for clarity
            }
            for ds in data_sources.values()
        ]
        pd.DataFrame(ds_data).to_csv(output_path, index=False)
        logger.info(f"Generated data sources report: {output_path}")
    
    @staticmethod
    def generate_calculations_report(calculations: Dict[str, Calculation], output_path: str) -> None:
        """Generate CSV report for calculations."""
        calc_data = []
        for calc_name, calc in calculations.items():
            used_in = sorted(list(set(calc.used_in)))
            dependencies = sorted(list(set(calc.dependencies)))
            params_used = sorted(list(set(calc.parameters_used)))
            
            cross_source = 'No' # This is hard to determine accurately without deeper analysis
                                # For now, assume 'No' unless specific patterns are found.
                                # A simple check could be if dependencies reference fields with a '.'
            if any('.' in dep for dep in dependencies if dep not in calculations): # e.g. [DatasourceName].[FieldName]
                 cross_source = 'Potentially (references external field)'

            calc_data.append({
                'Calculation Name': calc.name,
                'Data Source': calc.datasource,
                'Formula': calc.formula,
                'Used In Sheets': ', '.join(used_in) if used_in else 'None',
                'Dependencies (Other Calcs)': ', '.join(dependencies) if dependencies else 'None',
                'Parameters Used': ', '.join(params_used) if params_used else 'None',
                'Is Cross-Source Referenced': cross_source, # Renamed for clarity
                'Dependency Count': len(dependencies),
                'Sheet Usage Count': len(used_in)
            })
        
        calc_df = pd.DataFrame(calc_data)
        if not calc_df.empty:
            calc_df = calc_df.sort_values(by=['Dependency Count', 'Sheet Usage Count', 'Calculation Name'], ascending=[False, False, True])
        
        # Select and reorder columns for the final report
        report_columns = ['Calculation Name', 'Data Source', 'Formula', 'Used In Sheets', 
                          'Dependencies (Other Calcs)', 'Parameters Used', 'Is Cross-Source Referenced']
        final_df = calc_df[report_columns] if not calc_df.empty else pd.DataFrame(columns=report_columns)
        final_df.to_csv(output_path, index=False)
        logger.info(f"Generated calculations report: {output_path}")
    
    @staticmethod
    def generate_sheets_report(sheets: Dict[str, Sheet], output_path: str) -> None:
        """Generate CSV report for sheets."""
        sheet_data = []
        for sheet_name, sheet in sheets.items():
            datasources = sorted(list(set(sheet.datasources)))
            calcs_used = sorted(list(set(sheet.calculations_used)))
            params_used = sorted(list(set(sheet.parameters_used)))
            
            sheet_data.append({
                'Sheet Name': sheet.name,
                'Data Sources Used': ', '.join(datasources) if datasources else 'None',
                'Calculations Used': ', '.join(calcs_used) if calcs_used else 'None',
                'Parameters Used': ', '.join(params_used) if params_used else 'None',
                'Calculation Count': len(calcs_used),
                'Parameter Count': len(params_used),
                'Filter Count': len(sheet.filters_used)
            })
        
        sheet_df = pd.DataFrame(sheet_data)
        if not sheet_df.empty:
             sheet_df = sheet_df.sort_values(by=['Calculation Count', 'Parameter Count', 'Sheet Name'], ascending=[False, False, True])
        
        report_columns = ['Sheet Name', 'Data Sources Used', 'Calculations Used', 'Parameters Used', 'Filter Count']
        final_df = sheet_df[report_columns] if not sheet_df.empty else pd.DataFrame(columns=report_columns)
        final_df.to_csv(output_path, index=False)
        logger.info(f"Generated sheets report: {output_path}")

    @staticmethod
    def generate_parameters_report(parameters: Dict[str, Parameter], output_path: str) -> None:
        """Generate CSV report for parameters."""
        param_data = []
        for param_name, param in parameters.items():
            used_in_sheets = sorted(list(set(param.used_in)))
            used_in_calcs = sorted(list(set(param.used_in_calculations)))
            
            range_values = 'N/A'
            if param.range_min or param.range_max:
                range_values = f"Min: {param.range_min or 'N/A'}, Max: {param.range_max or 'N/A'}"
            elif param.allowed_values:
                range_values = f"List: {', '.join(param.allowed_values)}"
                
            param_data.append({
                'Parameter Name': param.name,
                'Data Type': param.datatype,
                'Current Value': param.current_value,
                'Range/Allowed Values': range_values,
                'Used In Sheets': ', '.join(used_in_sheets) if used_in_sheets else 'None',
                'Used In Calculations': ', '.join(used_in_calcs) if used_in_calcs else 'None',
                'Total Usage Count': len(used_in_sheets) + len(used_in_calcs)
            })
        
        param_df = pd.DataFrame(param_data)
        if not param_df.empty:
            param_df = param_df.sort_values(by=['Total Usage Count', 'Parameter Name'], ascending=[False, True])

        report_columns = ['Parameter Name', 'Data Type', 'Current Value', 'Range/Allowed Values', 
                          'Used In Sheets', 'Used In Calculations']
        final_df = param_df[report_columns] if not param_df.empty else pd.DataFrame(columns=report_columns)
        final_df.to_csv(output_path, index=False)
        logger.info(f"Generated parameters report: {output_path}")

    @staticmethod
    def generate_dashboards_report(dashboards: Dict[str, Dashboard], output_path: str) -> None:
        """Generate CSV report for dashboards."""
        dashboard_data = [
            {
                'Dashboard Name': db.name,
                'Sheets Contained': ', '.join(sorted(list(set(db.sheets)))) if db.sheets else 'None',
                'Sheet Count': len(set(db.sheets))
            }
            for db in dashboards.values()
        ]
        
        db_df = pd.DataFrame(dashboard_data)
        if not db_df.empty:
            db_df = db_df.sort_values(by=['Sheet Count', 'Dashboard Name'], ascending=[False, True])

        report_columns = ['Dashboard Name', 'Sheets Contained', 'Sheet Count']
        final_df = db_df[report_columns] if not db_df.empty else pd.DataFrame(columns=report_columns)
        final_df.to_csv(output_path, index=False)
        logger.info(f"Generated dashboards report: {output_path}")

    @staticmethod
    def generate_actions_report(actions: Dict[str, Action], output_path: str) -> None:
        """Generate CSV report for actions."""
        action_data = []
        for action_name, action in actions.items():
            target = action.target_sheet or action.target_dashboard or action.url or 'N/A'
            action_data.append({
                'Action Name': action.name,
                'Type': action.type,
                'Source Sheet': action.source_sheet or 'N/A',
                'Target': target,
                'Source Field': action.source_field or 'N/A',
                'Target Field': action.target_field or 'N/A'
            })
        
        action_df = pd.DataFrame(action_data)
        if not action_df.empty:
            action_df = action_df.sort_values(by=['Type', 'Action Name'])
        action_df.to_csv(output_path, index=False)
        logger.info(f"Generated actions report: {output_path}")

    @staticmethod
    def generate_hierarchies_report(hierarchies: Dict[str, Hierarchy], output_path: str) -> None:
        """Generate CSV report for hierarchies."""
        hierarchy_data = [
            {
                'Hierarchy Name': h.name,
                'Data Source': h.datasource,
                'Levels': ' → '.join(h.levels),
                'Level Count': len(h.levels)
            }
            for h in hierarchies.values()
        ]
        
        h_df = pd.DataFrame(hierarchy_data)
        if not h_df.empty:
            h_df = h_df.sort_values(by=['Level Count', 'Hierarchy Name'], ascending=[False, True])
        
        report_columns = ['Hierarchy Name', 'Data Source', 'Levels'] # Level count is for sorting
        final_df = h_df[report_columns] if not h_df.empty else pd.DataFrame(columns=report_columns)
        final_df.to_csv(output_path, index=False)
        logger.info(f"Generated hierarchies report: {output_path}")

    @staticmethod
    def generate_summary_report(analyzer: 'TableauWorkbookAnalyzer', output_path: str) -> None:
        """Generate a summary report of the workbook analysis."""
        num_sheets = len(analyzer.sheets)
        avg_calcs_per_sheet = (sum(len(s.calculations_used) for s in analyzer.sheets.values()) / num_sheets) if num_sheets else 0
        avg_params_per_sheet = (sum(len(s.parameters_used) for s in analyzer.sheets.values()) / num_sheets) if num_sheets else 0

        summary_data = {
            'Metric': [
                'Total Data Sources', 'Total Calculations', 'Total Parameters',
                'Total Worksheets', 'Total Dashboards', 'Total Actions', 'Total Hierarchies',
                'Average Calculations per Sheet', 'Average Parameters per Sheet',
                'Sheets with Calculations', 'Sheets with Parameters'
            ],
            'Count': [
                len(analyzer.data_sources), len(analyzer.calculations), len(analyzer.parameters),
                num_sheets, len(analyzer.dashboards), len(analyzer.actions), len(analyzer.hierarchies),
                f"{avg_calcs_per_sheet:.2f}", f"{avg_params_per_sheet:.2f}",
                sum(1 for s in analyzer.sheets.values() if s.calculations_used),
                sum(1 for s in analyzer.sheets.values() if s.parameters_used)
            ]
        }
        pd.DataFrame(summary_data).to_csv(output_path, index=False)
        logger.info(f"Generated summary report: {output_path}")

    @staticmethod
    def generate_backend_offload_recommendations(analyzer: 'TableauWorkbookAnalyzer', output_path: str) -> None:
        """Generate CSV report for backend offload recommendations."""
        recommendations_data = []
        processed_calcs = set() # To avoid duplicate entries for the same calculation

        for calc_name, calc in analyzer.calculations.items():
            if calc_name in processed_calcs:
                continue

            reasons = []
            notes_list = []

            formula_len = len(calc.formula)
            num_ifs = calc.formula.upper().count("IF ")
            num_case = calc.formula.upper().count("CASE ")
            # More robust check for nested IFs could involve parsing or more complex regex,
            # but for now, count of IFs is a proxy.
            complexity_score = (formula_len / 100) + (num_ifs * 2) + (num_case * 2) # Simple heuristic

            # Reason 1: Heavy/complex logic
            if num_ifs >= 3 or num_case >= 2 or formula_len > 500 or complexity_score > 7:
                reasons.append("Heavy or complex logic (e.g., multiple IFs/CASEs, long formula)")
                notes_list.append(f"Formula length: {formula_len}, IFs: {num_ifs}, CASEs: {num_case}.")

            # Reason 2: Used across many sheets
            if len(calc.used_in) >= 5:
                reasons.append("Used across 5+ sheets")
                notes_list.append(f"Used in {len(calc.used_in)} sheets.")
            elif len(calc.used_in) >= 2 : # Still notable if used in 2-4 sheets
                 notes_list.append(f"Used in {len(calc.used_in)} sheets.")


            # Reason 3: Constant logic (not user-driven by parameters)
            if not calc.parameters_used:
                reasons.append("Constant logic (not user-driven by parameters)")
                notes_list.append("Appears to be a static derivation.")
            
            # Reason 4: Specific keywords (example: Slot Mapping, Origin Level)
            if "SLOT" in calc_name.upper() or "MAPPING" in calc_name.upper():
                reasons.append("Potential repeated calculation of slot/mapping conditions (name-based heuristic)")
                if "SLOT" in calc_name.upper(): notes_list.append("Name contains 'SLOT'.")
                if "MAPPING" in calc_name.upper(): notes_list.append("Name contains 'MAPPING'.")

            if "ORIGIN" in calc_name.upper() and "LEVEL" in calc_name.upper():
                 reasons.append("Potential static derivation of hierarchical level (name-based heuristic)")
                 notes_list.append("Name suggests hierarchical level derivation.")


            if reasons:
                recommendations_data.append({
                    'Calculation Name': calc.name,
                    'Reason for Offloading': '; '.join(reasons),
                    'Suggested Backend Field': f"bknd_{calc.name.lower().replace(' ', '_').replace('[','').replace(']','')}",
                    'Notes': '; '.join(notes_list) if notes_list else "Review for backend suitability."
                })
                processed_calcs.add(calc_name)
        
        pd.DataFrame(recommendations_data).to_csv(output_path, index=False)
        logger.info(f"Generated backend offload recommendations: {output_path}")

    @staticmethod
    def generate_architecture_recommendations(analyzer: 'TableauWorkbookAnalyzer', output_path: str) -> None:
        """Generate Markdown report for architectural recommendations."""
        recommendations_md = ["# Architectural Recommendations for Tableau Workbook\n"]

        # 1. Merge similar calculations
        recommendations_md.append("## 1. Calculation Consolidation")
        # Simple heuristic: look for names ending with numbers or common suffixes
        similar_calc_groups = {}
        for calc_name in analyzer.calculations.keys():
            base_name = re.sub(r'[_ ]*([vV]\d+|\d+|[Oo]ld|[Nn]ew|[Cc]opy)$', '', calc_name).strip()
            if base_name != calc_name : # It had a suffix that we stripped
                if base_name not in similar_calc_groups:
                    similar_calc_groups[base_name] = []
                similar_calc_groups[base_name].append(calc_name)
        
        significant_groups = {k: v for k, v in similar_calc_groups.items() if len(v) > 1}
        if significant_groups:
            recommendations_md.append("- **Merge similar calculations:** Consider consolidating calculations that appear to be variations of a common logic.")
            for base, variants in significant_groups.items():
                if len(variants) > 2 : # Only list if more than 2, to keep it concise
                    recommendations_md.append(f"  - Example group (base '{base}'): {', '.join(variants[:3])}{'...' if len(variants) > 3 else ''}")
            recommendations_md.append("  Review `calculations_report.csv` for calculations with similar names or formulas (e.g., 'Dest Group 1', 'Dest Group 2').\n")
        else:
            recommendations_md.append("- Review `calculations_report.csv` for calculations with similar naming patterns or functionalities to identify opportunities for reducing redundancy.\n")

        # 2. Normalize hierarchies
        recommendations_md.append("## 2. Hierarchy Management")
        if analyzer.hierarchies:
            recommendations_md.append(f"- **Normalize hierarchies in backend:** The workbook defines {len(analyzer.hierarchies)} explicit hierarchies (see `hierarchies_report.csv`).")
            recommendations_md.append("  Consider defining and managing these hierarchies within the backend data source(s) for better consistency, performance, and reusability across workbooks.\n")
        else:
            recommendations_md.append("- No explicit hierarchies were found. If implicit hierarchies (e.g., related fields used together consistently) exist, consider formalizing them in the backend data model.\n")

        # 3. Consolidate filters
        recommendations_md.append("## 3. Filter Optimization")
        # Check for repeated filter fields across sheets
        filter_field_counts = {}
        for sheet in analyzer.sheets.values():
            unique_filters_on_sheet = set()
            for f_info in sheet.filters_used:
                unique_filters_on_sheet.add(f_info['column'])
            for f_col in unique_filters_on_sheet:
                filter_field_counts[f_col] = filter_field_counts.get(f_col, 0) + 1
        
        repeated_filters = [f"{k} (used on {v} sheets)" for k, v in filter_field_counts.items() if v >= 3] # Threshold: used on 3+ sheets
        
        if repeated_filters:
            recommendations_md.append("- **Consolidate filters that repeat across sheets:** Certain fields are used as filters on multiple sheets.")
            recommendations_md.append(f"  - Examples: {', '.join(repeated_filters[:5])}{'...' if len(repeated_filters)>5 else ''}")
            recommendations_md.append("  Consider creating shared filter logic (e.g., via parameters that drive calculated fields used as filters, or by standardizing filter application if appropriate). Also, review `actions_report.csv` for repetitive filter actions.\n")
        else:
            recommendations_md.append("- Review filter usage across sheets (see `sheets_report.csv` for filter counts) and `actions_report.csv` for filter actions. If common filtering patterns emerge, explore consolidation.\n")

        # 4. Push parameter logic
        recommendations_md.append("## 4. Parameter Usage")
        param_candidates_for_backend = []
        for param in analyzer.parameters.values():
            is_static_list = param.allowed_values and not param.range_min and not param.range_max
            usage_count = len(param.used_in) + len(param.used_in_calculations)
            if is_static_list and len(param.allowed_values) < 5 and usage_count >= 3: # Small list, widely used
                param_candidates_for_backend.append(f"'{param.name}' (list with {len(param.allowed_values)} values, used {usage_count} times)")
        
        if param_candidates_for_backend:
            recommendations_md.append("- **Push parameter logic to UI or backend:** Some parameters with small, fixed lists of allowed values might behave like hard-coded configurations.")
            recommendations_md.append(f"  - Candidates: {', '.join(param_candidates_for_backend[:3])}{'...' if len(param_candidates_for_backend)>3 else ''}")
            recommendations_md.append("  Evaluate if this logic could be better managed as attributes in the backend data or as more dynamic UI controls if they represent true user choices.\n")
        else:
            recommendations_md.append("- Review parameters (see `parameters_report.csv`). If any have static, limited choices and drive core logic, consider if they represent data attributes that could be moved to the backend.\n")

        # 5. Replace data blending with joins
        recommendations_md.append("## 5. Data Source Integration")
        blending_candidate_sheets = []
        for sheet in analyzer.sheets.values():
            if len(sheet.datasources) > 1:
                blending_candidate_sheets.append(f"'{sheet.name}' (uses: {', '.join(sheet.datasources)})")
        
        if blending_candidate_sheets:
            recommendations_md.append("- **Replace data blending with joins where possible:** Sheets using multiple data sources may indicate data blending.")
            recommendations_md.append(f"  - Sheets to review: {'; '.join(blending_candidate_sheets[:3])}{'...' if len(blending_candidate_sheets)>3 else ''}")
            recommendations_md.append("  Consult `data_sources_report.csv` to understand the fields within these sources. If common keys exist, using joins at the database level or in Tableau's data source pane is generally more performant and manageable than blending within worksheets.\n")
        else:
            recommendations_md.append("- No sheets were identified as explicitly using multiple primary data sources in their definition. However, if data blending is occurring implicitly (e.g., linking fields from different sources on a sheet), evaluate if these can be replaced by proper joins in the data preparation phase.\n")

        with open(output_path, 'w', encoding='utf-8') as f:
            f.write("\n".join(recommendations_md))
        logger.info(f"Generated architectural recommendations: {output_path}")

class DBTProjectGenerator:
    """Generates a DBT project structure based on Tableau workbook analysis."""
    
    def __init__(self, analyzer: TableauWorkbookAnalyzer, output_dir: str):
        self.analyzer = analyzer
        self.dbt_dir = os.path.join(output_dir, 'suggested_dbt_project')
        self.model_dirs = ['staging', 'intermediate', 'marts']
        
    def generate_project(self):
        """Generate the complete DBT project structure."""
        try:
            # Create directory structure
            os.makedirs(self.dbt_dir, exist_ok=True)
            for dir_name in ['models', 'macros', 'tests', 'seeds', 'analyses']:
                os.makedirs(os.path.join(self.dbt_dir, dir_name), exist_ok=True)
            
            for model_dir in self.model_dirs:
                os.makedirs(os.path.join(self.dbt_dir, 'models', model_dir), exist_ok=True)
            
            # Generate project files
            self._generate_dbt_project_yml()
            self._generate_sources_yml()
            self._generate_staging_models()
            self._generate_intermediate_models()
            self._generate_mart_models()
            
            logger.info(f"Generated DBT project in {self.dbt_dir}")
        except Exception as e:
            logger.error(f"Failed to generate DBT project: {e}", exc_info=True)
    
    def _generate_dbt_project_yml(self):
        """Generate dbt_project.yml file."""
        content = f"""name: 'tableau_derived_models'
version: '1.0.0'
config-version: 2

profile: 'tableau_derived_models'

model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]

target-path: "target"
clean-targets:
  - "target"
  - "dbt_packages"

models:
  tableau_derived_models:
    staging:
      +materialized: view
      +schema: staging
    intermediate:
      +materialized: ephemeral
      +schema: intermediate
    marts:
      +materialized: table
      +schema: marts"""
        
        with open(os.path.join(self.dbt_dir, 'dbt_project.yml'), 'w') as f:
            f.write(content)
    
    def _generate_sources_yml(self):
        """Generate sources.yml file based on Tableau datasources."""
        sources = []
        for ds_name, ds in self.analyzer.data_sources.items():
            if ds.server and ds.database:  # Only include actual database sources
                source = {
                    'name': ds_name.lower().replace(' ', '_'),
                    'database': ds.database,
                    'schema': '{{ target.schema }}',  # This can be customized based on needs
                    'tables': []
                }
                # Add tables/fields as individual tables if they exist
                if ds.tables:
                    for table in ds.tables:
                        source['tables'].append({
                            'name': table.lower().replace(' ', '_'),
                            'description': f'Table from {ds_name} datasource'
                        })
                sources.append(source)
        
        content = """version: 2

sources:"""
        for source in sources:
            content += f"""
  - name: {source['name']}
    database: {source['database']}
    schema: {source['schema']}
    tables:"""
            for table in source['tables']:
                content += f"""
      - name: {table['name']}
        description: "{table['description']}\""""
        
        with open(os.path.join(self.dbt_dir, 'models', 'staging', 'sources.yml'), 'w') as f:
            f.write(content)
    
    def _generate_staging_models(self):
        """Generate staging models for each datasource."""
        for ds_name, ds in self.analyzer.data_sources.items():
            if not ds.tables:  # Skip if no tables/fields defined
                continue
            
            model_name = f"stg_{ds_name.lower().replace(' ', '_')}"
            content = f"""with source as (
    select * from {{{{ source('{ds_name.lower().replace(' ', '_')}', 'raw_data') }}}}
),

renamed as (
    select
        -- Add column selection based on the fields in the datasource
        {', '.join(field.lower().replace(' ', '_') for field in ds.tables)}
    from source
)

select * from renamed"""
            
            with open(os.path.join(self.dbt_dir, 'models', 'staging', f'{model_name}.sql'), 'w') as f:
                f.write(content)
    
    def _generate_intermediate_models(self):
        """Generate intermediate models based on Tableau calculations."""
        # Group calculations by datasource
        calcs_by_ds = {}
        for calc_name, calc in self.analyzer.calculations.items():
            if calc.datasource not in calcs_by_ds:
                calcs_by_ds[calc.datasource] = []
            calcs_by_ds[calc.datasource].append(calc)
        
        for ds_name, calcs in calcs_by_ds.items():
            model_name = f"int_{ds_name.lower().replace(' ', '_')}_metrics"
            content = f"""with source_data as (
    select * from {{{{ ref('stg_{ds_name.lower().replace(' ', '_')}') }}}}
),

calculated_metrics as (
    select
        *,
        -- Add calculated fields from Tableau
        {self._format_calculations(calcs)}
    from source_data
)

select * from calculated_metrics"""
            
            with open(os.path.join(self.dbt_dir, 'models', 'intermediate', f'{model_name}.sql'), 'w') as f:
                f.write(content)
    
    def _generate_mart_models(self):
        """Generate mart models based on Tableau sheets/dashboards."""
        # Create marts based on dashboard groupings
        for dash_name, dash in self.analyzer.dashboards.items():
            if not dash.sheets:  # Skip if no sheets
                continue
            
            model_name = f"mart_{dash_name.lower().replace(' ', '_')}"
            
            # Collect all datasources and calculations used in this dashboard's sheets
            used_datasources = set()
            used_calculations = set()
            for sheet_name in dash.sheets:
                if sheet_name in self.analyzer.sheets:
                    sheet = self.analyzer.sheets[sheet_name]
                    used_datasources.update(sheet.datasources)
                    used_calculations.update(sheet.calculations_used)
            
            # Generate CTE for each datasource
            ctes = []
            for ds_name in used_datasources:
                cte_name = f"metrics_{ds_name.lower().replace(' ', '_')}"
                ctes.append(f"""
{cte_name} as (
    select * from {{{{ ref('int_{ds_name.lower().replace(' ', '_')}_metrics') }}}}
)""")
            
            # Generate final select
            content = f"""with {','.join(ctes)}

select
    -- Join logic would need to be customized based on actual relationships
    {', '.join(f"m1.{calc.lower().replace(' ', '_')}" for calc in used_calculations)}
from {ctes[0].split()[0]} m1
-- Add appropriate joins here
"""
            
            with open(os.path.join(self.dbt_dir, 'models', 'marts', f'{model_name}.sql'), 'w') as f:
                f.write(content)
    
    def _format_calculations(self, calculations: List[Calculation]) -> str:
        """Format Tableau calculations into SQL expressions."""
        formatted_calcs = []
        for calc in calculations:
            # Convert Tableau formula to SQL
            # This is a simplified conversion and might need manual adjustment
            sql_formula = calc.formula.replace('[', '').replace(']', '')
            formatted_calcs.append(f"{sql_formula} as {calc.name.lower().replace(' ', '_')}")
        return ',\n        '.join(formatted_calcs)

def main():
    """Main function to run the analysis."""
    try:
        if len(sys.argv) < 2:
            print("Usage: python analyze_tableau_workbook.py <path_to_workbook>")
            print("       python analyze_tableau_workbook.py --version")
            sys.exit(1)
            
        if sys.argv[1] == '--version':
            print(f"Tableau Workbook Analyzer version {__version__}")
            sys.exit(0)
            
        workbook_path = Path(sys.argv[1])
        if not workbook_path.exists():
            print(f"Error: Workbook file not found: {workbook_path}")
            sys.exit(1)
            
        # Create output directory
        output_dir = "output_files"
        os.makedirs(output_dir, exist_ok=True)
        
        logger.info(f"Starting analysis of workbook: {workbook_path}")
        analyzer = TableauWorkbookAnalyzer(workbook_path)
        
        logger.info("Step 1: Extracting XML content...")
        analyzer.extract_xml()
        if analyzer.root is None:
            logger.error("XML root not parsed. Aborting analysis.")
            sys.exit(1)

        logger.info("Step 2: Extracting parameters...")
        analyzer.extract_parameters()
        logger.info(f"Found {len(analyzer.parameters)} parameters.")
        
        logger.info("Step 3: Extracting data sources...")
        analyzer.extract_data_sources()
        logger.info(f"Found {len(analyzer.data_sources)} data sources.")
        
        logger.info("Step 4: Extracting calculated fields...")
        analyzer.extract_calculations() # Depends on parameters being extracted
        logger.info(f"Found {len(analyzer.calculations)} calculated fields.")
        
        logger.info("Step 5: Extracting sheets and their dependencies...")
        analyzer.extract_sheets() # Depends on calculations and parameters
        logger.info(f"Found {len(analyzer.sheets)} worksheets.")
        
        logger.info("Step 6: Extracting dashboards...")
        analyzer.extract_dashboards() # Depends on sheets
        logger.info(f"Found {len(analyzer.dashboards)} dashboards.")
        
        logger.info("Step 7: Extracting actions...")
        analyzer.extract_actions()
        logger.info(f"Found {len(analyzer.actions)} actions.")
        
        logger.info("Step 8: Extracting hierarchies...")
        analyzer.extract_hierarchies() # Depends on data sources
        logger.info(f"Found {len(analyzer.hierarchies)} hierarchies.")
        
        logger.info("Step 9: Generating reports...")
        
        ReportGenerator.generate_data_sources_report(
            analyzer.data_sources, os.path.join(output_dir, 'data_sources_report.csv'))
        ReportGenerator.generate_calculations_report(
            analyzer.calculations, os.path.join(output_dir, 'calculations_report.csv'))
        ReportGenerator.generate_sheets_report(
            analyzer.sheets, os.path.join(output_dir, 'sheets_report.csv'))
        ReportGenerator.generate_parameters_report(
            analyzer.parameters, os.path.join(output_dir, 'parameters_report.csv'))
        ReportGenerator.generate_dashboards_report(
            analyzer.dashboards, os.path.join(output_dir, 'dashboards_report.csv'))
        ReportGenerator.generate_actions_report(
            analyzer.actions, os.path.join(output_dir, 'actions_report.csv'))
        ReportGenerator.generate_hierarchies_report(
            analyzer.hierarchies, os.path.join(output_dir, 'hierarchies_report.csv'))
        ReportGenerator.generate_summary_report(
            analyzer, os.path.join(output_dir, 'workbook_summary.csv'))
        
        # Generate new recommendation reports
        ReportGenerator.generate_backend_offload_recommendations(
            analyzer, os.path.join(output_dir, 'backend_offload_recommendations.csv'))
        ReportGenerator.generate_architecture_recommendations(
            analyzer, os.path.join(output_dir, 'architecture_recommendations.md'))
        
        # Generate DBT project
        dbt_generator = DBTProjectGenerator(analyzer, output_dir)
        dbt_generator.generate_project()
        
        logger.info("Analysis complete! Generated reports and DBT project in 'output_files' directory.")
        print("\nAnalysis complete! Generated files in 'output_files' directory:")
        for report_file in sorted(os.listdir(output_dir)):
            print(f"- {report_file}")
        
        print(f"\nSummary: Found {len(analyzer.calculations)} calculations, {len(analyzer.parameters)} parameters, and {len(analyzer.sheets)} sheets.")
        print(f"A suggested DBT project has been generated in {os.path.join(output_dir, 'suggested_dbt_project')}")
        
    except Exception as e:
        logger.error(f"Analysis failed: {e}", exc_info=True)
        print(f"\nError during analysis: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # Example usage (for testing in an IDE):
    # sys.argv.append('path/to/your/workbook.twbx') 
    main()
