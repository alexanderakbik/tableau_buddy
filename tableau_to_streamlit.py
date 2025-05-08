import os
import xml.etree.ElementTree as ET
import xmltodict
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from typing import Dict, List, Any, Optional
import zipfile
import tempfile
import shutil
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TableauWorkbookConverter:
    def __init__(self, workbook_path: str):
        """Initialize the converter with a Tableau workbook path."""
        self.workbook_path = workbook_path
        self.workbook_data = None
        self.extracted_dir = None
        self.embedded_data_path = None
        self.embedded_data_files = []
        self.xml_data_df = None  # For .twb inline data
        
    def _extract_twbx(self) -> str:
        """Extract .twbx file to temporary directory and return path to .twb file. Also, find embedded data files."""
        if not self.workbook_path.endswith('.twbx'):
            return self.workbook_path
            
        self.extracted_dir = tempfile.mkdtemp()
        with zipfile.ZipFile(self.workbook_path, 'r') as zip_ref:
            zip_ref.extractall(self.extracted_dir)
        
        # Find the .twb file in the extracted directory
        twb_path = None
        for file in os.listdir(self.extracted_dir):
            if file.endswith('.twb'):
                twb_path = os.path.join(self.extracted_dir, file)
            # Collect embedded data files
            if file.endswith('.csv') or file.endswith('.hyper') or file.endswith('.tde'):
                self.embedded_data_files.append(os.path.join(self.extracted_dir, file))
        if not twb_path:
            raise ValueError("No .twb file found in the .twbx archive")
        return twb_path

    def _get_embedded_csv(self) -> Optional[str]:
        """Return the path to the first embedded CSV file, if any."""
        for f in self.embedded_data_files:
            if f.endswith('.csv'):
                return f
        return None

    def _parse_workbook(self) -> Dict:
        """Parse the Tableau workbook XML structure."""
        try:
            if self.workbook_path.endswith('.twb'):
                twb_path = self.workbook_path
            else:
                twb_path = self._extract_twbx()
            logger.info(f"Reading workbook from: {twb_path}")
            
            with open(twb_path, 'r', encoding='utf-8') as f:
                content = f.read()
                
            # Clean the XML content
            content = content.replace('&', '&amp;')  # Fix common XML entity issues
            
            # Try parsing with xmltodict
            try:
                return xmltodict.parse(content)
            except Exception as e:
                logger.error(f"Error parsing with xmltodict: {str(e)}")
                
                # Fallback to ElementTree
                try:
                    root = ET.fromstring(content)
                    # Convert ElementTree to dict
                    def etree_to_dict(t):
                        return {t.tag: list(map(etree_to_dict, t))} if len(t) > 0 else t.text
                    return etree_to_dict(root)
                except Exception as e2:
                    logger.error(f"Error parsing with ElementTree: {str(e2)}")
                    raise
                    
        except Exception as e:
            logger.error(f"Error reading workbook: {str(e)}")
            raise

    def _extract_inline_data_from_twb(self):
        # Only for .twb files
        if not self.workbook_path.endswith('.twb'):
            return None
        try:
            tree = ET.parse(self.workbook_path)
            root = tree.getroot()
            ns = {'t': root.tag.split('}')[0].strip('{')} if '}' in root.tag else {}
            # Find the first datasource with a <rows> section
            for datasource in root.findall('.//t:datasource', ns):
                for relation in datasource.findall('.//t:relation', ns):
                    rows = relation.find('t:rows', ns)
                    if rows is not None:
                        data = []
                        columns = []
                        for row in rows.findall('t:row', ns):
                            row_data = {}
                            for cell in row:
                                row_data[cell.tag.split('}', 1)[-1]] = cell.text
                                if cell.tag.split('}', 1)[-1] not in columns:
                                    columns.append(cell.tag.split('}', 1)[-1])
                            data.append(row_data)
                        if data:
                            df = pd.DataFrame(data, columns=columns)
                            # Try to convert numeric columns
                            for col in df.columns:
                                df[col] = pd.to_numeric(df[col], errors='ignore')
                            self.xml_data_df = df
                            return df
            return None
        except Exception as e:
            logger.error(f"Error extracting inline data from .twb: {str(e)}")
            return None

    def _extract_datasources(self) -> List[Dict]:
        """Extract data source information from the workbook."""
        try:
            if not self.workbook_data:
                self.workbook_data = self._parse_workbook()
                
            datasources = []
            if 'workbook' in self.workbook_data and 'datasources' in self.workbook_data['workbook']:
                ds_list = self.workbook_data['workbook']['datasources'].get('datasource', [])
                if not isinstance(ds_list, list):
                    ds_list = [ds_list]
                    
                for ds in ds_list:
                    datasources.append({
                        'name': ds.get('name', ''),
                        'caption': ds.get('caption', ''),
                        'connection': ds.get('connection', {})
                    })
            return datasources
        except Exception as e:
            logger.error(f"Error extracting datasources: {str(e)}")
            return []

    def _extract_worksheets(self) -> List[Dict]:
        """Extract worksheet information from the workbook."""
        try:
            if not self.workbook_data:
                self.workbook_data = self._parse_workbook()
                
            worksheets = []
            if 'workbook' in self.workbook_data and 'worksheets' in self.workbook_data['workbook']:
                ws_list = self.workbook_data['workbook']['worksheets'].get('worksheet', [])
                if not isinstance(ws_list, list):
                    ws_list = [ws_list]
                    
                for ws in ws_list:
                    worksheets.append({
                        'name': ws.get('name', ''),
                        'caption': ws.get('caption', ''),
                        'view': ws.get('view', {})
                    })
            return worksheets
        except Exception as e:
            logger.error(f"Error extracting worksheets: {str(e)}")
            return []

    def _create_plotly_chart(self, worksheet: Dict, data: pd.DataFrame) -> go.Figure:
        """Create a Plotly chart based on worksheet information."""
        try:
            worksheet_name = worksheet['name'].lower()
            
            if 'sales' in worksheet_name:
                # Create a sales dashboard with multiple metrics
                fig = go.Figure()
                
                # Add Sales and Profit bars
                if 'Region' in data.columns and 'Sales' in data.columns:
                    fig.add_trace(go.Bar(
                        x=data['Region'],
                        y=data['Sales'],
                        name='Sales',
                        marker_color='rgb(55, 83, 109)'
                    ))
                
                if 'Region' in data.columns and 'Profit' in data.columns:
                    fig.add_trace(go.Bar(
                        x=data['Region'],
                        y=data['Profit'],
                        name='Profit',
                        marker_color='rgb(26, 118, 255)'
                    ))
                
                if 'Region' in data.columns and 'Profit Ratio' in data.columns:
                    fig.add_trace(go.Scatter(
                        x=data['Region'],
                        y=data['Profit Ratio'],
                        name='Profit Ratio',
                        yaxis='y2',
                        mode='lines+markers',
                        marker_color='orange'
                    ))
                    fig.update_layout(
                        yaxis2=dict(title='Profit Ratio', overlaying='y', side='right')
                    )
                
                fig.update_layout(
                    title='Sales and Profit by Region',
                    barmode='group',
                    xaxis_title='Region',
                    yaxis_title='Amount',
                    showlegend=True
                )
                
            elif 'customer detail' in worksheet_name:
                # Create a customer analysis dashboard
                fig = go.Figure()
                
                # Create a scatter plot of Customer Score vs Sales
                fig.add_trace(go.Scatter(
                    x=data['Customer Score'],
                    y=data['Sales'],
                    mode='markers',
                    marker=dict(
                        size=10,
                        color=data['Customer Score'],
                        colorscale='Viridis',
                        showscale=True
                    ),
                    text=data['Customer ID'],
                    name='Customer Score vs Sales'
                ))
                
                fig.update_layout(
                    title='Customer Score vs Sales',
                    xaxis_title='Customer Score',
                    yaxis_title='Sales',
                    showlegend=False
                )
                
            elif 'product metrics' in worksheet_name:
                # Create a product analysis dashboard
                fig = go.Figure()
                
                # Create a grouped bar chart for product categories
                categories = data['Category'].unique()
                for category in categories:
                    category_data = data[data['Category'] == category]
                    fig.add_trace(go.Bar(
                        x=[category],
                        y=[category_data['Final Price'].mean()],
                        name=category
                    ))
                
                fig.update_layout(
                    title='Average Final Price by Category',
                    xaxis_title='Category',
                    yaxis_title='Average Final Price',
                    showlegend=True
                )
                
            elif 'combined dashboard logic' in worksheet_name:
                # Create a combined metrics dashboard
                fig = go.Figure()
                
                # Create a line chart showing the relationship between metrics
                fig.add_trace(go.Scatter(
                    x=data['Sales'],
                    y=data['Customer Score'],
                    mode='lines+markers',
                    name='Sales vs Customer Score'
                ))
                
                fig.update_layout(
                    title='Sales vs Customer Score Relationship',
                    xaxis_title='Sales',
                    yaxis_title='Customer Score',
                    showlegend=True
                )
                
            else:
                # Default visualization
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=data.iloc[:, 0],
                    y=data.iloc[:, 1],
                    mode='lines+markers',
                    name=worksheet['name']
                ))
                
                fig.update_layout(
                    title=worksheet['caption'] or worksheet['name'],
                    xaxis_title='X Axis',
                    yaxis_title='Y Axis'
                )
            
            return fig
        except Exception as e:
            logger.error(f"Error creating chart for {worksheet.get('name', 'unknown')}: {str(e)}")
            # Return a simple error chart
            fig = go.Figure()
            fig.add_annotation(
                text=f"Error creating chart: {str(e)}",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False
            )
            return fig

    def create_streamlit_dashboard(self, data: Optional[pd.DataFrame] = None):
        """Create a Streamlit dashboard from the Tableau workbook."""
        try:
            st.set_page_config(layout="wide")
            st.title("Tableau Workbook Dashboard")
            
            # Extract workbook information
            datasources = self._extract_datasources()
            worksheets = self._extract_worksheets()
            
            # Display data source information
            st.sidebar.header("Data Sources")
            for ds in datasources:
                st.sidebar.write(f"**{ds['name']}**")
                st.sidebar.write(f"Caption: {ds['caption']}")
            
            # If no data provided, try to use embedded CSV
            if data is None and self.embedded_data_files:
                csv_path = self._get_embedded_csv()
                if csv_path:
                    st.info(f"Using embedded CSV: {os.path.basename(csv_path)}")
                    data = pd.read_csv(csv_path)
                else:
                    st.warning("No embedded CSV found. Embedded data files:")
                    for f in self.embedded_data_files:
                        st.write(os.path.basename(f))
                    st.stop()
            elif data is None and self.workbook_path.endswith('.twb'):
                # Try to extract inline data from .twb
                data = self._extract_inline_data_from_twb()
                if data is not None:
                    st.info("Using inline data from .twb file.")
                else:
                    st.warning("No inline data found in the .twb file.")
                    st.stop()
            elif data is None:
                st.warning("No data provided and no embedded data found in the .twbx file.")
                st.stop()
            
            # Add filters in the sidebar
            st.sidebar.header("Filters")
            if data is not None and 'Region' in data.columns:
                regions = ['All'] + list(data['Region'].unique())
                selected_region = st.sidebar.selectbox('Select Region', regions)
                if selected_region != 'All':
                    data = data[data['Region'] == selected_region]
            
            # Create tabs for each worksheet
            if worksheets:
                tabs = st.tabs([ws['name'] for ws in worksheets])
                for tab, worksheet in zip(tabs, worksheets):
                    with tab:
                        st.subheader(worksheet['caption'] or worksheet['name'])
                        if data is not None:
                            fig = self._create_plotly_chart(worksheet, data)
                            st.plotly_chart(fig, use_container_width=True)
                            st.write("Data Table:")
                            st.dataframe(data)
                        else:
                            st.warning("No data provided for visualization")
            else:
                st.error("No worksheets found in the workbook")
            
        except Exception as e:
            st.error(f"Error creating dashboard: {str(e)}")
            logger.error(f"Error creating dashboard: {str(e)}")
        finally:
            # Cleanup temporary files
            if self.extracted_dir and os.path.exists(self.extracted_dir):
                shutil.rmtree(self.extracted_dir)

def main():
    """Main function to run the Streamlit dashboard."""
    try:
        workbook_path = "examplefiles/SalesDashboard.twb"
        # No data_path: rely on embedded CSV
        converter = TableauWorkbookConverter(workbook_path)
        converter.create_streamlit_dashboard()
    except Exception as e:
        st.error(f"Error running dashboard: {str(e)}")
        logger.error(f"Error running dashboard: {str(e)}")

if __name__ == "__main__":
    main() 