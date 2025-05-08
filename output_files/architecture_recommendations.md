# Architectural Recommendations for Tableau Workbook

## 1. Calculation Consolidation
- **Merge similar calculations:** Consider consolidating calculations that appear to be variations of a common logic.
  - Example group (base 'Parameter'): Parameter 2, Parameter 3, Parameter 4...
  - Example group (base 'Calculation'): Calculation_1392175277239238660, Calculation_700872758527991827, Calculation_700872758530551828...
  - Example group (base 'Open Demand (copy)'): Open Demand (copy)_1663235650134941703, Open Demand (copy)_1709679020568346626, Open Demand (copy)_2016486762012291073
  - Example group (base 'Dest. Group (Level 2, Oils - PO)* (copy)'): Dest. Group (Level 2, Oils - PO)* (copy)_547187366505148416, Dest. Group (Level 2, Oils - PO)* (copy)_547187366510571521, Dest. Group (Level 2, Oils - PO)* (copy)_547187366513963011
  Review `calculations_report.csv` for calculations with similar names or formulas (e.g., 'Dest Group 1', 'Dest Group 2').

## 2. Hierarchy Management
- No explicit hierarchies were found. If implicit hierarchies (e.g., related fields used together consistently) exist, consider formalizing them in the backend data model.

## 3. Filter Optimization
- **Consolidate filters that repeat across sheets:** Certain fields are used as filters on multiple sheets.
  - Examples: sqlproxy.1kpyl5p0e3iu3n15utyig0bois6v].[none:Calculation_804455546384973826:nk (used on 76 sheets), sqlproxy.1kpyl5p0e3iu3n15utyig0bois6v].[none:Commodity:nk (used on 93 sheets), sqlproxy.1kpyl5p0e3iu3n15utyig0bois6v].[md:AsOfDate:ok (used on 117 sheets), sqlproxy.1kpyl5p0e3iu3n15utyig0bois6v].[none:CategoryGroup:nk (used on 82 sheets), sqlproxy.1kpyl5p0e3iu3n15utyig0bois6v].[none:DataSource:nk (used on 56 sheets)...
  Consider creating shared filter logic (e.g., via parameters that drive calculated fields used as filters, or by standardizing filter application if appropriate). Also, review `actions_report.csv` for repetitive filter actions.

## 4. Parameter Usage
- Review parameters (see `parameters_report.csv`). If any have static, limited choices and drive core logic, consider if they represent data attributes that could be moved to the backend.

## 5. Data Source Integration
- **Replace data blending with joins where possible:** Sheets using multiple data sources may indicate data blending.
  - Sheets to review: 'All Major Exporters by Month' (uses: Parameters, sqlproxy.1kpyl5p0e3iu3n15utyig0bois6v); 'All Major Exporters by Quarter' (uses: Parameters, sqlproxy.1kpyl5p0e3iu3n15utyig0bois6v); 'All Major Exporters by Year' (uses: Parameters, sqlproxy.1kpyl5p0e3iu3n15utyig0bois6v)...
  Consult `data_sources_report.csv` to understand the fields within these sources. If common keys exist, using joins at the database level or in Tableau's data source pane is generally more performant and manageable than blending within worksheets.
