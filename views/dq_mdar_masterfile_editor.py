import pandas as pd
import streamlit as st
from databricks import sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from typing import Dict, Any, List, Optional
import json
import re
from datetime import datetime
import pytz

# Dropdown configuration - update these values as needed
DROPDOWN_VALUES = {
    "data_owner": [
        "Beverly Dolor",
        "Bert Lorica",
        "Jose Yulo",
        "Nitin Chawre",
        "Pat Hipol",
        "Javy Olives",
        "Proletaryo Cabales",
        "Camille Valmores",
        "Leo Capote",
        "Justin Dayrit",
        "Mike Favila",
        "Alvin Wong",
        "Mel Valerio",
        "Thai Dinh",
        "Ma. Cecilia Jimlan",
        "Michael Obaldo",
        "Addison Pelayo",
        "Bryan Judelle Ramos",
        "Steven Michael Reyes",
        "Abi Tabayoyong",
        "Pranay Chukkala",
        "Alvin Delagon",
        "Patrick Hipol",
        "Geraldine Veracion",
        "Lance Cham",
        "Donna Glaraga",
    ],
    
    "tech_group": [
        "DG",
        "PE02",
        "PE01",
        "PE03",
        "DE",
        "PO - Javy",
        "IDEA",
        "ISS",
        "EDS",
        "CrPM",
        "PO - Nitin",
        "PO - Bert",
        "PO - Cams",
        "Ops",
        "Fraud Analytics",
    ],
    
    "overall_status": [
        "Closed",
        "Open",
    ],
    
    "mdar_priority": [
        "High",
        "Low",
        "Medium",
    ],
    
    "root_cause_category": [
        "Data Inconsistency",
        "Source Issue",
        "Ingestion",
    ],
    
    "dq_poc": [
        "Gibe",
        "Roy",
        "Lors",
        "Rev",
        "Mar",
    ],
    
    "internal_domain": [
        "Lending",
        "Negosyo",
        "Consumer",
        "Transaction - Wallet",
        "Merchant",
        "Transaction - Bank",
        "Treasury",
        "Deposit",
    ],
    
    "internal_subdomain": [
        "Regulatory",
        "CPM",
        "BAU",
        "IDEA",
        "MDM",
        "CrPM",
        "CMS",
        "Lending",
        "FCI",
        "Crypto",
        "DS",
        "Cypto",
        "Escalated",
        "Financial",
        "Anomalo",
        "Finance",
    ],
    
    "mesh_team": [
        "Consumer Lending",
        "User Profiles",
        "Accounts Management",
        "Marketing Tech",
        "Enterprise Products Core",
        "Credit Infrastructure",
        "Business Deposits",
        "Cards Management",
        "Core Payment Platforms",
        "Partner Settlements",
        "Business Lending",
        "SKU Management",
        "Partner Cash Solutions",
        "Crypto",
        "Transaction Assets",
        "Salesforce",
        "Identity and Access Management",
        "Online Acceptance",
        "Core Platform Lending",
        "Open Loop Payment Channels",
        "Consumer Deposits",
        "Money Movement",
        "DE",
        "DG - MDM",
        "ISS",
        "EDG - DQ",
        "IDEA",
        "OPIC",
        "SSAP",
        "Self Top-up",
        "InterBank Transfers",
        "Risk Tech",
        "DG - Metadata",
        "Ops",
        "CBS Platform",
        "Business Manager",
        "Developer Experience",
        "Data Engineering",
        "Tech Ops",
        "Fraud",
        "Base App - Mobile",
        "E-Commerce",
        "Account Assets",
        "Closed Loop Payment Channels",
    ],
    
    "timeline_year": [
        "2026",
        "2027",
        "2028",
    ],
    
    "timeline_month": [
        "1",
        "2",
        "3",
        "4",
        "5",
        "6",
        "7",
        "8",
        "9",
        "10",
        "11",
        "12",
    ],
    
    "timeline_quarter": [
        "Q1",
        "Q2",
        "Q3",
        "Q4",
    ],
}

# Fields that should use text_area for multi-line input
MULTILINE_FIELDS = [
    "updates",
    "resolution_updates",
    "notes",
    "comments",
    "description",
    "details",
]

st.header(body="DQ MDAR Inventory Masterfile", divider=True)
st.subheader("Form-based Table Editor")
st.write(
    "Manage records in the **dg_prod.sandbox.dq_mdar_inventory_masterfile** table using this user-friendly form interface."
)

# Pre-configured connection details
DATABRICKS_HOST = "dbc-7d305f7c-9def.cloud.databricks.com"
HTTP_PATH = "/sql/1.0/warehouses/80e5636f05f63c9b"
TABLE_NAME = "dg_prod.sandbox.dq_mdar_inventory_masterfile"

# Initialize session state
if 'selected_record' not in st.session_state:
    st.session_state.selected_record = None
if 'form_data' not in st.session_state:
    st.session_state.form_data = {}
if 'table_data' not in st.session_state:
    st.session_state.table_data = None
if 'table_schema' not in st.session_state:
    st.session_state.table_schema = None
if 'connection_established' not in st.session_state:
    st.session_state.connection_established = False

@st.cache_resource(ttl="1h")
def get_connection(server_hostname: str, http_path: str):
    """Create connection to Databricks SQL warehouse"""
    return sql.connect(
        server_hostname=server_hostname,
        http_path=http_path,
        credentials_provider=lambda: Config().authenticate,
    )

def get_table_schema(table_name: str, conn) -> Dict[str, str]:
    """Get table schema information"""
    with conn.cursor() as cursor:
        cursor.execute(f"DESCRIBE {table_name}")
        schema_info = cursor.fetchall()
        return {row[0]: row[1] for row in schema_info}

def get_manila_timestamp() -> str:
    """Get current timestamp in Manila timezone"""
    manila_tz = pytz.timezone('Asia/Manila')
    manila_time = datetime.now(manila_tz)
    return manila_time.strftime('%Y-%m-%d %H:%M:%S')

def validate_ticket_format(ticket: str) -> bool:
    """Validate ticket follows MDAR-#### format"""
    if not ticket or not isinstance(ticket, str):
        return False
    ticket = ticket.strip()
    return bool(re.match(r'^MDAR-\d+$', ticket))

def check_ticket_exists(ticket: str, table_data: pd.DataFrame) -> bool:
    """Check if ticket already exists in the table"""
    if table_data is None or len(table_data) == 0:
        return False
    
    # Check if 'ticket' column exists
    if 'ticket' not in table_data.columns:
        return False
    
    # Check if the ticket exists (case-insensitive)
    ticket = ticket.strip().upper()
    existing_tickets = table_data['ticket'].astype(str).str.strip().str.upper()
    return ticket in existing_tickets.values

def validate_new_record(record_data: Dict[str, Any]) -> tuple[bool, str]:
    """
    Validate new record data.
    Returns: (is_valid, error_message)
    """
    # Define optional fields (including auto-generated timestamp fields)
    optional_fields = ['root_cause', 'timeline_year', 'timeline_month', 'timeline_quarter', 'created_pht', 'updated_pht']
    
    # Check mandatory fields
    for field, value in record_data.items():
        if field not in optional_fields:
            # Check if field is empty or placeholder
            if value is None or \
               (isinstance(value, str) and (value.strip() == "" or value == "-- Select --")):
                return False, f"Field '{field}' is mandatory and cannot be empty."
    
    # Check timeline conditional logic
    timeline_year = record_data.get('timeline_year', '')
    timeline_month = record_data.get('timeline_month', '')
    timeline_quarter = record_data.get('timeline_quarter', '')
    
    # Clean up placeholder values
    if timeline_year == "-- Select --":
        timeline_year = ""
    if timeline_month == "-- Select --":
        timeline_month = ""
    if timeline_quarter == "-- Select --":
        timeline_quarter = ""
    
    # If timeline_year is filled, then month or quarter must be filled
    if timeline_year and isinstance(timeline_year, str) and timeline_year.strip():
        if (not timeline_month or (isinstance(timeline_month, str) and not timeline_month.strip())) and \
           (not timeline_quarter or (isinstance(timeline_quarter, str) and not timeline_quarter.strip())):
            return False, "If 'timeline_year' is provided, either 'timeline_month' or 'timeline_quarter' must be filled."
    
    return True, ""

def read_table(table_name: str, conn, limit: int = 1000) -> pd.DataFrame:
    """Read table data with optional limit"""
    with conn.cursor() as cursor:
        query = f"SELECT * FROM {table_name} LIMIT {limit}"
        cursor.execute(query)
        return cursor.fetchall_arrow().to_pandas()

def insert_record(table_name: str, record_data: Dict[str, Any], conn):
    """Insert a new record"""
    # Clean up placeholder values
    for key, value in record_data.items():
        if value == "-- Select --":
            record_data[key] = ""
    
    # Add timestamps for Manila timezone
    manila_timestamp = get_manila_timestamp()
    record_data['created_pht'] = manila_timestamp
    record_data['updated_pht'] = manila_timestamp
    
    # Validate and clean ticket field
    if 'ticket' in record_data:
        record_data['ticket'] = record_data['ticket'].strip()
        if not validate_ticket_format(record_data['ticket']):
            raise ValueError(f"Invalid ticket format: '{record_data['ticket']}'. Must follow pattern MDAR-#### (e.g., MDAR-1234)")
    
    columns = list(record_data.keys())
    values = []
    
    for val in record_data.values():
        if val is None or val == "":
            values.append("NULL")
        elif isinstance(val, str):
            escaped_val = val.replace("'", "''")  # Escape single quotes
            values.append(f"'{escaped_val}'")
        else:
            values.append(str(val))
    
    columns_str = ", ".join(columns)
    values_str = ", ".join(values)
    
    with conn.cursor() as cursor:
        query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({values_str})"
        cursor.execute(query)

def update_record(table_name: str, record_data: Dict[str, Any], where_clause: str, conn):
    """Update an existing record"""
    # Update the updated_pht timestamp to current Manila time
    record_data['updated_pht'] = get_manila_timestamp()
    
    # Validate and clean ticket field
    if 'ticket' in record_data:
        record_data['ticket'] = record_data['ticket'].strip()
        if not validate_ticket_format(record_data['ticket']):
            raise ValueError(f"Invalid ticket format: '{record_data['ticket']}'. Must follow pattern MDAR-#### (e.g., MDAR-1234)")
    
    set_clauses = []
    
    for col, val in record_data.items():
        # Don't update created_pht - it should remain unchanged
        if col == 'created_pht':
            continue
            
        if val is None or val == "":
            set_clauses.append(f"{col} = NULL")
        elif isinstance(val, str):
            escaped_val = val.replace("'", "''")  # Escape single quotes
            set_clauses.append(f"{col} = '{escaped_val}'")
        else:
            set_clauses.append(f"{col} = {val}")
    
    set_clause = ", ".join(set_clauses)
    
    with conn.cursor() as cursor:
        query = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}"
        cursor.execute(query)

def delete_record(table_name: str, where_clause: str, conn):
    """Delete a record"""
    with conn.cursor() as cursor:
        query = f"DELETE FROM {table_name} WHERE {where_clause}"
        cursor.execute(query)

def render_form_field(column_name: str, column_type: str, current_value: Any = None, key_suffix: str = ""):
    """Render appropriate form field based on column type"""
    if current_value is None:
        current_value = ""
    
    # Handle pandas NaN values
    if pd.isna(current_value):
        current_value = ""
    
    field_key = f"{column_name}_{key_suffix}" if key_suffix else column_name
    
    # Special handling for timestamp fields - make them read-only and auto-populated
    if column_name.lower() in ['created_pht', 'updated_pht']:
        # For new records, show placeholder
        if key_suffix == "add":
            display_value = "(Auto-generated on save)"
        else:
            display_value = str(current_value) if current_value else "(Not set)"
        
        st.text_input(
            f"{column_name} ({column_type})",
            value=display_value,
            disabled=True,
            help="Automatically set to Manila time (PHT)",
            key=field_key
        )
        return current_value  # Return original value, not the display value
    
    # Special handling for ticket field - enforce MDAR-#### pattern
    if column_name.lower() == "ticket":
        ticket_value = st.text_input(
            f"{column_name} ({column_type})",
            value=str(current_value).strip() if current_value != "" and current_value is not None else "",
            max_chars=20,
            help="Format: MDAR-#### (e.g., MDAR-1234)",
            key=field_key
        )
        # Validate the ticket format
        if ticket_value and not re.match(r'^MDAR-\d+$', ticket_value.strip()):
            st.warning("⚠️ Ticket must follow format: MDAR-#### (e.g., MDAR-1234)")
        return ticket_value.strip()  # Always strip trailing spaces
    
    # Check if this field should be a dropdown
    if column_name in DROPDOWN_VALUES:
        options = DROPDOWN_VALUES[column_name]
        
        # For new records (key_suffix="add"), add a blank placeholder
        if key_suffix == "add":
            options_with_placeholder = ["-- Select --"] + options
            # Find the index of current value
            try:
                if current_value and current_value != "":
                    default_index = options_with_placeholder.index(str(current_value))
                else:
                    default_index = 0  # Default to placeholder
            except (ValueError, AttributeError):
                default_index = 0
            
            return st.selectbox(
                f"{column_name} ({column_type})",
                options=options_with_placeholder,
                index=default_index,
                key=field_key
            )
        else:
            # For editing existing records, use original options
            try:
                if current_value and current_value != "":
                    default_index = options.index(str(current_value))
                else:
                    default_index = 0
            except (ValueError, AttributeError):
                default_index = 0
            
            return st.selectbox(
                f"{column_name} ({column_type})",
                options=options,
                index=default_index,
                key=field_key
            )
    
    # Convert column type to appropriate Streamlit input
    if "int" in column_type.lower() or "bigint" in column_type.lower():
        try:
            default_val = int(current_value) if current_value != "" and current_value is not None else 0
        except (ValueError, TypeError):
            default_val = 0
        return st.number_input(
            f"{column_name} ({column_type})",
            value=default_val,
            step=1,
            key=field_key
        )
    elif "float" in column_type.lower() or "double" in column_type.lower() or "decimal" in column_type.lower():
        try:
            default_val = float(current_value) if current_value != "" and current_value is not None else 0.0
        except (ValueError, TypeError):
            default_val = 0.0
        return st.number_input(
            f"{column_name} ({column_type})",
            value=default_val,
            step=0.01,
            key=field_key
        )
    elif "boolean" in column_type.lower():
        try:
            default_val = bool(current_value) if current_value != "" and current_value is not None else False
        except (ValueError, TypeError):
            default_val = False
        return st.checkbox(
            f"{column_name} ({column_type})",
            value=default_val,
            key=field_key
        )
    elif "date" in column_type.lower() and "timestamp" not in column_type.lower():
        return st.date_input(
            f"{column_name} ({column_type})",
            key=field_key
        )
    elif "timestamp" in column_type.lower():
        return st.text_input(
            f"{column_name} ({column_type})",
            value=str(current_value) if current_value != "" and current_value is not None else "",
            help="Format: YYYY-MM-DD HH:MM:SS",
            key=field_key
        )
    else:  # Default to text input for strings and other types
        # Check if this field should use text_area for multi-line input
        if column_name.lower() in [field.lower() for field in MULTILINE_FIELDS]:
            return st.text_area(
                f"{column_name} ({column_type})",
                value=str(current_value) if current_value != "" and current_value is not None else "",
                height=150,
                help="Supports multi-line text with line breaks and paragraphs",
                key=field_key
            )
        else:
            return st.text_input(
                f"{column_name} ({column_type})",
                value=str(current_value) if current_value != "" and current_value is not None else "",
                key=field_key
            )

# Main interface
tab_form, tab_view = st.tabs(["**Form Editor**", "**Table View**"])

with tab_form:
    # Connection info display
    st.info(f"🔗 **Connection Details:**\n- Host: `{DATABRICKS_HOST}`\n- Warehouse: `{HTTP_PATH}`\n- Table: `{TABLE_NAME}`")
    
    # Connect button
    if st.button("🔌 Connect to Table", type="primary"):
        try:
            with st.spinner("Connecting to Databricks..."):
                conn = get_connection(DATABRICKS_HOST, HTTP_PATH)
                st.session_state.table_data = read_table(TABLE_NAME, conn)
                st.session_state.table_schema = get_table_schema(TABLE_NAME, conn)
                st.session_state.connection_established = True
            st.success("✅ Successfully connected to the table!")
            st.rerun()
        except Exception as e:
            st.error(f"❌ Connection failed: {str(e)}")
            st.session_state.connection_established = False
    
    # Show connection status
    if st.session_state.connection_established and st.session_state.table_data is not None:
        st.success(f"✅ Connected to {TABLE_NAME} ({len(st.session_state.table_data)} records)")
        
        # Refresh button
        if st.button("🔄 Refresh Data"):
            try:
                with st.spinner("Refreshing data..."):
                    conn = get_connection(DATABRICKS_HOST, HTTP_PATH)
                    st.session_state.table_data = read_table(TABLE_NAME, conn)
                    st.session_state.table_schema = get_table_schema(TABLE_NAME, conn)
                st.success("✅ Data refreshed!")
                st.rerun()
            except Exception as e:
                st.error(f"❌ Refresh failed: {str(e)}")
        
        # Action selection
        action = st.radio(
            "Select Action:",
            ["View/Edit Record", "Add New Record", "Delete Record"],
            horizontal=True
        )
        
        if action == "View/Edit Record":
            st.subheader("📝 Edit Existing Record")
            
            # Record selection
            if len(st.session_state.table_data) > 0:
                # Create more readable record options with a placeholder
                record_options = ["-- Select a record to edit --"]
                for i, row in st.session_state.table_data.iterrows():
                    # Show first few non-null columns for identification
                    display_cols = []
                    for col, val in row.items():
                        if pd.notna(val) and val != "" and len(display_cols) < 3:
                            display_cols.append(f"{col}={val}")
                    record_display = f"Row {i}: {' | '.join(display_cols)}"
                    record_options.append(record_display)
                
                selected_option = st.selectbox(
                    "Select record to edit:",
                    range(len(record_options)),
                    format_func=lambda x: record_options[x],
                    index=0
                )
                
                # Only show form if a valid record is selected (not the placeholder)
                if selected_option > 0:
                    selected_idx = selected_option - 1  # Adjust for placeholder
                    selected_record = st.session_state.table_data.iloc[selected_idx]
                    
                    # Show current record in expandable section
                    with st.expander("📋 Current Record Details", expanded=False):
                        st.json(selected_record.to_dict())
                    
                    # Form for editing
                    with st.form("edit_record_form"):
                        st.write("**Edit Record:**")
                        form_data = {}
                        
                        # Create form fields in columns for better layout
                        cols = st.columns(2)
                        col_idx = 0
                        
                        for column, dtype in st.session_state.table_schema.items():
                            with cols[col_idx % 2]:
                                current_value = selected_record.get(column, "")
                                form_data[column] = render_form_field(column, dtype, current_value, "edit")
                            col_idx += 1
                        
                        col_save, col_cancel = st.columns(2)
                        with col_save:
                            save_changes = st.form_submit_button("💾 Save Changes", type="primary")
                        with col_cancel:
                            cancel_changes = st.form_submit_button("❌ Cancel")
                        
                        if save_changes:
                            try:
                                conn = get_connection(DATABRICKS_HOST, HTTP_PATH)
                                # Create WHERE clause using first column as identifier
                                first_col = list(st.session_state.table_schema.keys())[0]
                                first_val = selected_record[first_col]
                                if isinstance(first_val, str):
                                    escaped_val = first_val.replace("'", "''")
                                    where_clause = f"{first_col} = '{escaped_val}'"
                                else:
                                    where_clause = f"{first_col} = {first_val}"
                                
                                update_record(TABLE_NAME, form_data, where_clause, conn)
                                st.success("✅ Record updated successfully!")
                                st.session_state.table_data = None  # Force refresh
                                st.rerun()
                            except Exception as e:
                                st.error(f"❌ Error updating record: {str(e)}")
            else:
                st.info("No records found in the table.")
        
        elif action == "Add New Record":
            st.subheader("➕ Add New Record")
            
            # Show mandatory field info
            st.info("ℹ️ **Mandatory fields:** All fields except root_cause, timeline_year, timeline_month, and timeline_quarter.\n\n"
                   "**Note:** If timeline_year is filled, then either timeline_month or timeline_quarter must be provided.")
            
            with st.form("add_record_form"):
                form_data = {}
                
                # Create form fields in columns for better layout
                cols = st.columns(2)
                col_idx = 0
                
                for column, dtype in st.session_state.table_schema.items():
                    with cols[col_idx % 2]:
                        form_data[column] = render_form_field(column, dtype, key_suffix="add")
                    col_idx += 1
                
                col_add, col_clear = st.columns(2)
                with col_add:
                    add_record_btn = st.form_submit_button("➕ Add Record", type="primary")
                with col_clear:
                    clear_form = st.form_submit_button("🗑️ Clear Form")
                
                if add_record_btn:
                    # Check if ticket already exists
                    ticket_value = form_data.get('ticket', '').strip()
                    if check_ticket_exists(ticket_value, st.session_state.table_data):
                        st.error("❌ Ticket already exists. Please add a new one or go to View/Edit to update details.")
                    else:
                        # Validate the record
                        is_valid, error_msg = validate_new_record(form_data)
                        
                        if not is_valid:
                            st.error(f"❌ Validation Error: {error_msg}")
                        else:
                            try:
                                conn = get_connection(DATABRICKS_HOST, HTTP_PATH)
                                insert_record(TABLE_NAME, form_data, conn)
                                st.success("✅ Record added successfully!")
                                st.session_state.table_data = None  # Force refresh
                                st.rerun()
                            except Exception as e:
                                st.error(f"❌ Error adding record: {str(e)}")
        
        elif action == "Delete Record":
            st.subheader("🗑️ Delete Record")
            st.warning("⚠️ This action cannot be undone!")
            
            if len(st.session_state.table_data) > 0:
                # Create more readable record options with a placeholder
                record_options = ["-- Select a record to delete --"]
                for i, row in st.session_state.table_data.iterrows():
                    display_cols = []
                    for col, val in row.items():
                        if pd.notna(val) and val != "" and len(display_cols) < 3:
                            display_cols.append(f"{col}={val}")
                    record_display = f"Row {i}: {' | '.join(display_cols)}"
                    record_options.append(record_display)
                
                selected_option = st.selectbox(
                    "Select record to delete:",
                    range(len(record_options)),
                    format_func=lambda x: record_options[x],
                    index=0
                )
                
                # Only show delete confirmation if a valid record is selected (not the placeholder)
                if selected_option > 0:
                    selected_idx = selected_option - 1  # Adjust for placeholder
                    selected_record = st.session_state.table_data.iloc[selected_idx]
                    
                    st.write("**Record to Delete:**")
                    st.json(selected_record.to_dict())
                    
                    if st.button("🗑️ Confirm Delete", type="secondary"):
                        try:
                            conn = get_connection(DATABRICKS_HOST, HTTP_PATH)
                            # Create WHERE clause using first column as identifier
                            first_col = list(st.session_state.table_schema.keys())[0]
                            first_val = selected_record[first_col]
                            if isinstance(first_val, str):
                                escaped_val = first_val.replace("'", "''")
                                where_clause = f"{first_col} = '{escaped_val}'"
                            else:
                                where_clause = f"{first_col} = {first_val}"
                            
                            delete_record(TABLE_NAME, where_clause, conn)
                            st.success("✅ Record deleted successfully!")
                            st.session_state.table_data = None  # Force refresh
                            st.rerun()
                        except Exception as e:
                            st.error(f"❌ Error deleting record: {str(e)}")
            else:
                st.info("No records found in the table.")
    else:
        st.info("👆 Click 'Connect to Table' to start managing your data.")

with tab_view:
    st.subheader("📊 Table Data View with Inline Editing")
    
    if st.session_state.table_data is not None:
        st.info("💡 **Tip:** Click on any cell to edit. Changes are saved when you click 'Save Changes' button below.")
        
        # Search functionality
        search_term = st.text_input("🔍 Search records:", placeholder="Enter search term...", key="search_table_view")
        
        display_data = st.session_state.table_data.copy()
        
        if search_term and search_term.strip():
            # Simple search across all columns
            search_lower = search_term.lower().strip()
            mask = display_data.astype(str).apply(
                lambda x: x.str.lower().str.contains(search_lower, na=False, regex=False)
            ).any(axis=1)
            display_data = display_data[mask]
            st.info(f"Found {len(display_data)} records matching '{search_term}'")
        
        # Configure column types for data editor
        column_config = {}
        
        # Make timestamp fields read-only
        if 'created_pht' in display_data.columns:
            column_config['created_pht'] = st.column_config.TextColumn(
                "created_pht",
                disabled=True,
                help="Auto-generated timestamp (read-only)"
            )
        if 'updated_pht' in display_data.columns:
            column_config['updated_pht'] = st.column_config.TextColumn(
                "updated_pht",
                disabled=True,
                help="Auto-updated timestamp (read-only)"
            )
        
        # Editable data editor
        edited_data = st.data_editor(
            display_data,
            use_container_width=True,
            hide_index=True,
            num_rows="fixed",
            column_config=column_config,
            key="data_editor"
        )
        
        # Save changes button
        col_save, col_cancel = st.columns([1, 4])
        with col_save:
            if st.button("💾 Save Changes", type="primary"):
                try:
                    conn = get_connection(DATABRICKS_HOST, HTTP_PATH)
                    
                    # Find changed rows
                    changes_made = False
                    for idx in edited_data.index:
                        original_row = display_data.loc[idx]
                        edited_row = edited_data.loc[idx]
                        
                        # Check if row was modified
                        if not original_row.equals(edited_row):
                            # Get the ticket for WHERE clause
                            ticket = original_row['ticket']
                            
                            # Validate ticket format
                            if not validate_ticket_format(str(ticket)):
                                st.error(f"❌ Invalid ticket format for row with ticket '{ticket}'. Skipping.")
                                continue
                            
                            # Create record data from edited row
                            record_data = edited_row.to_dict()
                            
                            # Create WHERE clause
                            escaped_ticket = str(ticket).replace("'", "''")
                            where_clause = f"ticket = '{escaped_ticket}'"
                            
                            # Update the record
                            update_record(TABLE_NAME, record_data, where_clause, conn)
                            changes_made = True
                    
                    if changes_made:
                        st.success("✅ Changes saved successfully!")
                        st.session_state.table_data = None  # Force refresh
                        st.rerun()
                    else:
                        st.info("ℹ️ No changes detected.")
                        
                except Exception as e:
                    st.error(f"❌ Error saving changes: {str(e)}")
        
        # Table statistics
        st.divider()
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Records", len(st.session_state.table_data))
        with col2:
            st.metric("Displayed", len(display_data))
        with col3:
            st.metric("Columns", len(st.session_state.table_data.columns))
        with col4:
            memory_kb = st.session_state.table_data.memory_usage(deep=True).sum() / 1024
            st.metric("Memory Usage", f"{memory_kb:.1f} KB")
    else:
        st.info("Connect to the table in the Form Editor tab to view data here.")
