import io
import pandas as pd
import streamlit as st
from databricks import sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from typing import Dict, Any
from datetime import datetime
import pytz

# Pre-configured connection details
DATABRICKS_HOST = "dbc-7d305f7c-9def.cloud.databricks.com"
HTTP_PATH = "/sql/1.0/warehouses/80e5636f05f63c9b"
UPLOAD_VOLUME = "dg_dev.sandbox.csv_uploads"

# Initialize Databricks clients
w = WorkspaceClient()

# Initialize session state
if 'uploaded_df' not in st.session_state:
    st.session_state.uploaded_df = None
if 'upload_success' not in st.session_state:
    st.session_state.upload_success = False

@st.cache_resource(ttl="1h")
def get_connection(server_hostname: str, http_path: str):
    """Create connection to Databricks SQL warehouse"""
    return sql.connect(
        server_hostname=server_hostname,
        http_path=http_path,
        credentials_provider=lambda: Config().authenticate,
    )

def get_current_user_email() -> str:
    """Get the current user's Databricks email - no caching to ensure fresh data"""
    try:
        # Method 1: Try SQL query to get current user (most reliable in Databricks Apps)
        try:
            conn = get_connection(DATABRICKS_HOST, HTTP_PATH)
            with conn.cursor() as cursor:
                cursor.execute("SELECT current_user()")
                result = cursor.fetchone()
                if result and result[0]:
                    user_value = result[0]
                    # If it's an email, return it
                    if '@' in str(user_value):
                        return str(user_value)
        except Exception as sql_error:
            pass
        
        # Method 2: Try Streamlit's experimental user info
        if hasattr(st, 'experimental_user') and st.experimental_user:
            user_email = st.experimental_user.get('email')
            if user_email and '@' in user_email:
                return user_email
        
        # Method 3: Try WorkspaceClient
        w = WorkspaceClient()
        current_user = w.current_user.me()
        
        # Check if we got a user_name (email)
        if current_user.user_name and '@' in str(current_user.user_name):
            return current_user.user_name
        
        # Try to get email from emails array
        if hasattr(current_user, 'emails') and current_user.emails and len(current_user.emails) > 0:
            email_value = current_user.emails[0].value
            if email_value and '@' in email_value:
                return email_value
        
        # Try display name
        if hasattr(current_user, 'display_name') and current_user.display_name and '@' in current_user.display_name:
            return current_user.display_name
        
        # If we only have an ID, return it
        user_id = str(current_user.id) if hasattr(current_user, 'id') and current_user.id else "unknown"
        return user_id
        
    except Exception as e:
        return "unknown@databricks.com"

def get_manila_timestamp() -> str:
    """Get current timestamp in Manila timezone"""
    manila_tz = pytz.timezone('Asia/Manila')
    manila_time = datetime.now(manila_tz)
    return manila_time.strftime('%Y-%m-%d_%H-%M-%S')

def check_upload_permissions(volume_name: str) -> str:
    """Check if user has permissions to upload to volume"""
    try:
        volume = w.volumes.read(name=volume_name)
        current_user = w.current_user.me()
        catalog = w.catalogs.get(name=volume.catalog_name)
        grants = w.grants.get_effective(
            securable_type="volume",
            full_name=volume.full_name,
            principal=current_user.user_name,
        )
        
        if catalog.owner == current_user.user_name:
            return "valid"
        
        if not grants or not grants.privilege_assignments:
            return "No grants found"
        
        for assignment in grants.privilege_assignments:
            for privilege in assignment.privileges:
                if privilege.privilege.value in ["ALL_PRIVILEGES", "WRITE_VOLUME"]:
                    return "valid"
        
        return "Required privileges not found"
    except Exception as e:
        return f"Error: {str(e)}"

def upload_csv_to_volume(uploaded_file, volume_path: str) -> str:
    """Upload CSV to Unity Catalog Volume"""
    uploaded_file.seek(0)  # Reset file pointer
    file_bytes = uploaded_file.read()
    binary_data = io.BytesIO(file_bytes)
    
    # Add timestamp to filename to avoid conflicts
    timestamp = get_manila_timestamp()
    original_name = uploaded_file.name.rsplit('.', 1)[0]
    extension = uploaded_file.name.rsplit('.', 1)[1] if '.' in uploaded_file.name else 'csv'
    file_name = f"{original_name}_{timestamp}.{extension}"
    
    parts = volume_path.strip().split(".")
    catalog, schema, volume_name = parts[0], parts[1], parts[2]
    volume_file_path = f"/Volumes/{catalog}/{schema}/{volume_name}/{file_name}"
    
    w.files.upload(volume_file_path, binary_data, overwrite=True)
    return volume_file_path

def infer_sql_type(dtype) -> str:
    """Infer SQL data type from pandas dtype"""
    if dtype == 'object':
        return 'STRING'
    elif dtype == 'int64':
        return 'BIGINT'
    elif dtype == 'float64':
        return 'DOUBLE'
    elif dtype == 'bool':
        return 'BOOLEAN'
    elif 'datetime' in str(dtype):
        return 'TIMESTAMP'
    else:
        return 'STRING'

def table_exists(table_name: str, conn) -> bool:
    """Check if table exists"""
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"DESCRIBE TABLE {table_name}")
            return True
    except Exception:
        return False

def create_table_from_dataframe(df: pd.DataFrame, table_name: str, conn):
    """Create Delta table from DataFrame"""
    columns_def = []
    for col, dtype in df.dtypes.items():
        sql_type = infer_sql_type(dtype)
        # Escape column names with backticks
        columns_def.append(f"`{col}` {sql_type}")
    
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {', '.join(columns_def)}
    ) USING DELTA
    """
    
    with conn.cursor() as cursor:
        cursor.execute(create_sql)

def insert_data_to_table(df: pd.DataFrame, table_name: str, conn, mode: str = "append"):
    """Insert DataFrame data into Delta table"""
    rows = list(df.itertuples(index=False, name=None))
    if not rows:
        return
    
    cols = list(df.columns)
    params = {}
    values_sql_parts = []
    p = 0
    
    # Build parameterized query
    for row in rows:
        ph = []
        for v in row:
            key = f"p{p}"
            ph.append(f":{key}")
            # Handle None/NaN values
            if pd.isna(v):
                params[key] = None
            else:
                params[key] = v
            p += 1
        values_sql_parts.append("(" + ",".join(ph) + ")")
    
    values_sql = ",".join(values_sql_parts)
    col_list_sql = ",".join([f"`{col}`" for col in cols])
    
    with conn.cursor() as cursor:
        if mode == "overwrite":
            cursor.execute(
                f"INSERT OVERWRITE {table_name} ({col_list_sql}) VALUES {values_sql}",
                params
            )
        else:  # append
            cursor.execute(
                f"INSERT INTO {table_name} ({col_list_sql}) VALUES {values_sql}",
                params
            )

# Page header
st.header(body="CSV Upload to Databricks Table", divider=True)
st.subheader("Bulk Data Import")

# Display current user
current_user = get_current_user_email()
if '@' in current_user:
    st.info(f"👤 **Logged in as:** {current_user}")
else:
    st.warning(f"⚠️ **User ID:** {current_user}")

# Main tabs
tab_upload, tab_config, tab_help = st.tabs(["📤 Upload CSV", "⚙️ Configuration", "❓ Help"])

with tab_upload:
    st.write("Upload a CSV file to create or update a Databricks Delta table.")
    
    # File uploader
    uploaded_file = st.file_uploader(
        "Choose a CSV file",
        type=['csv'],
        help="Select a CSV file to upload. Maximum recommended size: 100MB"
    )
    
    if uploaded_file:
        try:
            # Read and preview CSV
            uploaded_file.seek(0)
            df = pd.read_csv(uploaded_file)
            st.session_state.uploaded_df = df
            
            # Display preview
            st.success(f"✅ CSV loaded successfully!")
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Rows", len(df))
            with col2:
                st.metric("Columns", len(df.columns))
            with col3:
                st.metric("File Size", f"{uploaded_file.size / 1024:.1f} KB")
            
            # Preview data
            with st.expander("📋 Data Preview (First 10 rows)", expanded=True):
                st.dataframe(df.head(10), use_container_width=True)
            
            # Column information
            with st.expander("📊 Column Information"):
                col_info = pd.DataFrame({
                    'Column Name': df.columns,
                    'Data Type': df.dtypes.astype(str),
                    'SQL Type': [infer_sql_type(dtype) for dtype in df.dtypes],
                    'Non-Null Count': [df[col].notna().sum() for col in df.columns],
                    'Null Count': [df[col].isna().sum() for col in df.columns]
                })
                st.dataframe(col_info, use_container_width=True, hide_index=True)
            
            st.markdown("---")
            
            # Target table configuration
            st.subheader("🎯 Target Table Configuration")
            
            col_left, col_right = st.columns(2)
            
            with col_left:
                target_table = st.text_input(
                    "Target Table Name:",
                    placeholder="dg_dev.sandbox.out_merchant_business_size_for_bank",
                    help="Format: catalog.schema.table_name"
                )
            
            with col_right:
                upload_mode = st.selectbox(
                    "Upload Mode:",
                    ["Create New Table", "Append to Existing Table", "Overwrite Existing Table"],
                    help="Choose how to handle the data"
                )
            
            # Additional options
            with st.expander("🔧 Advanced Options"):
                add_metadata = st.checkbox(
                    "Add metadata columns",
                    value=True,
                    help="Add upload_timestamp and uploaded_by columns"
                )
                
                if add_metadata:
                    st.info("The following columns will be added automatically:")
                    st.code("""
upload_timestamp: TIMESTAMP (Manila timezone)
uploaded_by: STRING (user email)
                    """)
            
            # Upload button
            st.markdown("---")
            
            if st.button("🚀 Upload and Create/Update Table", type="primary", use_container_width=True):
                if not target_table:
                    st.error("❌ Please specify a target table name")
                else:
                    try:
                        progress_bar = st.progress(0)
                        status_text = st.empty()
                        
                        # Step 1: Check permissions
                        status_text.info("🔐 Checking permissions...")
                        progress_bar.progress(10)
                        
                        permission_check = check_upload_permissions(UPLOAD_VOLUME)
                        if permission_check != "valid":
                            st.warning(f"⚠️ Volume permission check: {permission_check}")
                            st.info("Proceeding with upload attempt...")
                        
                        # Step 2: Upload to volume
                        status_text.info("📤 Uploading CSV to Unity Catalog Volume...")
                        progress_bar.progress(30)
                        
                        volume_path = upload_csv_to_volume(uploaded_file, UPLOAD_VOLUME)
                        st.success(f"✅ CSV uploaded to: `{volume_path}`")
                        
                        # Step 3: Prepare data
                        status_text.info("📊 Preparing data...")
                        progress_bar.progress(50)
                        
                        df_to_upload = df.copy()
                        
                        # Add metadata if requested
                        if add_metadata:
                            manila_tz = pytz.timezone('Asia/Manila')
                            df_to_upload['upload_timestamp'] = datetime.now(manila_tz).strftime('%Y-%m-%d %H:%M:%S')
                            df_to_upload['uploaded_by'] = current_user
                        
                        # Step 4: Connect to SQL warehouse
                        status_text.info("🔌 Connecting to SQL warehouse...")
                        progress_bar.progress(60)
                        
                        conn = get_connection(DATABRICKS_HOST, HTTP_PATH)
                        
                        # Step 5: Create or check table
                        status_text.info("🏗️ Creating/checking table...")
                        progress_bar.progress(70)
                        
                        if upload_mode == "Create New Table":
                            if table_exists(target_table, conn):
                                st.error(f"❌ Table `{target_table}` already exists. Choose 'Append' or 'Overwrite' mode.")
                                progress_bar.empty()
                                status_text.empty()
                            else:
                                create_table_from_dataframe(df_to_upload, target_table, conn)
                                st.success(f"✅ Table `{target_table}` created successfully!")
                                
                                # Step 6: Insert data
                                status_text.info("💾 Inserting data...")
                                progress_bar.progress(85)
                                
                                insert_data_to_table(df_to_upload, target_table, conn, mode="append")
                                
                                progress_bar.progress(100)
                                status_text.success("✅ Upload complete!")
                                st.balloons()
                                
                                st.success(f"""
                                **Upload Summary:**
                                - ✅ {len(df_to_upload)} rows inserted
                                - ✅ Table: `{target_table}`
                                - ✅ Backup: `{volume_path}`
                                """)
                        
                        elif upload_mode == "Append to Existing Table":
                            if not table_exists(target_table, conn):
                                st.error(f"❌ Table `{target_table}` does not exist. Choose 'Create New Table' mode.")
                                progress_bar.empty()
                                status_text.empty()
                            else:
                                # Step 6: Insert data
                                status_text.info("💾 Appending data...")
                                progress_bar.progress(85)
                                
                                insert_data_to_table(df_to_upload, target_table, conn, mode="append")
                                
                                progress_bar.progress(100)
                                status_text.success("✅ Upload complete!")
                                st.balloons()
                                
                                st.success(f"""
                                **Upload Summary:**
                                - ✅ {len(df_to_upload)} rows appended
                                - ✅ Table: `{target_table}`
                                - ✅ Backup: `{volume_path}`
                                """)
                        
                        else:  # Overwrite
                            if not table_exists(target_table, conn):
                                create_table_from_dataframe(df_to_upload, target_table, conn)
                                st.info(f"ℹ️ Table `{target_table}` created (did not exist)")
                            
                            # Step 6: Overwrite data
                            status_text.info("💾 Overwriting data...")
                            progress_bar.progress(85)
                            
                            insert_data_to_table(df_to_upload, target_table, conn, mode="overwrite")
                            
                            progress_bar.progress(100)
                            status_text.success("✅ Upload complete!")
                            st.balloons()
                            
                            st.success(f"""
                            **Upload Summary:**
                            - ✅ {len(df_to_upload)} rows written (overwrite mode)
                            - ✅ Table: `{target_table}`
                            - ✅ Backup: `{volume_path}`
                            """)
                        
                        st.session_state.upload_success = True
                        
                    except Exception as e:
                        st.error(f"❌ Error during upload: {str(e)}")
                        st.exception(e)
        
        except Exception as e:
            st.error(f"❌ Error reading CSV file: {str(e)}")
            st.info("Please ensure the file is a valid CSV format.")
    
    else:
        st.info("👆 Upload a CSV file to get started")

with tab_config:
    st.subheader("Current Configuration")
    
    st.write("**Databricks Connection:**")
    st.code(f"""
Host: {DATABRICKS_HOST}
SQL Warehouse Path: {HTTP_PATH}
Upload Volume: {UPLOAD_VOLUME}
    """)
    
    st.write("**Supported Data Types:**")
    type_mapping = pd.DataFrame({
        'Pandas Type': ['object', 'int64', 'float64', 'bool', 'datetime64'],
        'SQL Type': ['STRING', 'BIGINT', 'DOUBLE', 'BOOLEAN', 'TIMESTAMP']
    })
    st.dataframe(type_mapping, use_container_width=True, hide_index=True)
    
    st.write("**Required Permissions:**")
    st.markdown("""
    **For Volume Upload:**
    - `USE CATALOG` on the catalog
    - `USE SCHEMA` on the schema
    - `WRITE VOLUME` on the volume
    
    **For Table Operations:**
    - `CREATE TABLE` on the schema (for new tables)
    - `MODIFY` on the table (for append/overwrite)
    - `SELECT` on the table (for validation)
    
    **For SQL Warehouse:**
    - `CAN USE` on the SQL warehouse
    """)

with tab_help:
    st.subheader("How to Use CSV Upload")
    
    st.markdown("""
    ### Step-by-Step Guide
    
    1. **Prepare Your CSV File**
       - Ensure your CSV has proper headers
       - Check for data quality (no extra commas, proper encoding)
       - Recommended max size: 100MB
    
    2. **Upload the File**
       - Click "Choose a CSV file" button
       - Select your CSV file
       - Preview the data to verify it loaded correctly
    
    3. **Configure Target Table**
       - Enter the full table name: `catalog.schema.table_name`
       - Choose upload mode:
         - **Create New**: Creates a new table (fails if exists)
         - **Append**: Adds data to existing table
         - **Overwrite**: Replaces all data in existing table
    
    4. **Review Options**
       - Enable metadata columns to track upload info
       - Check column types in the preview
    
    5. **Upload**
       - Click "Upload and Create/Update Table"
       - Wait for confirmation
       - CSV is backed up to Volume automatically
    
    ### Upload Modes Explained
    
    **Create New Table:**
    - Best for first-time uploads
    - Creates table structure from CSV
    - Fails if table already exists (safety feature)
    
    **Append to Existing Table:**
    - Adds new rows to existing data
    - Keeps all existing records
    - Column names must match existing table
    
    **Overwrite Existing Table:**
    - Replaces all data in table
    - Keeps table structure
    - Use with caution!
    
    ### Integration with Maker-Checker Workflow
    
    After uploading CSV data:
    1. Records are available in the target table
    2. Makers can review and update records
    3. Checkers can approve as normal
    4. Original CSV is backed up in Volume for audit trail
    
    ### Troubleshooting
    
    **"Permission denied" error:**
    - Contact admin to grant volume/table permissions
    
    **"Table already exists" error:**
    - Use "Append" or "Overwrite" mode instead
    
    **"Column mismatch" error:**
    - Ensure CSV columns match table structure
    - Check column names and types
    
    **Large file upload slow:**
    - Consider splitting into smaller files
    - Upload during off-peak hours
    """)
    
    st.info("💡 **Tip:** Always test with a small sample CSV first before uploading large datasets!")

# Footer
st.markdown("---")
st.caption("CSV Upload Feature | Databricks Apps")
