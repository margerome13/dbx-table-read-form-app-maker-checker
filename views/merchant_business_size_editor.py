import pandas as pd
import streamlit as st
from databricks import sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from typing import Dict, Any, List, Optional
import json
from datetime import datetime
import pytz

# Pre-configured connection details
DATABRICKS_HOST = "dbc-7d305f7c-9def.cloud.databricks.com"
HTTP_PATH = "/sql/1.0/warehouses/80e5636f05f63c9b"

# Available tables
AVAILABLE_TABLES = {
    "Dev - Merchant Business Size": "dg_dev.sandbox.out_merchant_business_size_for_bank",
    "Prod Test - Merchant Business Size": "dg_prod.sandbox.out_merchant_business_size_for_bank_test"
}

# Dropdown values for review fields
BUSINESS_SIZE_OPTIONS = ["", "MICRO", "SMALL", "MEDIUM", "LARGE"]
GENDER_OPTIONS = ["", "MALE", "FEMALE"]

# Workflow statuses
STATUS_PENDING = "PENDING"
STATUS_APPROVED = "APPROVED"
STATUS_REJECTED = "REJECTED"

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
if 'selected_table' not in st.session_state:
    st.session_state.selected_table = list(AVAILABLE_TABLES.keys())[0]
if 'current_table_name' not in st.session_state:
    st.session_state.current_table_name = AVAILABLE_TABLES[list(AVAILABLE_TABLES.keys())[0]]
if 'user_role' not in st.session_state:
    st.session_state.user_role = "MAKER"  # Default role

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
    return manila_time.strftime('%Y-%m-%d %H:%M:%S')

def get_table_schema(table_name: str, conn) -> Dict[str, str]:
    """Get table schema information"""
    with conn.cursor() as cursor:
        cursor.execute(f"DESCRIBE {table_name}")
        schema_info = cursor.fetchall()
        return {row[0]: row[1] for row in schema_info}

def read_table(table_name: str, conn, limit: int = 1000, status_filter: str = None) -> pd.DataFrame:
    """Read table data with optional status filter"""
    with conn.cursor() as cursor:
        if status_filter:
            query = f"SELECT * FROM {table_name} WHERE review_status = '{status_filter}' LIMIT {limit}"
        else:
            query = f"SELECT * FROM {table_name} LIMIT {limit}"
        cursor.execute(query)
        return cursor.fetchall_arrow().to_pandas()

def update_record(table_name: str, record_data: Dict[str, Any], where_clause: str, conn):
    """Update an existing record"""
    set_clauses = []
    
    for col, val in record_data.items():
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

def render_form_field(column_name: str, column_type: str, current_value: Any = None, key_suffix: str = "", disabled: bool = False):
    """Render appropriate form field based on column type"""
    if current_value is None:
        current_value = ""
    
    # Handle pandas NaN values
    if pd.isna(current_value):
        current_value = ""
    
    field_key = f"{column_name}_{key_suffix}" if key_suffix else column_name
    
    # Special handling for review fields with dropdowns
    if column_name in ["business_reviewed_size_pending", "business_reviewed_size"]:
        current_str = str(current_value) if current_value != "" and current_value is not None else ""
        try:
            default_index = BUSINESS_SIZE_OPTIONS.index(current_str) if current_str in BUSINESS_SIZE_OPTIONS else 0
        except ValueError:
            default_index = 0
        return st.selectbox(
            f"{column_name}",
            options=BUSINESS_SIZE_OPTIONS,
            index=default_index,
            key=field_key,
            help="Select business size: MICRO, SMALL, MEDIUM, or LARGE",
            disabled=disabled
        )
    elif column_name in ["business_reviewed_gender_pending", "business_reviewed_gender"]:
        current_str = str(current_value) if current_value != "" and current_value is not None else ""
        try:
            default_index = GENDER_OPTIONS.index(current_str) if current_str in GENDER_OPTIONS else 0
        except ValueError:
            default_index = 0
        return st.selectbox(
            f"{column_name}",
            options=GENDER_OPTIONS,
            index=default_index,
            key=field_key,
            help="Select gender: MALE or FEMALE",
            disabled=disabled
        )
    else:
        # Default to text input for other types
        return st.text_input(
            f"{column_name} ({column_type})",
            value=str(current_value) if current_value != "" and current_value is not None else "",
            key=field_key,
            disabled=disabled
        )

# Page header and user info
st.header(body="Merchant Business Size and Gender Review", divider=True)
st.subheader("Maker-Checker Workflow")

# Display current user
current_user = get_current_user_email()
if '@' in current_user:
    st.info(f"👤 **Logged in as:** {current_user}")
else:
    st.warning(f"⚠️ **User ID:** {current_user}")

# Role selector
col_role, col_table = st.columns(2)
with col_role:
    user_role = st.selectbox(
        "🎭 Select Your Role:",
        options=["MAKER", "CHECKER"],
        index=0 if st.session_state.user_role == "MAKER" else 1,
        help="Makers submit reviews, Checkers approve/reject them"
    )
    st.session_state.user_role = user_role

with col_table:
    selected_table_name = st.selectbox(
        "📊 Select Table:",
        options=list(AVAILABLE_TABLES.keys()),
        index=list(AVAILABLE_TABLES.keys()).index(st.session_state.selected_table)
    )
    
    if selected_table_name != st.session_state.selected_table:
        st.session_state.selected_table = selected_table_name
        st.session_state.current_table_name = AVAILABLE_TABLES[selected_table_name]
        st.session_state.connection_established = False
        st.session_state.table_data = None
        st.session_state.table_schema = None

TABLE_NAME = st.session_state.current_table_name

# Connection section
st.info(f"🔗 **Table:** `{TABLE_NAME}`")

if st.button("🔌 Connect to Table", type="primary"):
    try:
        with st.spinner("Connecting to Databricks..."):
            conn = get_connection(DATABRICKS_HOST, HTTP_PATH)
            st.session_state.table_data = read_table(TABLE_NAME, conn)
            st.session_state.table_schema = get_table_schema(TABLE_NAME, conn)
            st.session_state.connection_established = True
        st.success("✅ Successfully connected!")
        st.rerun()
    except Exception as e:
        st.error(f"❌ Connection failed: {str(e)}")
        st.session_state.connection_established = False

# Main workflow interface
if st.session_state.connection_established and st.session_state.table_data is not None:
    
    if user_role == "MAKER":
        # ============ MAKER INTERFACE ============
        st.markdown("---")
        st.subheader("📝 Maker: Submit Reviews")
        
        tab_submit, tab_my_submissions = st.tabs(["Submit New Review", "My Submissions"])
        
        with tab_submit:
            st.write("Select a record and fill in the business size and gender for review.")
            
            # Record selection
            if len(st.session_state.table_data) > 0:
                record_options = ["-- Select a record --"]
                for i, row in st.session_state.table_data.iterrows():
                    display_cols = []
                    for col, val in row.items():
                        if pd.notna(val) and val != "" and len(display_cols) < 3:
                            display_cols.append(f"{col}={val}")
                    record_display = f"Row {i}: {' | '.join(display_cols)}"
                    record_options.append(record_display)
                
                selected_option = st.selectbox(
                    "Select record:",
                    range(len(record_options)),
                    format_func=lambda x: record_options[x],
                    index=0,
                    key="maker_select"
                )
                
                if selected_option > 0:
                    selected_idx = selected_option - 1
                    selected_record = st.session_state.table_data.iloc[selected_idx]
                    
                    # Show current record details
                    with st.expander("📋 Record Details", expanded=True):
                        for column, value in selected_record.items():
                            if column not in ["business_reviewed_size_pending", "business_reviewed_gender_pending", 
                                            "review_status", "reviewed_by_maker", "reviewed_date_maker",
                                            "reviewed_by_checker", "reviewed_date_checker", "checker_comments"]:
                                st.text(f"{column}: {value}")
                    
                    # Show current status
                    current_status = selected_record.get("review_status", "")
                    if current_status == STATUS_PENDING:
                        st.warning("⏳ This record has a PENDING review")
                    elif current_status == STATUS_APPROVED:
                        st.success("✅ This record was APPROVED")
                    elif current_status == STATUS_REJECTED:
                        st.error("❌ This record was REJECTED")
                        if pd.notna(selected_record.get("checker_comments")):
                            st.info(f"💬 Checker comments: {selected_record.get('checker_comments')}")
                    
                    # Maker form
                    with st.form("maker_form"):
                        st.write("**Fill in Review Values:**")
                        
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            current_val = selected_record.get("business_reviewed_size_pending", "")
                            size_value = render_form_field(
                                "business_reviewed_size_pending",
                                "string",
                                current_val,
                                "maker"
                            )
                        
                        with col2:
                            current_val = selected_record.get("business_reviewed_gender_pending", "")
                            gender_value = render_form_field(
                                "business_reviewed_gender_pending",
                                "string",
                                current_val,
                                "maker"
                            )
                        
                        submit_btn = st.form_submit_button("📤 Submit for Approval", type="primary")
                        
                        if submit_btn:
                            if not size_value or not gender_value:
                                st.error("❌ Please fill in both business size and gender")
                            else:
                                try:
                                    conn = get_connection(DATABRICKS_HOST, HTTP_PATH)
                                    first_col = list(st.session_state.table_schema.keys())[0]
                                    first_val = selected_record[first_col]
                                    
                                    if isinstance(first_val, str):
                                        escaped_val = first_val.replace("'", "''")
                                        where_clause = f"{first_col} = '{escaped_val}'"
                                    else:
                                        where_clause = f"{first_col} = {first_val}"
                                    
                                    update_data = {
                                        "business_reviewed_size_pending": size_value,
                                        "business_reviewed_gender_pending": gender_value,
                                        "review_status": STATUS_PENDING,
                                        "reviewed_by_maker": current_user,
                                        "reviewed_date_maker": get_manila_timestamp()
                                    }
                                    
                                    update_record(TABLE_NAME, update_data, where_clause, conn)
                                    st.success("✅ Submitted for checker approval!")
                                    st.session_state.table_data = None
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"❌ Error: {str(e)}")
            else:
                st.info("No records found")
        
        with tab_my_submissions:
            st.write("View your submitted reviews and their status")
            
            if st.button("🔄 Refresh My Submissions"):
                st.session_state.table_data = None
                st.rerun()
            
            # Filter for current user's submissions
            my_submissions = st.session_state.table_data[
                st.session_state.table_data['reviewed_by_maker'] == current_user
            ]
            
            if len(my_submissions) > 0:
                # Status filter
                status_filter = st.multiselect(
                    "Filter by status:",
                    options=[STATUS_PENDING, STATUS_APPROVED, STATUS_REJECTED],
                    default=[STATUS_PENDING, STATUS_APPROVED, STATUS_REJECTED]
                )
                
                filtered_data = my_submissions[my_submissions['review_status'].isin(status_filter)]
                
                st.dataframe(
                    filtered_data[[
                        col for col in filtered_data.columns 
                        if col in ['business_reviewed_size_pending', 'business_reviewed_gender_pending',
                                 'review_status', 'reviewed_date_maker', 'reviewed_by_checker',
                                 'reviewed_date_checker', 'checker_comments']
                    ]],
                    use_container_width=True,
                    hide_index=True
                )
                
                st.metric("Total Submissions", len(my_submissions))
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Pending", len(my_submissions[my_submissions['review_status'] == STATUS_PENDING]))
                with col2:
                    st.metric("Approved", len(my_submissions[my_submissions['review_status'] == STATUS_APPROVED]))
                with col3:
                    st.metric("Rejected", len(my_submissions[my_submissions['review_status'] == STATUS_REJECTED]))
            else:
                st.info("You haven't submitted any reviews yet")
    
    else:
        # ============ CHECKER INTERFACE ============
        st.markdown("---")
        st.subheader("✅ Checker: Review & Approve")
        
        tab_pending, tab_all = st.tabs(["Pending Reviews", "All Reviews"])
        
        with tab_pending:
            if st.button("🔄 Refresh Pending"):
                st.session_state.table_data = None
                st.rerun()
            
            # Filter pending reviews
            pending_reviews = st.session_state.table_data[
                st.session_state.table_data['review_status'] == STATUS_PENDING
            ]
            
            if len(pending_reviews) > 0:
                st.info(f"📋 {len(pending_reviews)} pending review(s)")
                
                # Select a pending review
                record_options = ["-- Select a pending review --"]
                for i, row in pending_reviews.iterrows():
                    maker = row.get('reviewed_by_maker', 'Unknown')
                    date = row.get('reviewed_date_maker', 'Unknown')
                    size = row.get('business_reviewed_size_pending', '')
                    gender = row.get('business_reviewed_gender_pending', '')
                    record_display = f"By {maker} on {date} - Size: {size}, Gender: {gender}"
                    record_options.append(record_display)
                
                selected_option = st.selectbox(
                    "Select review to check:",
                    range(len(record_options)),
                    format_func=lambda x: record_options[x],
                    index=0,
                    key="checker_select"
                )
                
                if selected_option > 0:
                    selected_idx = list(pending_reviews.index)[selected_option - 1]
                    selected_record = pending_reviews.loc[selected_idx]
                    
                    # Show record details
                    with st.expander("📋 Full Record Details", expanded=False):
                        st.json(selected_record.to_dict())
                    
                    # Show review details
                    col1, col2 = st.columns(2)
                    with col1:
                        st.write("**Maker's Submission:**")
                        st.info(f"👤 Maker: {selected_record.get('reviewed_by_maker')}")
                        st.info(f"📅 Date: {selected_record.get('reviewed_date_maker')}")
                        st.success(f"📏 Size: {selected_record.get('business_reviewed_size_pending')}")
                        st.success(f"👥 Gender: {selected_record.get('business_reviewed_gender_pending')}")
                    
                    with col2:
                        st.write("**Checker Actions:**")
                        
                        # Option to edit before approving
                        with st.form("checker_form"):
                            st.write("Edit if needed before approving:")
                            
                            edited_size = render_form_field(
                                "business_reviewed_size",
                                "string",
                                selected_record.get('business_reviewed_size_pending'),
                                "checker_edit"
                            )
                            
                            edited_gender = render_form_field(
                                "business_reviewed_gender",
                                "string",
                                selected_record.get('business_reviewed_gender_pending'),
                                "checker_edit"
                            )
                            
                            checker_comments = st.text_area(
                                "Comments (optional):",
                                key="checker_comments"
                            )
                            
                            col_approve, col_reject = st.columns(2)
                            with col_approve:
                                approve_btn = st.form_submit_button("✅ Approve", type="primary")
                            with col_reject:
                                reject_btn = st.form_submit_button("❌ Reject", type="secondary")
                            
                            if approve_btn:
                                try:
                                    conn = get_connection(DATABRICKS_HOST, HTTP_PATH)
                                    first_col = list(st.session_state.table_schema.keys())[0]
                                    first_val = selected_record[first_col]
                                    
                                    if isinstance(first_val, str):
                                        escaped_val = first_val.replace("'", "''")
                                        where_clause = f"{first_col} = '{escaped_val}'"
                                    else:
                                        where_clause = f"{first_col} = {first_val}"
                                    
                                    update_data = {
                                        "business_reviewed_size": edited_size,
                                        "business_reviewed_gender": edited_gender,
                                        "review_status": STATUS_APPROVED,
                                        "reviewed_by_checker": current_user,
                                        "reviewed_date_checker": get_manila_timestamp(),
                                        "checker_comments": checker_comments if checker_comments else ""
                                    }
                                    
                                    update_record(TABLE_NAME, update_data, where_clause, conn)
                                    st.success("✅ Review APPROVED!")
                                    st.session_state.table_data = None
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"❌ Error: {str(e)}")
                            
                            if reject_btn:
                                if not checker_comments:
                                    st.error("❌ Please provide comments when rejecting")
                                else:
                                    try:
                                        conn = get_connection(DATABRICKS_HOST, HTTP_PATH)
                                        first_col = list(st.session_state.table_schema.keys())[0]
                                        first_val = selected_record[first_col]
                                        
                                        if isinstance(first_val, str):
                                            escaped_val = first_val.replace("'", "''")
                                            where_clause = f"{first_col} = '{escaped_val}'"
                                        else:
                                            where_clause = f"{first_col} = {first_val}"
                                        
                                        update_data = {
                                            "review_status": STATUS_REJECTED,
                                            "reviewed_by_checker": current_user,
                                            "reviewed_date_checker": get_manila_timestamp(),
                                            "checker_comments": checker_comments
                                        }
                                        
                                        update_record(TABLE_NAME, update_data, where_clause, conn)
                                        st.success("✅ Review REJECTED!")
                                        st.session_state.table_data = None
                                        st.rerun()
                                    except Exception as e:
                                        st.error(f"❌ Error: {str(e)}")
            else:
                st.success("🎉 No pending reviews!")
        
        with tab_all:
            st.write("View all reviews with their status")
            
            # Status filter
            status_filter = st.multiselect(
                "Filter by status:",
                options=[STATUS_PENDING, STATUS_APPROVED, STATUS_REJECTED],
                default=[STATUS_PENDING, STATUS_APPROVED, STATUS_REJECTED],
                key="all_status_filter"
            )
            
            filtered_data = st.session_state.table_data[
                st.session_state.table_data['review_status'].isin(status_filter)
            ]
            
            if len(filtered_data) > 0:
                st.dataframe(
                    filtered_data[[
                        col for col in filtered_data.columns 
                        if col in ['business_reviewed_size_pending', 'business_reviewed_gender_pending',
                                 'business_reviewed_size', 'business_reviewed_gender',
                                 'review_status', 'reviewed_by_maker', 'reviewed_date_maker',
                                 'reviewed_by_checker', 'reviewed_date_checker', 'checker_comments']
                    ]],
                    use_container_width=True,
                    hide_index=True
                )
                
                # Statistics
                st.markdown("### 📊 Statistics")
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Total", len(st.session_state.table_data))
                with col2:
                    st.metric("Pending", len(st.session_state.table_data[st.session_state.table_data['review_status'] == STATUS_PENDING]))
                with col3:
                    st.metric("Approved", len(st.session_state.table_data[st.session_state.table_data['review_status'] == STATUS_APPROVED]))
                with col4:
                    st.metric("Rejected", len(st.session_state.table_data[st.session_state.table_data['review_status'] == STATUS_REJECTED]))
            else:
                st.info("No reviews found")

else:
    st.info("👆 Click 'Connect to Table' to start")
