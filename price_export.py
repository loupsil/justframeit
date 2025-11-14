from flask import Blueprint, jsonify, request
import xmlrpc.client
import os
import json
import base64
from io import BytesIO, StringIO
from datetime import datetime
import logging
from dotenv import load_dotenv
import pandas as pd
import openpyxl
import re
from utils import log_route_call

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Create blueprint
price_export_bp = Blueprint('price-export', __name__)

def create_log_capture_handler():
    """Create a string handler to capture logs during processing"""
    log_capture_string = StringIO()
    ch = logging.StreamHandler(log_capture_string)
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    return ch, log_capture_string

# Odoo Configuration
    """
    Log route calls to Odoo ir.logging model.

    Args:
        models: Odoo models proxy (can be None - function will establish connection if needed)
        uid: User ID (can be None - function will establish connection if needed)
        route_name: Name of the route being called
        payload: The request payload (dict)
        server_logs: Captured server logs (string)
        response_data: The final response data returned by the route (dict)

    Returns:
        int: ID of the created log record, or None if logging failed
    """
    try:
        # If models or uid are missing, try to establish connection
        if not models or not uid:
            logger.info(f"Attempting to establish Odoo connection for logging: {route_name}")
            try:
                uid = get_uid()
                models = get_odoo_models()
                logger.info("Successfully established Odoo connection for logging")
            except Exception as conn_error:
                logger.warning(f"Cannot log to Odoo - failed to establish connection. Route: {route_name}, Error: {str(conn_error)}")
                return None

        logger.info(f"Logging route call to Odoo ir.logging model: {route_name}")

        # Format the log entry in a readable way
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Format payload as readable JSON
        payload_formatted = json.dumps(payload, indent=2, ensure_ascii=False)

        # Format response data as readable JSON
        response_formatted = json.dumps(response_data, indent=2, ensure_ascii=False)

        # Determine log level based on response status
        is_error = response_data.get('status') == 'error'
        log_level = 'error' if is_error else 'info'

        # Set appropriate name based on operation type and status
        log_name = 'Price Export Generation'

        # Create the notes content
        notes_content = f"""🕒 Timestamp: {timestamp}
📍 Route: {route_name}
⚠️  Status: {'ERROR' if is_error else 'SUCCESS'}

📋 PAYLOAD:
{payload_formatted}

📤 RESPONSE:
{response_formatted}

📝 SERVER LOGS:
{server_logs}

{'='*80}
"""

        # Create log record in Odoo ir.logging model
        log_vals = {
            'name': log_name,
            'message': response_formatted,
            'x_studio_notes': notes_content,
            'type': 'server',  # Standard type for server logs
            'level': log_level,  # Error level for failed operations, info for successful
            'path': f'Route: {route_name}',  # Required path field - set to route name
            'func': 'log_route_call',  # Function name where logging occurs
            'line': '0',  # Line number (set to 0 since we don't have actual line number)
            'dbname': ODOO_DB  # Database name from environment
        }

        # Try to create the log record in ir.logging model
        log_id = models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
            'ir.logging', 'create', [log_vals])

        logger.info(f"Successfully created log record with ID: {log_id}")
        return log_id

    except Exception as e:
        logger.error(f"Failed to create log record in Odoo: {str(e)}")
        # Don't raise the exception - logging failure shouldn't break the main flow
        return None

# Odoo Configuration
ODOO_URL = os.getenv('JUSTFRAMEIT_ODOO_URL')
ODOO_DB = os.getenv('JUSTFRAMEIT_ODOO_DB')
ODOO_USERNAME = os.getenv('JUSTFRAMEIT_ODOO_USERNAME')
ODOO_API_KEY = os.getenv('JUSTFRAMEIT_ODOO_API_KEY')

def get_odoo_common():
    """Get Odoo common endpoint"""
    try:
        common = xmlrpc.client.ServerProxy(f'{ODOO_URL}/xmlrpc/2/common', allow_none=True)
        return common
    except Exception as e:
        logger.error(f"Failed to connect to Odoo common endpoint: {str(e)}")
        raise

def get_odoo_models():
    """Get Odoo models endpoint"""
    try:
        models = xmlrpc.client.ServerProxy(f'{ODOO_URL}/xmlrpc/2/object', allow_none=True)
        return models
    except Exception as e:
        logger.error(f"Failed to connect to Odoo models endpoint: {str(e)}")
        raise

def get_uid():
    """Get Odoo user ID"""
    try:
        common = get_odoo_common()
        uid = common.authenticate(ODOO_DB, ODOO_USERNAME, ODOO_API_KEY, {})
        if not uid:
            raise Exception("Authentication failed")
        return uid
    except Exception as e:
        logger.error(f"Failed to authenticate with Odoo: {str(e)}")
        raise

def generate_price_export_excel(models, uid):
    """
    Generate Excel file using the exact same logic as the Jupyter notebook.
    Creates a new Excel file with 3 tabs populated with Odoo data.
    Returns the Excel file as bytes, total products, total pricelists, and total duration rules.
    """
    try:
        logger.info("Starting Excel price-export generation using exact Jupyter notebook logic")

        import time
        import shutil
        from openpyxl import load_workbook

        # =============================================
        # 📋 STEP 1: Fetch products with price computation = Surface or Circumference
        # =============================================
        logger.info("Fetching products with price computation = Surface or Circumference...")

        products = models.execute_kw(
            ODOO_DB, uid, ODOO_API_KEY,
            'product.product', 'search_read',
            [[['x_studio_price_computation', 'in', ['Surface', 'Circumference']]]],  # Filter by price computation
            {
                'fields': [
                    'name',
                    'id',
                    'x_studio_product_code',
                    'x_studio_location_code',
                    'description_ecommerce',
                    'x_studio_price_computation',
                    'standard_price',
                    'x_studio_associated_service',
                    'x_studio_associated_work_center',
                    'x_studio_associated_cost_per_employee_per_hour'
                ]
                # No limit - fetch all products
            }
        )

        total_products = len(products)
        logger.info(f"Fetched {total_products} products")

        # =============================================
        # 📋 STEP 2: Create new Excel file from template
        # =============================================
        # Use the empty template file (must be in the same directory as this script)
        template_file = 'justframeit pricelist template empty.xlsx'
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = f'justframeit_pricelist_{timestamp}.xlsx'

        # Copy the template to create a new file
        shutil.copy(template_file, output_file)
        logger.info(f"Created new file: {output_file}")

        # Load the newly created Excel file
        wb = load_workbook(output_file)

        # =============================================
        # 📋 STEP 3: Fill TAB 1 with products
        # =============================================
        logger.info("Filling TAB 1 with products data...")
        ws1 = wb.worksheets[0]  # First tab (index 0)

        # Starting row is A7 (row 7)
        start_row = 7
        start_time = time.time()

        # Fill data for each product
        for idx, product in enumerate(products):
            current_row = start_row + idx

            # Helper function to convert list/tuple values to string
            def convert_value(value):
                if isinstance(value, (list, tuple)):
                    # If it's a list/tuple, join elements or take the second element (name)
                    if len(value) > 1:
                        return str(value[1])  # Return the name part
                    return str(value[0]) if value else ''
                return value

            # Fill columns A through J with the product data
            ws1[f'A{current_row}'] = convert_value(product.get('name'))
            ws1[f'B{current_row}'] = convert_value(product.get('id'))
            ws1[f'C{current_row}'] = convert_value(product.get('x_studio_product_code'))
            ws1[f'D{current_row}'] = convert_value(product.get('x_studio_location_code'))
            ws1[f'E{current_row}'] = convert_value(product.get('description_ecommerce'))
            ws1[f'F{current_row}'] = convert_value(product.get('x_studio_price_computation'))
            ws1[f'G{current_row}'] = convert_value(product.get('standard_price'))
            ws1[f'H{current_row}'] = convert_value(product.get('x_studio_associated_service'))
            ws1[f'I{current_row}'] = convert_value(product.get('x_studio_associated_work_center'))
            ws1[f'J{current_row}'] = convert_value(product.get('x_studio_associated_cost_per_employee_per_hour'))

            if idx % 50 == 0:  # Log progress every 50 products
                logger.info(f"Filled row {current_row} on tab 1 with product: {product.get('name')}")

        # Save the workbook
        wb.save(output_file)
        logger.info(f"Successfully filled {len(products)} products into tab 1")

        # =============================================
        # 📋 STEP 4: Fetch pricelists and fill TAB 2
        # =============================================
        logger.info("Fetching pricelists and filling TAB 2...")

        pricelists = models.execute_kw(
            ODOO_DB, uid, ODOO_API_KEY,
            'product.pricelist', 'search_read',
            [[]],  # Empty domain = all pricelists
            {
                'fields': ['name', 'x_studio_price_discount']
            }
        )

        total_pricelists = len(pricelists)
        logger.info(f"Fetched {total_pricelists} pricelists")

        # Access the second tab (sheet)
        if len(wb.sheetnames) > 1:
            ws2 = wb.worksheets[1]
        else:
            logger.warning("Second tab not found in workbook")
            ws2 = None

        if ws2:
            # Starting row is A3 (row 3)
            pricelist_start_row = 3

            # Fill data for each pricelist
            for idx, pricelist in enumerate(pricelists):
                current_row = pricelist_start_row + idx

                # Helper function to convert list/tuple values to string
                def convert_value(value):
                    if isinstance(value, (list, tuple)):
                        if len(value) > 1:
                            return str(value[1])
                        return str(value[0]) if value else ''
                    return value

                # Fill columns A and B with pricelist data
                ws2[f'A{current_row}'] = convert_value(pricelist.get('name'))
                ws2[f'B{current_row}'] = convert_value(pricelist.get('x_studio_price_discount'))

                if idx % 50 == 0:  # Log progress every 50 pricelists
                    logger.info(f"Filled row {current_row} on tab 2 with pricelist: {pricelist.get('name')}")

            # Save the workbook again with pricelist data
            wb.save(output_file)
            logger.info(f"Successfully filled {len(pricelists)} pricelists into tab 2")

        # =============================================
        # 📋 STEP 5: Fetch service duration rules and fill TAB 3
        # =============================================
        logger.info("Fetching service duration rules and filling TAB 3...")

        duration_rules = models.execute_kw(
            ODOO_DB, uid, ODOO_API_KEY,
            'x_services_duration_rules', 'search_read',
            [[]],  # Empty domain = all duration rules
            {
                'fields': [
                    'x_associated_service',
                    'x_studio_work_center',
                    'x_studio_quantity',
                    'x_duurtijd_totaal'
                ]
            }
        )

        total_duration_rules = len(duration_rules)
        logger.info(f"Fetched {total_duration_rules} service duration rules")

        # Access the third tab (sheet)
        if len(wb.sheetnames) > 2:
            ws3 = wb.worksheets[2]
        else:
            logger.warning("Third tab not found in workbook")
            ws3 = None

        if ws3:
            # Starting row is A3 (row 3)
            duration_start_row = 3

            # Fill data for each duration rule
            for idx, rule in enumerate(duration_rules):
                current_row = duration_start_row + idx

                # Helper function to convert list/tuple values to string
                def convert_value(value):
                    if isinstance(value, (list, tuple)):
                        if len(value) > 1:
                            return str(value[1])
                        return str(value[0]) if value else ''
                    return value

                # Fill columns A through D with duration rule data
                ws3[f'A{current_row}'] = convert_value(rule.get('x_associated_service'))
                ws3[f'B{current_row}'] = convert_value(rule.get('x_studio_work_center'))
                ws3[f'C{current_row}'] = convert_value(rule.get('x_studio_quantity'))
                ws3[f'D{current_row}'] = convert_value(rule.get('x_duurtijd_totaal'))

                if idx % 50 == 0:  # Log progress every 50 rules
                    logger.info(f"Filled row {current_row} on tab 3 with service: {rule.get('x_associated_service')}")

            # Save the workbook again with duration rules data
            wb.save(output_file)
            logger.info(f"Successfully filled {len(duration_rules)} service duration rules into tab 3")

        # =============================================
        # 📋 STEP 6: Read the final Excel file and return bytes
        # =============================================
        logger.info("Reading final Excel file to return as bytes...")

        # Read the file back as bytes
        with open(output_file, 'rb') as f:
            excel_bytes = f.read()

        # Clean up the temporary file
        try:
            os.remove(output_file)
            logger.info(f"Cleaned up temporary file: {output_file}")
        except Exception as e:
            logger.warning(f"Failed to clean up temporary file {output_file}: {str(e)}")

        logger.info("Excel file generated successfully with all tabs populated")

        # Return: excel_bytes, products_count, pricelists_count, duration_rules_count, source_description
        return excel_bytes, total_products, total_pricelists, f"Generated from Odoo data: {total_products} products, {total_pricelists} pricelists, {total_duration_rules} duration rules"

    except Exception as e:
        logger.error(f"Error generating price-export Excel: {str(e)}")
        raise

def generate_csv_from_excel(excel_bytes):
    """
    Generate CSV from Excel bytes using the exact same logic as the Jupyter notebook.
    Uses xlwings to force calculations before extracting data.
    Takes Excel bytes, processes the first worksheet, and returns CSV bytes.
    """
    try:
        logger.info("Starting CSV generation from Excel bytes using xlwings calculation")

        import xlwings as xw
        import tempfile

        # Save Excel bytes to temporary file for xlwings
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as temp_excel:
            temp_excel.write(excel_bytes)
            temp_excel_path = temp_excel.name

        logger.info(f"Saved Excel bytes to temporary file: {temp_excel_path}")

        try:
            # Open with xlwings to force calculation
            logger.info("Opening Excel file with xlwings to force calculations")
            app = xw.App(visible=False)
            wb_xlwings = app.books.open(temp_excel_path)
            ws_xlwings = wb_xlwings.sheets[0]

            # Force calculation
            logger.info("Forcing Excel calculation...")
            wb_xlwings.app.calculate()

            # Save the calculated workbook to another temporary file
            with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as temp_calculated:
                temp_calculated_path = temp_calculated.name

            wb_xlwings.save(temp_calculated_path)
            wb_xlwings.close()
            app.quit()

            logger.info(f"Calculated workbook saved as: {temp_calculated_path}")

            # Now load with openpyxl
            logger.info("Loading calculated Excel file with openpyxl")
            wb = openpyxl.load_workbook(temp_calculated_path, data_only=True)
            logger.info("Calculated Excel file loaded successfully")

        finally:
            # Clean up the first temporary file
            try:
                os.remove(temp_excel_path)
                logger.info(f"Cleaned up temporary input file: {temp_excel_path}")
            except Exception as e:
                logger.warning(f"Could not remove temporary input file: {e}")

        # Access the first tab
        ws = wb.worksheets[0]
        logger.info(f"Accessing first worksheet: {ws.title}")

        # Headers are in row 6
        header_row = 6
        start_row = 7  # Data starts from row 7

        # Find the last row with data in column A (starting from row 7)
        last_row = start_row

        logger.info("Finding last row with data...")
        # Find where column A becomes 0 or empty
        for row in range(start_row, ws.max_row + 1):
            if row % 100 == 0:  # Log progress every 100 rows
                logger.info(f"   Checking row {row}...")
            cell_value = ws[f'A{row}'].value
            if cell_value == 0 or cell_value is None or cell_value == '':
                break
            last_row = row

        logger.info(f"Extracting data from row {start_row} to row {last_row}")

        # Get headers from row 6, but check row 5 for dimension indicators
        logger.info(f"Reading headers from row {header_row}...")
        headers = []
        valid_columns = []  # Track which columns to include
        dimension_columns = []  # Track which columns are dimensions
        found_first_dimension = False
        last_dimension_col = 0
        for col in range(1, ws.max_column + 1):
            if col % 50 == 0:  # Log progress every 50 columns
                logger.info(f"   Processing column {col}...")
            # Check row 5 for dimension indicator (e.g., "10.0 x 20.0")
            dimension_cell = ws.cell(row=5, column=col)
            dimension_value = dimension_cell.value

            # Get the header from row 6
            header_cell = ws.cell(row=header_row, column=col)
            header_value = header_cell.value

            # Stricter check: dimension must contain 'x' AND at least one number
            is_dimension = False
            if dimension_value and isinstance(dimension_value, str):
                # Check if it contains 'x' and at least one digit
                if 'x' in dimension_value.lower() and re.search(r'\d', dimension_value):
                    is_dimension = True
                    found_first_dimension = True
                    last_dimension_col = col

            # If we found dimensions before but now stopped finding them, stop processing columns
            if found_first_dimension and not is_dimension:
                logger.info(f"Stopping at column {col} - no more dimension headers found")
                break

            # If row 5 contains a valid dimension indicator, use it
            if is_dimension:
                headers.append(dimension_value)
                valid_columns.append(col)
                dimension_columns.append(len(headers) - 1)  # Track the index of this dimension column
            elif header_value and not found_first_dimension:
                # Only include non-dimension columns if we haven't found dimensions yet
                headers.append(header_value)
                valid_columns.append(col)
            elif not found_first_dimension:
                headers.append(f'Column_{col}')
                valid_columns.append(col)

        logger.info(f"Found {len(headers)} columns (last dimension column: {last_dimension_col})")

        # Extract data from row 7 onwards to the last valid row
        logger.info(f"Extracting data from {last_row - start_row + 1} rows...")
        data = []
        for row in range(start_row, last_row + 1):
            if (row - start_row) % 50 == 0:  # Log progress every 50 rows
                logger.info(f"   Processing row {row} ({row - start_row + 1}/{last_row - start_row + 1})...")
            row_data = []
            for idx, col in enumerate(valid_columns):  # Only extract data from valid columns
                cell = ws.cell(row=row, column=col)
                # Get the actual value, not the formula
                cell_value = cell.value

                # Check if this is a dimension column
                is_dimension_col = idx in dimension_columns

                # Handle special cases
                if cell_value is None:
                    row_data.append(None)
                elif isinstance(cell_value, str) and cell_value.startswith('!REF'):
                    # Handle reference errors
                    row_data.append(None)
                elif isinstance(cell_value, (int, float)):
                    # Round all numeric values to 2 decimal places
                    row_data.append(round(float(cell_value), 2))
                else:
                    row_data.append(cell_value)
            data.append(row_data)

        # Create DataFrame
        logger.info("Creating DataFrame...")
        df = pd.DataFrame(data, columns=headers)
        logger.info(f"DataFrame created with {len(df)} rows and {len(df.columns)} columns")

        # Convert DataFrame to CSV bytes
        logger.info("Converting DataFrame to CSV bytes...")
        csv_buffer = BytesIO()
        df.to_csv(csv_buffer, index=False)
        csv_bytes = csv_buffer.getvalue()

        logger.info(f"Successfully generated CSV with {len(df)} rows from Excel data")

        # Clean up the calculated temporary file
        try:
            os.remove(temp_calculated_path)
            logger.info(f"Cleaned up temporary calculated file: {temp_calculated_path}")
        except Exception as e:
            logger.warning(f"Could not remove temporary calculated file: {e}")

        return csv_bytes

    except Exception as e:
        logger.error(f"Error generating CSV from Excel: {str(e)}")
        raise

@price_export_bp.route('/generate-price-export', methods=['POST'])
def generate_price_export():
    """
    Generate Excel price-export and CSV, then save both to Odoo x_configuration fields.

    This route:
    1. Fetches Excel template from x_configuration record ID 1, field x_studio_price_export_template
    2. Processes the template using the exact code from the Jupyter notebook
    3. Saves the generated Excel file to Odoo's x_configuration.x_studio_price_list_1 binary field
    4. Generates CSV from the Excel data using the same logic as the Jupyter notebook
    5. Saves the generated CSV file to Odoo's x_configuration.x_studio_price_list_1_csv binary field
    6. Returns success/failure status

    Simple POST request with empty body to trigger:
    POST /generate-price-export
    Content-Type: application/json
    {}
    """
    try:
        # Set up log capture
        log_handler, log_capture_string = create_log_capture_handler()
        logger.addHandler(log_handler)

        logger.info("Starting price-export generation route")

        # Get Odoo connection
        logger.info("Connecting to Odoo")
        uid = get_uid()
        models = get_odoo_models()
        logger.info("Connected to Odoo successfully")

        # Generate timestamp for filenames
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        # Generate the Excel file
        logger.info("Generating Excel price-export file")
        excel_bytes, total_products, total_pricelists, source_file = generate_price_export_excel(models, uid)


        # Generate CSV from the Excel data
        logger.info("Generating CSV from Excel data for x_studio_price_list_1_csv")
        csv_bytes = generate_csv_from_excel(excel_bytes)


        # Find the x_configuration record to update
        logger.info("Finding x_configuration record")
        config_ids = models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
            'x_configuration', 'search', [[]])

        if not config_ids:
            raise Exception("No x_configuration record found")

        config_id = config_ids[0]  # Use the first configuration record
        logger.info(f"Found configuration record ID: {config_id}")

        # Generate filename with timestamp
        filename = f"justframeit_price_export_generated_{timestamp}.xlsx"

        # Encode Excel bytes to base64 for Odoo binary field
        excel_base64 = base64.b64encode(excel_bytes).decode('ascii')

        # Encode CSV bytes to base64 for Odoo binary field
        csv_base64 = base64.b64encode(csv_bytes).decode('ascii')
        csv_filename = f"justframeit_price_export_generated_{timestamp}.csv"

        # Update the x_configuration record with the Excel file and CSV
        logger.info(f"Saving Excel file to x_studio_price_list_1 field and CSV to x_studio_price_list_1_csv")
        update_vals = {
            'x_studio_price_list_1': excel_base64,
            'x_studio_price_list_1_filename': filename,
            'x_studio_price_list_1_csv': csv_base64,
            'x_studio_price_list_1_csv_filename': csv_filename,
        }

        models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
            'x_configuration', 'write', [config_id, update_vals])

        logger.info("Excel file and CSV saved to Odoo successfully")

        # Post to chatter with summary, logs, and attachments
        logger.info("Posting to configuration chatter")

        # Create attachments for Excel, CSV files and logs
        attachment_ids = []

        # Create Excel attachment
        excel_attachment_data = {
            'name': filename,
            'type': 'binary',
            'datas': excel_base64,
            'res_model': 'x_configuration',
            'res_id': config_id,
            'mimetype': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        }
        excel_attachment_id = models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
            'ir.attachment', 'create', [excel_attachment_data])
        attachment_ids.append(excel_attachment_id)
        logger.info(f"Created Excel attachment with ID: {excel_attachment_id}")

        # Create CSV attachment
        csv_attachment_data = {
            'name': csv_filename,
            'type': 'binary',
            'datas': csv_base64,
            'res_model': 'x_configuration',
            'res_id': config_id,
            'mimetype': 'text/csv'
        }
        csv_attachment_id = models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
            'ir.attachment', 'create', [csv_attachment_data])
        attachment_ids.append(csv_attachment_id)
        logger.info(f"Created CSV attachment with ID: {csv_attachment_id}")

        # Get captured logs and create log attachment
        captured_logs = log_capture_string.getvalue()
        log_filename = f"price_export_logs_{timestamp}.txt"
        log_attachment_data = {
            'name': log_filename,
            'type': 'binary',
            'datas': base64.b64encode(captured_logs.encode('utf-8')).decode('ascii'),
            'res_model': 'x_configuration',
            'res_id': config_id,
            'mimetype': 'text/plain'
        }
        log_attachment_id = models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
            'ir.attachment', 'create', [log_attachment_data])
        attachment_ids.append(log_attachment_id)
        logger.info(f"Created log attachment with ID: {log_attachment_id}")

        # Prepare response
        response_data = {
            'message': 'Price-export Excel and CSV generated, saved locally, and saved to Odoo successfully',
            'config_id': config_id,
            'filename': filename,
            'csv_filename': csv_filename,
            'local_filename': local_filename,
            'local_csv_filename': local_csv_filename,
            'products_processed': total_products,
            'pricelists_processed': total_pricelists,
            'source_file': source_file,
            'status': 'success'
        }

        logger.info(f"Price-export generation completed - Config ID: {config_id}, Products: {total_products}, Pricelists: {total_pricelists}")

        # Get captured logs and clean up handler
        log_contents = log_capture_string.getvalue()
        logger.removeHandler(log_handler)
        log_capture_string.close()

        # Log the route call to Odoo logging model
        request_data = {}  # Empty payload since this route takes no input
        log_route_call(models, uid, '/generate-price-export', request_data, log_contents, response_data)

        # Create simple chatter message with response data
        response_formatted = json.dumps(response_data, indent=2, ensure_ascii=False)
        chatter_message = f"✅ Price-export generation completed\n\n📤 Response:\n```\n{response_formatted}\n```"

        # Post message to chatter
        chatter_data = {
            'body': chatter_message,
            'message_type': 'comment',
            'attachment_ids': attachment_ids,  # List of attachment IDs directly
        }

        models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
            'x_configuration', 'message_post', [config_id], chatter_data)

        logger.info("Successfully posted to configuration chatter")

        return jsonify(response_data)

    except Exception as e:
        # Clean up log handler and get logs if available
        log_contents = ""
        try:
            if 'log_handler' in locals() and 'log_capture_string' in locals():
                log_contents = log_capture_string.getvalue()
                logger.removeHandler(log_handler)
                log_capture_string.close()
        except:
            pass

        logger.error(f"Error in generate-price-export route: {str(e)}")

        # Prepare error response data
        error_response = {'error': str(e), 'status': 'error'}

        # Log the error to Odoo logging model
        request_data = {}  # Empty payload since this route takes no input
        log_route_call(None, None, '/generate-price-export', request_data, log_contents, error_response)

        return jsonify(error_response), 500
