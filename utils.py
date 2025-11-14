import os
import json
import xmlrpc.client
import logging
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Odoo Configuration for logging
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

def log_route_call(models, uid, route_name, payload, server_logs, response_data):
    """
    Log route calls to Odoo ir.logging model.

    Args:
        models: Odoo models proxy (can be None - function will establish connection if needed)
        uid: User ID (can be None - function will authenticate if needed)
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
        if route_name == '/handle-web-order':
            log_name = 'Web Order Processing'
        elif route_name == '/handle-odoo-order':
            log_name = 'Odoo Order Processing'
        elif route_name == '/generate-price-export':
            log_name = 'Price Export Generation'
        else:
            log_name = 'API Operation'

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
