from flask import Blueprint, jsonify
import xmlrpc.client
import os
from dotenv import load_dotenv
import logging

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Create blueprint
justframeit_bp = Blueprint('justframeit', __name__)

# Odoo Configuration
ODOO_URL = os.getenv('JUSTFRAMEIT_ODOO_URL')
ODOO_DB = os.getenv('JUSTFRAMEIT_ODOO_DB')
ODOO_USERNAME = os.getenv('JUSTFRAMEIT_ODOO_USERNAME')
ODOO_API_KEY = os.getenv('JUSTFRAMEIT_ODOO_API_KEY')

# JWT Configuration
JWT_SECRET_KEY = os.getenv('JWT_SECRET_KEY', 'your-secret-key')  # Change in production
JWT_EXPIRATION_HOURS = 24


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

@justframeit_bp.route('/justframeit-api/test', methods=['GET'])
def dummy_route():
    """Dummy route for testing purposes"""
    return jsonify({'message': 'This is a dummy route', 'status': 'success'})