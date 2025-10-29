from flask import Blueprint, jsonify, request
import xmlrpc.client
import os
from dotenv import load_dotenv
import logging
from datetime import datetime

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

@justframeit_bp.route('/handle-web-order', methods=['POST'])
def handle_web_order():
    """
    Handle web order by creating product, BOM and sale order in Odoo

    Expected payload structure:
    {
        "product": {
            "width": 400,
            "height": 400,
            "price": 200.00,
            "components": [
                {
                    "name": "Frame A",
                    "reference": "TEST_FRAME_A"
                },
                {
                    "name": "Glass A",
                    "reference": "TEST_GLASS_A"
                },
                {
                    "name": "Passe-Partout A",
                    "reference": "TEST_PP_A"
                }
            ]
        }
    }

    Note: 'name' and 'reference' fields are optional and will be auto-generated with timestamps if not provided.
    """
    try:
        # Get payload from request
        data = request.get_json()
        if not data or 'product' not in data:
            return jsonify({'error': 'Missing product data in request'}), 400

        payload = data

        # Generate timestamp for unique naming if needed
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        # Auto-generate product name and reference with timestamp if not provided or placeholder
        product_name = payload['product'].get('name', '').strip()
        product_reference = payload['product'].get('reference', '').strip()

        # If name is empty or contains placeholder, generate with timestamp
        if not product_name or product_name.lower() in ['', 'auto', 'generate']:
            product_name = f"Finished Product {timestamp}"

        # If reference is empty or contains placeholder, generate with timestamp
        if not product_reference or product_reference.lower() in ['', 'auto', 'generate']:
            product_reference = f"FINISHED_PRODUCT_{timestamp}"

        # Get Odoo connection using existing helper functions
        uid = get_uid()
        models = get_odoo_models()

        # Create product
        product_vals = {
            'name': product_name,
            'type': 'consu',  # Changed from 'product' to 'consu' as it's a valid selection value
            'x_studio_width': payload['product']['width'],
            'x_studio_height': payload['product']['height'],
            'list_price': payload['product']['price'],
            'default_code': product_reference,
            'categ_id': 8,  # Set category ID to 8
        }

        product_id = models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
            'product.product', 'create', [product_vals])

        # Create components and BOM
        bom_components = []
        bom_operations = []
        width = payload['product']['width']
        height = payload['product']['height']
        surface = width * height
        circumference = 2 * (width + height)

        print(f"Surface: {surface} mm²")
        print(f"Surface: {surface/1000000} m²")
        print(f"Circumference: {circumference} mm")

        # Convert dimensions to meters for duration rules
        surface_m2 = surface / 1000000  # Convert mm² to m²
        circumference_m = circumference / 1000  # Convert mm to m

        for component in payload['product']['components']:
            # Search for existing component
            component_ids = models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
                'product.product', 'search',
                [[['default_code', '=', component['reference']]]])

            if not component_ids:
                # Create component if it doesn't exist
                component_vals = {
                    'name': component['name'],
                    'type': 'product',
                    'default_code': component['reference'],
                    'categ_id': 8,
                }
                component_id = models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
                    'product.product', 'create', [component_vals])
            else:
                component_id = component_ids[0]

            # Get component details to check price computation method and associated service
            component_info = models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
                'product.product', 'read',
                [component_id],
                {'fields': ['x_studio_price_computation', 'x_studio_associated_service', 'x_studio_associated_service_duration_rule']})

            # Calculate quantity based on price computation method
            if component_info and component_info[0]['x_studio_price_computation'] == 'Circumference':
                quantity = circumference_m
            elif component_info and component_info[0]['x_studio_price_computation'] == 'Surface':
                quantity = surface_m2
            else:
                quantity = 1

            # Add to BOM components
            bom_components.append((0, 0, {
                'product_id': component_id,
                'product_qty': quantity,
            }))

            # Handle associated service/operation
            if component_info[0]['x_studio_associated_service']:
                service_id = component_info[0]['x_studio_associated_service'][0]

                # Get service details
                service_info = models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
                    'x_services', 'read',
                    [service_id],
                    {'fields': ['x_name', 'x_soort', 'x_studio_associated_work_center']})

                # Get duration rules directly from component
                duration_rules = models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
                    'x_services_duration_rules', 'read',
                    [component_info[0]['x_studio_associated_service_duration_rule']],
                    {'fields': ['x_omtrek', 'x_oppervlakte', 'x_duurtijd_totaal']})

                # Find appropriate duration based on x_soort
                if service_info[0]['x_soort'] == 'Oppervlakte':
                    relevant_value = surface_m2  # Use m² value
                    rules_sorted = sorted(duration_rules, key=lambda x: x['x_oppervlakte'])
                    matching_rule = next((rule for rule in rules_sorted if rule['x_oppervlakte'] >= relevant_value), rules_sorted[-1])
                    duration_seconds = matching_rule['x_duurtijd_totaal']
                else:  # 'Omtrek'
                    relevant_value = circumference_m  # Use m value
                    rules_sorted = sorted(duration_rules, key=lambda x: x['x_omtrek'])
                    matching_rule = next((rule for rule in rules_sorted if rule['x_omtrek'] >= relevant_value), rules_sorted[-1])
                    duration_seconds = matching_rule['x_duurtijd_totaal']

                # Convert duration from seconds to minutes and calculate MM:SS format
                duration_minutes = duration_seconds / 60
                minutes = int(duration_minutes)
                seconds = int((duration_minutes - minutes) * 60)
                odoo_display = f"{minutes:02d}:{seconds:02d}"
                print(f"Converting duration for {service_info[0]['x_name']}: {duration_seconds} seconds = {duration_minutes} minutes (will display as {odoo_display} in Odoo)")

                # Add operation to BOM
                bom_operations.append((0, 0, {
                    'name': service_info[0]['x_name'],
                    'time_cycle_manual': duration_minutes,
                    'workcenter_id': service_info[0]['x_studio_associated_work_center'][0] if service_info[0]['x_studio_associated_work_center'] else False
                }))

        # Get the product template ID from the created product
        product_data = models.execute_kw(
            ODOO_DB, uid, ODOO_API_KEY,
            'product.product', 'read', [product_id], {'fields': ['product_tmpl_id']}
        )
        product_tmpl_id = product_data[0]['product_tmpl_id'][0]

        # Create Bill of Materials using the template ID
        bom_vals = {
            'product_tmpl_id': product_tmpl_id,
            'product_qty': 1,
            'type': 'normal',
            'bom_line_ids': bom_components,
            'operation_ids': bom_operations,
        }

        bom_id = models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
            'mrp.bom', 'create', [bom_vals])

        # Create sale order
        order_vals = {
            'partner_id': 1,  # Default customer ID, adjust as needed
            'order_line': [(0, 0, {
                'product_id': product_id,
                'product_uom_qty': 1,
                'price_unit': payload['product']['price']
            })]
        }

        order_id = models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
            'sale.order', 'create', [order_vals])

        print(f"Created product ID: {product_id}")
        print(f"Created BOM ID: {bom_id}")
        print(f"Created sale order ID: {order_id}")
        print("Sale order from web simulation finished")

        return jsonify({
            'message': 'Sale order from web simulation finished',
            'product_id': product_id,
            'bom_id': bom_id,
            'order_id': order_id,
            'status': 'success'
        })

    except Exception as e:
        logger.error(f"Error handling web order: {str(e)}")
        return jsonify({'error': str(e), 'status': 'error'}), 500