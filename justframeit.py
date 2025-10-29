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

def create_product_and_bom(models, uid, product_name, product_reference, width, height, price, components):
    """
    Shared function to create product and BOM with components and operations.

    Args:
        models: Odoo models proxy
        uid: User ID
        product_name: Name for the product
        product_reference: Reference/Default code for the product
        width: Product width in mm
        height: Product height in mm
        price: Product price
        components: List of component dictionaries with 'name' and 'reference' keys

    Returns:
        tuple: (product_id, bom_id)
    """
    # Create product
    product_vals = {
        'name': product_name,
        'type': 'consu',
        'x_studio_width': width,
        'x_studio_height': height,
        'list_price': price,
        'default_code': product_reference,
        'categ_id': 8,  # Set category ID to 8
    }

    product_id = models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
        'product.product', 'create', [product_vals])

    # Create components and BOM
    bom_components = []
    bom_operations = []
    surface = width * height
    circumference = 2 * (width + height)

    print(f"Surface: {surface} mm²")
    print(f"Surface: {surface/1000000} m²")
    print(f"Circumference: {circumference} mm")

    # Convert dimensions to meters for duration rules
    surface_m2 = surface / 1000000  # Convert mm² to m²
    circumference_m = circumference / 1000  # Convert mm to m

    for component in components:
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

    return product_id, bom_id

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

        # Use shared function to create product and BOM
        product_id, bom_id = create_product_and_bom(
            models, uid,
            product_name, product_reference,
            payload['product']['width'], payload['product']['height'],
            payload['product']['price'], payload['product']['components']
        )

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

@justframeit_bp.route('/handle-odoo-order', methods=['POST'])
def handle_odoo_order():
    """
    Handle existing Odoo sale order by copying product specs and creating new product with BOM

    Expected payload structure:
    {
        "sale_order_id": 48
    }

    This will:
    1. Read the existing sale order and its product details
    2. Copy components from the existing product's BOM
    3. Create a new product with the same specifications
    4. Update the existing sale order with the new product
    """
    try:
        # Get payload from request
        data = request.get_json()
        if not data or 'sale_order_id' not in data:
            return jsonify({'error': 'Missing sale_order_id in request'}), 400

        sale_order_id = data['sale_order_id']

        # Get Odoo connection using existing helper functions
        uid = get_uid()
        models = get_odoo_models()

        # Get sale order details
        sale_order = models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
            'sale.order', 'read',
            [sale_order_id],
            {'fields': ['order_line']})

        if not sale_order or not sale_order[0]['order_line']:
            return jsonify({'error': 'Sale order not found or has no order lines'}), 404

        # Get first product from sale order
        order_line = models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
            'sale.order.line', 'read',
            [sale_order[0]['order_line'][0]],
            {'fields': ['product_id', 'price_unit']})

        product_info = models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
            'product.product', 'read',
            [order_line[0]['product_id'][0]],
            {'fields': ['x_studio_width', 'x_studio_height', 'name', 'default_code', 'product_tmpl_id']})

        # Extract product details
        width = product_info[0]['x_studio_width']
        height = product_info[0]['x_studio_height']
        price = order_line[0]['price_unit']

        # Get BOM for the existing product
        bom_ids = models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
            'mrp.bom', 'search',
            [[['product_tmpl_id', '=', product_info[0]['product_tmpl_id'][0]]]])

        if not bom_ids:
            return jsonify({'error': 'No BOM found for the existing product'}), 404

        bom_info = models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
            'mrp.bom', 'read',
            [bom_ids[0]],
            {'fields': ['bom_line_ids']})

        # Get components from existing BOM
        components = []
        for bom_line_id in bom_info[0]['bom_line_ids']:
            bom_line = models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
                'mrp.bom.line', 'read',
                [bom_line_id],
                {'fields': ['product_id']})
            component_info = models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
                'product.product', 'read',
                [bom_line[0]['product_id'][0]],
                {'fields': ['name', 'default_code']})
            components.append({
                'name': component_info[0]['name'],
                'reference': component_info[0]['default_code']
            })

        # Generate timestamp for unique naming
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        # Create new product name and reference with timestamp
        product_name = f"Finished Product {timestamp}"
        product_reference = f"FINISHED_PRODUCT_{timestamp}"

        # Use shared function to create product and BOM
        product_id, bom_id = create_product_and_bom(
            models, uid,
            product_name, product_reference,
            width, height, price, components
        )

        # Compute BOM cost for the newly created product
        try:
            # Get the product template ID from the created product
            product_data = models.execute_kw(
                ODOO_DB, uid, ODOO_API_KEY,
                'product.product', 'read', [product_id], {'fields': ['product_tmpl_id']}
            )
            product_tmpl_id = product_data[0]['product_tmpl_id'][0]

            # Get initial cost before computing BOM
            initial_cost = models.execute_kw(
                ODOO_DB, uid, ODOO_API_KEY,
                'product.template', 'read',
                [[product_tmpl_id]],
                {'fields': ['standard_price']}
            )[0]['standard_price']

            print(f"Initial cost: {initial_cost}")

            # Compute BOM cost - ignore return value from button_bom_cost
            models.execute_kw(
                ODOO_DB, uid, ODOO_API_KEY,
                'product.template', 'button_bom_cost',
                [[product_tmpl_id]]
            )

            # Get new cost after computation
            new_cost = models.execute_kw(
                ODOO_DB, uid, ODOO_API_KEY,
                'product.template', 'read',
                [[product_tmpl_id]],
                {'fields': ['standard_price']}
            )[0]['standard_price']

            print(f"New cost after BOM computation: {new_cost}")

            if new_cost != initial_cost:
                print("✅ Cost change detected - BOM computation successful")
            else:
                print("⚠️ Warning: No cost change detected after BOM computation")

        except Exception as e:
            print(f"⚠️ Warning: Failed to compute BOM cost: {str(e)}")
            # Continue with order processing even if BOM cost computation fails

        # Update existing sale order with new product
        models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
            'sale.order', 'write',
            [sale_order_id, {
                'order_line': [(1, sale_order[0]['order_line'][0], {
                    'product_id': product_id,
                    'product_uom_qty': 1,
                    'price_unit': price
                })]
            }])

        print(f"Created product ID: {product_id}")
        print(f"Created BOM ID: {bom_id}")
        print(f"Updated sale order ID: {sale_order_id}")
        print("Odoo order processing finished")

        return jsonify({
            'message': 'Odoo order processing finished',
            'product_id': product_id,
            'bom_id': bom_id,
            'updated_order_id': sale_order_id,
            'status': 'success'
        })

    except Exception as e:
        logger.error(f"Error handling Odoo order: {str(e)}")
        return jsonify({'error': str(e), 'status': 'error'}), 500