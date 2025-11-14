from flask import Blueprint, jsonify, request
import xmlrpc.client
import os
import json
import base64
from io import StringIO
from dotenv import load_dotenv
import logging
from datetime import datetime
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

def get_or_create_customer(models, uid, customer_data):
    """
    Get existing customer by email or create new one if not found.

    Args:
        models: Odoo models proxy
        uid: User ID
        customer_data: Dictionary containing customer information

    Returns:
        tuple: (partner_id, customer_action) where customer_action is 'found' or 'created'
    """
    logger.info(f"Searching for customer with email: {customer_data['email']}")

    # Search for existing customer by email
    partner_ids = models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
        'res.partner', 'search',
        [[['email', '=', customer_data['email']]]])

    if partner_ids:
        # Customer exists, return existing ID
        partner_id = partner_ids[0]
        logger.info(f"Found existing customer '{customer_data['name']}' with email {customer_data['email']} (ID: {partner_id})")
        return partner_id, 'found'

    # Customer doesn't exist, create new one
    logger.info(f"Creating new customer: {customer_data['name']}")
    partner_vals = {
        'name': customer_data['name'],
        'email': customer_data['email'],
        'customer_rank': 1,  # Mark as customer
    }

    # Add optional fields if provided
    if customer_data.get('phone'):
        partner_vals['phone'] = customer_data['phone']
        logger.debug(f"Added phone: {customer_data['phone']}")
    if customer_data.get('street'):
        partner_vals['street'] = customer_data['street']
        logger.debug(f"Added street: {customer_data['street']}")
    if customer_data.get('city'):
        partner_vals['city'] = customer_data['city']
        logger.debug(f"Added city: {customer_data['city']}")
    if customer_data.get('zip'):
        partner_vals['zip'] = customer_data['zip']
        logger.debug(f"Added zip: {customer_data['zip']}")
    if customer_data.get('country'):
        # Search for country by name
        logger.debug(f"Searching for country: {customer_data['country']}")
        country_ids = models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
            'res.country', 'search',
            [[['name', 'ilike', customer_data['country']]]])
        if country_ids:
            partner_vals['country_id'] = country_ids[0]
            logger.debug(f"Found country ID: {country_ids[0]}")

    partner_id = models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
        'res.partner', 'create', [partner_vals])

    logger.info(f"Created new customer '{customer_data['name']}' with email {customer_data['email']} (ID: {partner_id})")
    return partner_id, 'created'

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
        tuple: (product_id, bom_id, bom_components_count, bom_operations_count)
    """
    logger.info("Starting product and BOM creation process")
    logger.info(f"Product: {product_name} (ref: {product_reference})")
    logger.info(f"Dimensions: {width}mm x {height}mm, Price: €{price}")
    logger.info(f"Components: {len(components)} items")

    # Create product
    logger.info("Creating product in Odoo")
    product_vals = {
        'name': product_name,
        'type': 'consu',
        'x_studio_width': width,
        'x_studio_height': height,
        'list_price': price,
        'default_code': product_reference,
        'categ_id': 8,  # Set category ID to 8
    }

    logger.debug(f"Product creation values: {product_vals}")
    product_id = models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
        'product.product', 'create', [product_vals])
    logger.info(f"Product created with ID: {product_id}")

    # Create components and BOM
    logger.info("Setting up BOM creation")
    bom_components = []
    bom_operations = []
    surface = width * height
    circumference = 2 * (width + height)

    logger.info(f"Surface: {surface} mm² ({surface/1000000:.4f} m²)")
    logger.info(f"Circumference: {circumference} mm ({circumference/1000:.2f} m)")

    # Convert dimensions to meters for duration rules
    surface_m2 = surface / 1000000  # Convert mm² to m²
    circumference_m = circumference / 1000  # Convert mm to m
    logger.debug(f"Converted dimensions - Surface: {surface_m2} m², Circumference: {circumference_m} m")

    logger.info("Processing components for BOM")
    for i, component in enumerate(components, 1):
        logger.info(f"Processing component {i}/{len(components)}: {component['name']} (ref: {component['reference']})")

        # Search for existing component
        logger.debug(f"Searching for component with x_studio_product_code: {component['reference']}")
        component_ids = models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
            'product.product', 'search',
            [[['x_studio_product_code', '=', component['reference']]]])

        if not component_ids:
            logger.error(f"Component with reference '{component['reference']}' not found in Odoo")
            raise ValueError(f"Component with reference '{component['reference']}' not found in Odoo")

        component_id = component_ids[0]
        logger.info(f"Found component ID: {component_id}")

        # Get component details to check price computation method and associated service
        logger.debug(f"Reading component details for ID: {component_id}")
        component_info = models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
            'product.product', 'read',
            [component_id],
            {'fields': ['x_studio_price_computation', 'x_studio_associated_service', 'x_studio_associated_service_duration_rule']})

        # Calculate quantity based on price computation method
        if component_info and component_info[0]['x_studio_price_computation'] == 'Circumference':
            quantity = circumference_m
            logger.debug(f"Quantity calculation: Circumference method → {quantity} m")
        elif component_info and component_info[0]['x_studio_price_computation'] == 'Surface':
            quantity = surface_m2
            logger.debug(f"Quantity calculation: Surface method → {quantity} m²")
        else:
            quantity = 1
            logger.debug("Quantity calculation: Default method → 1 unit")

        logger.info(f"Component quantity: {quantity}")

        # Add to BOM components
        logger.debug("Adding component to BOM")
        bom_components.append((0, 0, {
            'product_id': component_id,
            'product_qty': quantity,
        }))
        logger.info("Added component to BOM")

        # Handle associated service/operation
        if component_info[0]['x_studio_associated_service']:
            service_id = component_info[0]['x_studio_associated_service'][0]
            logger.info(f"Processing associated service ID: {service_id}")

            # Get service details
            logger.debug(f"Reading service details for ID: {service_id}")
            service_info = models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
                'x_services', 'read',
                [service_id],
                {'fields': ['x_name', 'x_studio_associated_work_center']})

            logger.debug(f"Service info: {service_info[0]['x_name']}")

            # Get duration rules directly from component
            logger.debug("Reading duration rules")
            duration_rules = models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
                'x_services_duration_rules', 'read',
                [component_info[0]['x_studio_associated_service_duration_rule']],
                {'fields': ['x_studio_quantity', 'x_duurtijd_totaal']})

            # Find appropriate duration based on x_studio_quantity
            relevant_value = quantity  # Use the calculated quantity (surface, circumference, or default)
            rules_sorted = sorted(duration_rules, key=lambda x: x['x_studio_quantity'])
            matching_rule = next((rule for rule in rules_sorted if rule['x_studio_quantity'] >= relevant_value), rules_sorted[-1])
            duration_seconds = matching_rule['x_duurtijd_totaal']
            logger.debug(f"Duration calculation: Quantity-based ({relevant_value}) → {duration_seconds} seconds")

            # Convert duration from seconds to minutes and calculate MM:SS format
            duration_minutes = duration_seconds / 60
            minutes = int(duration_minutes)
            seconds = int((duration_minutes - minutes) * 60)
            odoo_display = f"{minutes:02d}:{seconds:02d}"
            logger.info(f"Duration for {service_info[0]['x_name']}: {duration_seconds}s = {duration_minutes:.2f}min ({odoo_display})")

            # Add operation to BOM
            logger.debug("Adding operation to BOM")
            bom_operations.append((0, 0, {
                'name': service_info[0]['x_name'],
                'time_cycle_manual': duration_minutes,
                'workcenter_id': service_info[0]['x_studio_associated_work_center'][0] if service_info[0]['x_studio_associated_work_center'] else False
            }))
            logger.info("Added operation to BOM")

    # Get the product template ID from the created product
    logger.info("Getting product template ID")
    product_data = models.execute_kw(
        ODOO_DB, uid, ODOO_API_KEY,
        'product.product', 'read', [product_id], {'fields': ['product_tmpl_id']}
    )
    product_tmpl_id = product_data[0]['product_tmpl_id'][0]
    logger.debug(f"Product template ID: {product_tmpl_id}")

    # Create Bill of Materials using the template ID
    logger.info("Creating Bill of Materials")
    logger.info(f"BOM components: {len(bom_components)}")
    logger.info(f"BOM operations: {len(bom_operations)}")

    bom_vals = {
        'product_tmpl_id': product_tmpl_id,
        'product_qty': 1,
        'type': 'normal',
        'bom_line_ids': bom_components,
        'operation_ids': bom_operations,
    }

    logger.debug(f"BOM creation values: {len(bom_components)} components, {len(bom_operations)} operations")
    bom_id = models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
        'mrp.bom', 'create', [bom_vals])

    logger.info(f"BOM created with ID: {bom_id}")
    logger.info("Product and BOM creation completed successfully")

    return product_id, bom_id, len(bom_components), len(bom_operations)

def interpret_craft_payload(craft_payload):
    """
    Convert a complex Craft CMS payload to the simple payload format used by handle_web_order.

    Args:
        craft_payload (dict): The complex payload from Craft CMS

    Returns:
        dict: Simplified payload with customer and product information
    """
    try:
        logger.info("Starting Craft CMS payload interpretation")
        logger.debug(f"Received payload keys: {list(craft_payload.keys())}")
        # Extract customer information
        logger.info("Extracting customer information")
        customer_data = craft_payload.get('customer', {})
        shipping_address = craft_payload.get('shippingAddress', {})
        billing_address = craft_payload.get('billingAddress', {})

        logger.debug(f"Customer data keys: {list(customer_data.keys()) if customer_data else 'None'}")
        logger.debug(f"Shipping address available: {bool(shipping_address)}")
        logger.debug(f"Billing address available: {bool(billing_address)}")

        # Use shipping address as primary, fallback to billing address
        address = shipping_address or billing_address
        address_type = "shipping" if shipping_address else "billing"
        logger.info(f"Using {address_type} address for customer details")

        customer = {
            'name': customer_data.get('fullName', ''),
            'email': customer_data.get('email', ''),
            'phone': customer_data.get('userPhone') or address.get('phone', ''),
            'street': f"{address.get('addressLine1', '')} {address.get('addressLine2', '')}".strip(),
            'city': address.get('locality', ''),
            'zip': address.get('postalCode', ''),
            'country': 'Belgium' if address.get('countryCode') == 'BE' else address.get('countryCode', '')
        }
        logger.info(f"Customer extracted: {customer['name']} ({customer['email']})")

        # Extract product information from first line item
        logger.info("Extracting product information")
        line_items = craft_payload.get('lineItems', [])
        logger.debug(f"Found {len(line_items)} line items")

        if not line_items:
            logger.error("No line items found in the order")
            raise ValueError("No line items found in the order")

        line_item = line_items[0]  # Assume first item is the main product
        logger.info("Processing first line item as main product")
        options = line_item.get('options', {})
        configuration = options.get('configuration', {})

        logger.debug(f"Configuration keys: {list(configuration.keys()) if configuration else 'None'}")

        # Extract dimensions (convert from cm to mm)
        width_cm = configuration.get('width', 0)
        height_cm = configuration.get('height', 0)
        price = line_item.get('total', 0)

        # Convert cm to mm
        width = width_cm * 10
        height = height_cm * 10

        logger.info(f"Dimensions: {width}mm x {height}mm (converted from {width_cm}cm x {height_cm}cm), Price: €{price}")

        # Extract components from SKUs
        logger.info("Extracting components from SKUs")
        components = []

        # Helper function to extract product code (part before dot)
        def extract_product_code(sku):
            if sku and '.' in sku:
                original = sku
                extracted = sku.split('.')[0]
                logger.debug(f"Extracted product code: {original} → {extracted}")
                return extracted
            return sku

        # Add list/frame component
        list_sku = configuration.get('listSku')
        if list_sku:
            processed_sku = extract_product_code(list_sku)
            components.append({
                'name': 'Frame',
                'reference': processed_sku
            })
            logger.info(f"Added Frame component: {processed_sku}")

        # Add glass component
        glass_sku = configuration.get('glassSku')
        if glass_sku:
            processed_sku = extract_product_code(glass_sku)
            components.append({
                'name': 'Glass',
                'reference': processed_sku
            })
            logger.info(f"Added Glass component: {processed_sku}")

        # Add passe-partout components (can be multiple)
        passe_partout_skus = configuration.get('passePartoutSku', [])
        if isinstance(passe_partout_skus, list):
            logger.debug(f"Found {len(passe_partout_skus)} passe-partout SKUs")
            for sku in passe_partout_skus:
                if sku:
                    processed_sku = extract_product_code(sku)
                    components.append({
                        'name': 'Passe-Partout',
                        'reference': processed_sku
                    })
                    logger.info(f"Added Passe-Partout component: {processed_sku}")
        elif passe_partout_skus:  # Single SKU
            processed_sku = extract_product_code(passe_partout_skus)
            components.append({
                'name': 'Passe-Partout',
                'reference': processed_sku
            })
            logger.info(f"Added Passe-Partout component: {processed_sku}")

        logger.info(f"Total components extracted: {len(components)}")


        product = {
            'width': width,
            'height': height,
            'price': price,
            'components': components
        }

        logger.info("Creating simplified payload structure")
        payload = {
            'customer': customer,
            'product': product
        }

        logger.info("Craft CMS payload interpretation completed successfully")
        logger.debug(f"Final payload structure: customer={bool(customer)}, product={bool(product)}")

        # Return simplified payload
        return payload

    except Exception as e:
        logger.error(f"Error interpreting Craft payload: {str(e)}")
        raise ValueError(f"Failed to interpret Craft payload: {str(e)}")


@justframeit_bp.route('/justframeit-api/test', methods=['GET'])
def dummy_route():
    """Dummy route for testing purposes"""
    return jsonify({'message': 'This is a dummy route', 'status': 'success'})

@justframeit_bp.route('/handle-web-order', methods=['POST'])
def handle_web_order():
    """
    Handle web order by creating product, BOM and sale order in Odoo

    Accepts both simple payload format and complex Craft CMS payload format.
    If Craft payload is detected, it will be automatically converted to simple format.

    Simple payload structure:
    {
        "customer": {
            "name": "John Doe",
            "email": "john.doe@example.com",
            "phone": "+1234567890",
            "street": "123 Main St",
            "city": "New York",
            "zip": "10001",
            "country": "United States"
        },
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

    Craft payload: Complex structure like order-21871ab.json will be automatically detected and converted.

    Note: 'name' and 'reference' fields are optional and will be auto-generated with timestamps if not provided.
    """
    try:
        # Set up log capture
        log_capture_string = StringIO()
        log_handler = logging.StreamHandler(log_capture_string)
        log_handler.setLevel(logging.DEBUG)
        log_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        logger.addHandler(log_handler)

        logger.info("Starting web order processing")

        # Get payload from request
        logger.info("Receiving payload from request")
        data = request.get_json()
        if not data:
            logger.error("No payload data in request")
            return jsonify({'error': 'Missing payload data in request'}), 400

        # Check if this is a Craft CMS payload by looking for specific fields
        logger.info("Detecting payload type")
        is_craft_payload = (
            'number' in data and
            'reference' in data and
            'customer' in data and
            'lineItems' in data and
            'shippingAddress' in data
        )

        if is_craft_payload:
            # Convert Craft payload to simple format
            logger.info("Detected Craft CMS payload, converting to simple format")
            payload = interpret_craft_payload(data)
        else:
            # Assume it's already in simple format
            logger.info("Detected simple payload format")
            if 'product' not in data or 'customer' not in data:
                logger.error("Simple payload missing required fields")
                return jsonify({'error': 'Missing product or customer data in request'}), 400
            payload = data

        # Log the payload type for debugging
        payload_type = "Craft CMS" if is_craft_payload else "Simple"
        logger.info(f"Processing {payload_type} payload successfully")

        # Generate timestamp for unique naming if needed
        logger.info("Generating timestamp for unique naming")
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        # Auto-generate product name and reference with timestamp if not provided or placeholder
        product_name = payload['product'].get('name', '').strip()
        product_reference = payload['product'].get('reference', '').strip()

        # If name is empty or contains placeholder, generate with timestamp
        if not product_name or product_name.lower() in ['', 'auto', 'generate']:
            product_name = f"Finished Product {timestamp}"
            logger.info(f"Auto-generated product name: {product_name}")

        # If reference is empty or contains placeholder, generate with timestamp
        if not product_reference or product_reference.lower() in ['', 'auto', 'generate']:
            product_reference = f"FINISHED_PRODUCT_{timestamp}"
            logger.info(f"Auto-generated product reference: {product_reference}")

        # Get Odoo connection using existing helper functions
        logger.info("Connecting to Odoo")
        uid = get_uid()
        models = get_odoo_models()
        logger.info("Connected to Odoo successfully")

        # Get or create customer
        logger.info("Processing customer information")
        partner_id, customer_action = get_or_create_customer(models, uid, payload['customer'])

        # Use shared function to create product and BOM
        logger.info("Creating product and BOM")
        product_id, bom_id, bom_components_count, bom_operations_count = create_product_and_bom(
            models, uid,
            product_name, product_reference,
            payload['product']['width'], payload['product']['height'],
            payload['product']['price'], payload['product']['components']
        )

        # Create sale order
        logger.info("Creating sale order")
        order_vals = {
            'partner_id': partner_id,
            'order_line': [(0, 0, {
                'product_id': product_id,
                'product_uom_qty': 1,
                'price_unit': payload['product']['price']
            })]
        }

        logger.debug(f"Sale order values: partner_id={partner_id}, product_id={product_id}")
        order_id = models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
            'sale.order', 'create', [order_vals])

        logger.info("Web order processing completed successfully")
        logger.info(f"Results - Customer ID: {partner_id}, Product ID: {product_id}, BOM ID: {bom_id}, Order ID: {order_id}")

        # Save original payload as attachment
        attachment_ids = []
        try:
            payload_json = json.dumps(data, indent=2, ensure_ascii=False)
            attachment_name = f"order_payload_{timestamp}.json"

            attachment_vals = {
                'name': attachment_name,
                'type': 'binary',
                'datas': base64.b64encode(payload_json.encode('utf-8')).decode('ascii'),  # Base64 encode the UTF-8 bytes
                'res_model': 'sale.order',
                'res_id': order_id,
                'mimetype': 'application/json'
            }

            attachment_id = models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
                'ir.attachment', 'create', [attachment_vals])

            attachment_ids = [attachment_id]
            logger.info(f"Successfully created payload attachment: {attachment_name} (ID: {attachment_id})")
        except Exception as e:
            logger.error(f"Failed to create payload attachment: {str(e)}")

        # Save processing logs as text attachment
        try:
            # Get the captured logs and remove the handler
            log_contents = log_capture_string.getvalue()
            logger.removeHandler(log_handler)
            log_capture_string.close()

            log_attachment_name = f"order_processing_logs_{timestamp}.txt"

            log_attachment_vals = {
                'name': log_attachment_name,
                'type': 'binary',
                'datas': base64.b64encode(log_contents.encode('utf-8')).decode('ascii'),  # Base64 encode the UTF-8 bytes
                'res_model': 'sale.order',
                'res_id': order_id,
                'mimetype': 'text/plain'
            }

            log_attachment_id = models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
                'ir.attachment', 'create', [log_attachment_vals])

            attachment_ids.append(log_attachment_id)
            logger.info(f"Successfully created log attachment: {log_attachment_name} (ID: {log_attachment_id})")
        except Exception as e:
            logger.error(f"Failed to create log attachment: {str(e)}")
            # Clean up the handler even if attachment creation fails
            try:
                logger.removeHandler(log_handler)
                log_capture_string.close()
            except:
                pass

        # Create comprehensive HTML chatter message with all logs and final return
        component_list_items = []
        for i, component in enumerate(payload['product']['components'], 1):
            component_list_items.append(f"<li>Component {i}: {component['name']} (ref: {component['reference']})</li>")

        chatter_message = f"""<p><strong>🎯 {payload_type} Order Processing Completed Successfully</strong></p>

<p><em>📎 Attachments: order_payload_{timestamp}.json, order_processing_logs_{timestamp}.txt</em></p>

<p><strong>📋 Order Summary:</strong></p>
<ul>
<li>Customer ID: {partner_id}</li>
<li>Product ID: {product_id}</li>
<li>BOM ID: {bom_id}</li>
<li>Order ID: {order_id}</li>
<li>Payload Type: {payload_type}</li>
<li>Status: success</li>
</ul>

<p><strong>📝 Processing Details:</strong></p>

<p><strong>Payload Processing:</strong></p>
<ul>
<li>Detected {payload_type} payload format</li>
<li>Payload processed successfully</li>
</ul>

<p><strong>Customer Processing:</strong></p>
<ul>
<li>Searched for customer with email: {payload['customer']['email']}</li>
<li>{customer_action.title()} customer: {payload['customer']['name']} ({payload['customer']['email']})</li>
</ul>

<p><strong>Product Creation:</strong></p>
<ul>
<li>Product: {product_name} (ref: {product_reference})</li>
<li>Dimensions: {payload['product']['width']}mm x {payload['product']['height']}mm</li>
<li>Price: €{payload['product']['price']}</li>
<li>Components: {len(payload['product']['components'])} items</li>
</ul>

<p><strong>BOM Creation:</strong></p>
<ul>
<li>Surface: {payload['product']['width'] * payload['product']['height']} mm² ({(payload['product']['width'] * payload['product']['height'])/1000000:.4f} m²)</li>
<li>Circumference: {2 * (payload['product']['width'] + payload['product']['height'])} mm ({2 * (payload['product']['width'] + payload['product']['height'])/1000:.2f} m)</li>
<li>BOM components: {bom_components_count} items</li>
<li>BOM operations: {bom_operations_count} items</li>
</ul>

<p><strong>Components Processed:</strong></p>
<ul>{''.join(component_list_items)}</ul>

<p><strong>Final Return Data:</strong></p>
<ul>
<li>message: '{payload_type} order processing finished'</li>
<li>partner_id: {partner_id}</li>
<li>product_id: {product_id}</li>
<li>bom_id: {bom_id}</li>
<li>order_id: {order_id}</li>
<li>payload_type: {payload_type}</li>
<li>status: 'success'</li>
</ul>"""

        # Post the comprehensive message to the sale order's chatter
        try:
            models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
                'sale.order', 'message_post',
                [order_id],
                {
                    'body': chatter_message,
                    'body_is_html': True,                # keep HTML rendering
                    'message_type': 'comment',
                    'subtype_xmlid': 'mail.mt_note',     # 🔑 internal note
                    'attachment_ids': attachment_ids,
                })
            logger.info(f"Successfully posted comprehensive processing logs to sale order {order_id} chatter")
        except Exception as e:
            logger.error(f"Failed to post message to sale order chatter: {str(e)}")

        # Prepare response data
        response_data = {
            'message': f'{payload_type} order processing finished',
            'partner_id': partner_id,
            'product_id': product_id,
            'bom_id': bom_id,
            'order_id': order_id,
            'payload_type': payload_type,
            'status': 'success'
        }

        # Log the route call to Odoo logging model
        log_route_call(models, uid, '/handle-web-order', data, log_contents, response_data)

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

        logger.error(f"Error handling web order: {str(e)}")

        # Prepare error response data
        error_response = {'error': str(e), 'status': 'error'}

        # Log the error to Odoo logging model
        # Use original data if available, otherwise use empty dict
        request_data = data if 'data' in locals() else {}
        log_route_call(None, None, '/handle-web-order', request_data, log_contents, error_response)

        return jsonify(error_response), 500

@justframeit_bp.route('/handle-odoo-order', methods=['POST'])
def handle_odoo_order():
    """
    Handle existing Odoo sale order by copying product specs and creating new product with BOM

    Expected payload structure:
    {
        "id": 48
    }

    This will:
    1. Read the existing sale order and its product details
    2. Copy components from the existing product's BOM
    3. Create a new product with the same specifications
    4. Update the existing sale order with the new product
    """
    try:
        # Set up log capture
        log_capture_string = StringIO()
        log_handler = logging.StreamHandler(log_capture_string)
        log_handler.setLevel(logging.DEBUG)
        log_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        logger.addHandler(log_handler)

        logger.info("Starting Odoo order processing")

        # Get payload from request
        logger.info("Receiving sale order ID from request")
        data = request.get_json()
        if not data or 'id' not in data:
            logger.error("Missing sale_order_id in request")
            return jsonify({'error': 'Missing sale_order_id in request'}), 400

        sale_order_id = data['id']
        logger.info(f"Processing sale order ID: {sale_order_id}")

        # Get Odoo connection using existing helper functions
        logger.info("Connecting to Odoo")
        uid = get_uid()
        models = get_odoo_models()
        logger.info("Connected to Odoo successfully")

        # Get sale order details
        logger.info("Reading sale order details")
        sale_order = models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
            'sale.order', 'read',
            [sale_order_id],
            {'fields': ['order_line']})

        if not sale_order or not sale_order[0]['order_line']:
            logger.error("Sale order not found or has no order lines")
            return jsonify({'error': 'Sale order not found or has no order lines'}), 404

        logger.info(f"Found sale order with {len(sale_order[0]['order_line'])} order lines")

        # Get first product from sale order
        logger.info("Reading order line details")
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

        logger.info(f"Extracted product specs: {width}mm x {height}mm, €{price}")
        logger.info(f"Original product: {product_info[0]['name']} ({product_info[0]['default_code']})")

        # Get BOM for the existing product
        logger.info("Finding BOM for existing product")
        bom_ids = models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
            'mrp.bom', 'search',
            [[['product_tmpl_id', '=', product_info[0]['product_tmpl_id'][0]]]])

        if not bom_ids:
            logger.error("No BOM found for the existing product")
            return jsonify({'error': 'No BOM found for the existing product'}), 404

        logger.info(f"Found BOM ID: {bom_ids[0]}")

        bom_info = models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
            'mrp.bom', 'read',
            [bom_ids[0]],
            {'fields': ['bom_line_ids']})

        # Get components from existing BOM
        logger.info("Copying components from existing BOM")
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
            logger.debug(f"Copied component: {component_info[0]['name']} ({component_info[0]['default_code']})")

        logger.info(f"Copied {len(components)} components from existing BOM")

        # Generate timestamp for unique naming
        logger.info("Generating timestamp for new product naming")
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        # Create new product name and reference with timestamp
        product_name = f"Finished Product {timestamp}"
        product_reference = f"FINISHED_PRODUCT_{timestamp}"
        logger.info(f"New product name: {product_name}")
        logger.info(f"New product reference: {product_reference}")

        # Use shared function to create product and BOM
        logger.info("Creating new product and BOM")
        product_id, bom_id, _, _ = create_product_and_bom(
            models, uid,
            product_name, product_reference,
            width, height, price, components
        )

        # Compute BOM cost for the newly created product
        logger.info("Computing BOM cost for new product")
        # Note: button_bom_cost method raises an exception as expected behavior
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

            logger.info(f"Initial cost: €{initial_cost}")

            # Compute BOM cost - method raises exception as expected
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

            logger.info(f"New cost after BOM computation: €{new_cost}")

            if new_cost != initial_cost:
                logger.info("Cost change detected - BOM computation successful")
            else:
                logger.info("No cost change detected after BOM computation")

        except Exception as e:
            # This exception is expected - the method completes successfully despite raising it
            logger.info(f"BOM cost computation completed for Product Template ID {product_tmpl_id}")

        # Update existing sale order with new product
        logger.info("Updating sale order with new product")
        models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
            'sale.order', 'write',
            [sale_order_id, {
                'order_line': [(1, sale_order[0]['order_line'][0], {
                    'product_id': product_id,
                    'product_uom_qty': 1,
                    'price_unit': price
                })]
            }])
        logger.info("Sale order updated successfully")

        # Trigger price update based on pricelist
        logger.info("Triggering price update based on pricelist")
        # Note: This method raises an exception as expected behavior
        try:
            models.execute_kw(
                ODOO_DB, uid, ODOO_API_KEY,
                'sale.order', 'action_update_prices',
                [[sale_order_id]]
            )
            logger.info(f"Successfully triggered 'Update Price (based on price list)' for Sale Order ID {sale_order_id}")
        except Exception as e:
            # This exception is expected - the method completes successfully despite raising it
            logger.info(f"Price update completed for Sale Order ID {sale_order_id}")

        logger.info("Odoo order processing completed successfully")
        logger.info(f"Results - Product ID: {product_id}, BOM ID: {bom_id}, Updated Order ID: {sale_order_id}")

        # Generate timestamp for unique naming
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        # Save original payload as attachment
        attachment_ids = []
        try:
            payload_json = json.dumps(data, indent=2, ensure_ascii=False)
            attachment_name = f"odoo_order_payload_{timestamp}.json"

            attachment_vals = {
                'name': attachment_name,
                'type': 'binary',
                'datas': base64.b64encode(payload_json.encode('utf-8')).decode('ascii'),  # Base64 encode the UTF-8 bytes
                'res_model': 'sale.order',
                'res_id': sale_order_id,
                'mimetype': 'application/json'
            }

            attachment_id = models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
                'ir.attachment', 'create', [attachment_vals])

            attachment_ids = [attachment_id]
            logger.info(f"Successfully created payload attachment: {attachment_name} (ID: {attachment_id})")
        except Exception as e:
            logger.error(f"Failed to create payload attachment: {str(e)}")

        # Save processing logs as text attachment
        try:
            # Get the captured logs and remove the handler
            log_contents = log_capture_string.getvalue()
            logger.removeHandler(log_handler)
            log_capture_string.close()

            log_attachment_name = f"odoo_order_processing_logs_{timestamp}.txt"

            log_attachment_vals = {
                'name': log_attachment_name,
                'type': 'binary',
                'datas': base64.b64encode(log_contents.encode('utf-8')).decode('ascii'),  # Base64 encode the UTF-8 bytes
                'res_model': 'sale.order',
                'res_id': sale_order_id,
                'mimetype': 'text/plain'
            }

            log_attachment_id = models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
                'ir.attachment', 'create', [log_attachment_vals])

            attachment_ids.append(log_attachment_id)
            logger.info(f"Successfully created log attachment: {log_attachment_name} (ID: {log_attachment_id})")
        except Exception as e:
            logger.error(f"Failed to create log attachment: {str(e)}")
            # Clean up the handler even if attachment creation fails
            try:
                logger.removeHandler(log_handler)
                log_capture_string.close()
            except:
                pass

        # Create comprehensive HTML chatter message with all logs and final return
        component_list_items = []
        for i, component in enumerate(components, 1):
            component_list_items.append(f"<li>Component {i}: {component['name']} (ref: {component['reference']})</li>")

        chatter_message = f"""<p><strong>🔄 Odoo Order Processing Completed Successfully</strong></p>

<p><em>📎 Attachments: odoo_order_payload_{timestamp}.json, odoo_order_processing_logs_{timestamp}.txt</em></p>

<p><strong>📋 Order Summary:</strong></p>
<ul>
<li>Original Order ID: {sale_order_id}</li>
<li>New Product ID: {product_id}</li>
<li>New BOM ID: {bom_id}</li>
<li>Status: success</li>
</ul>

<p><strong>📝 Processing Details:</strong></p>

<p><strong>Order Analysis:</strong></p>
<ul>
<li>Read existing sale order details</li>
<li>Extracted product specs: {width}mm x {height}mm, €{price}</li>
<li>Original product: {product_info[0]['name']} ({product_info[0]['default_code']})</li>
<li>Found BOM ID: {bom_ids[0]}</li>
</ul>

<p><strong>Component Copying:</strong></p>
<ul>
<li>Copied {len(components)} components from existing BOM</li>
</ul>

<p><strong>Product Creation:</strong></p>
<ul>
<li>New product: {product_name} (ref: {product_reference})</li>
<li>Dimensions: {width}mm x {height}mm</li>
<li>Price: €{price}</li>
<li>Surface: {width * height} mm² ({(width * height)/1000000:.4f} m²)</li>
<li>Circumference: {2 * (width + height)} mm ({2 * (width + height)/1000:.2f} m)</li>
</ul>

<p><strong>Components Processed:</strong></p>
<ul>{''.join(component_list_items)}</ul>

<p><strong>BOM Cost Computation:</strong></p>
<ul>
<li>Initial cost: €{initial_cost if 'initial_cost' in locals() else 'N/A'}</li>
<li>Cost after computation: €{new_cost if 'new_cost' in locals() else 'N/A'}</li>
</ul>

<p><strong>Order Update:</strong></p>
<ul>
<li>Updated sale order with new product</li>
<li>Triggered price update based on pricelist</li>
</ul>

<p><strong>Final Return Data:</strong></p>
<ul>
<li>message: 'Odoo order processing finished'</li>
<li>product_id: {product_id}</li>
<li>bom_id: {bom_id}</li>
<li>updated_order_id: {sale_order_id}</li>
<li>status: 'success'</li>
</ul>"""

        # Post the comprehensive message to the sale order's chatter
        try:
            models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
                'sale.order', 'message_post',
                [sale_order_id],
                {
                    'body': chatter_message,
                    'body_is_html': True,                # keep HTML rendering
                    'message_type': 'comment',
                    'subtype_xmlid': 'mail.mt_note',     # 🔑 internal note
                    'attachment_ids': attachment_ids,
                })
            logger.info(f"Successfully posted comprehensive processing logs to sale order {sale_order_id} chatter")
        except Exception as e:
            logger.error(f"Failed to post message to sale order chatter: {str(e)}")

        # Prepare response data
        response_data = {
            'message': 'Odoo order processing finished',
            'product_id': product_id,
            'bom_id': bom_id,
            'updated_order_id': sale_order_id,
            'status': 'success'
        }

        # Log the route call to Odoo logging model
        log_route_call(models, uid, '/handle-odoo-order', data, log_contents, response_data)

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

        logger.error(f"Error handling Odoo order: {str(e)}")

        # Prepare error response data
        error_response = {'error': str(e), 'status': 'error'}

        # Log the error to Odoo logging model
        # Use original data if available, otherwise use empty dict
        request_data = data if 'data' in locals() else {}
        log_route_call(None, None, '/handle-odoo-order', request_data, log_contents, error_response)

        return jsonify(error_response), 500


