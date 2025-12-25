from flask import Blueprint, jsonify, request
import xmlrpc.client
import os
import json
import base64
import re
import time
from io import StringIO
from dotenv import load_dotenv
import logging
from datetime import datetime
import requests
from utils import log_route_call
import concurrent.futures
import threading

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Thread-safe counter for unique product references
_reference_counter_lock = threading.Lock()
_reference_counter = 0

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

def generate_product_reference(line_logger=None):
    """
    Generate a unique product reference using the current timestamp in microseconds.
    Thread-safe: uses a counter to ensure uniqueness even when called simultaneously.
    
    Converts the Unix timestamp in microseconds to base36 (0-9, A-Z).
    Each microsecond produces a unique reference (~10-11 characters).
    
    Args:
        line_logger: Optional logger instance for parallel processing
    
    Returns:
        str: Unique product reference (e.g., 'HF4D2K8M1P')
    """
    global _reference_counter
    
    # Base36 alphabet: 0-9, A-Z (36 characters, uppercase only)
    BASE36_CHARS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    
    # Thread-safe: combine timestamp with a counter to ensure uniqueness
    with _reference_counter_lock:
        _reference_counter += 1
        counter_value = _reference_counter
    
    # Get current Unix timestamp in microseconds and add counter for uniqueness
    timestamp = int(time.time() * 1000000) + counter_value
    
    # Convert timestamp to base36
    if timestamp == 0:
        reference = "0"
    else:
        reference = ""
        num = timestamp
        while num > 0:
            reference = BASE36_CHARS[num % 36] + reference
            num //= 36
    
    log = line_logger or logger
    log.info(f"Generated product reference: {reference}")
    return reference


def download_image_as_base64(image_url):
    """
    Download an image from URL and return it as base64 encoded string.
    
    Args:
        image_url: URL of the image to download
        
    Returns:
        str: Base64 encoded image data, or None if download fails
    """
    try:
        logger.info(f"Downloading image from: {image_url}")
        response = requests.get(image_url, timeout=30)
        response.raise_for_status()
        
        # Convert image content to base64
        image_base64 = base64.b64encode(response.content).decode('ascii')
        logger.info(f"Successfully downloaded and encoded image ({len(response.content)} bytes)")
        return image_base64
    except Exception as e:
        logger.error(f"Failed to download image from {image_url}: {str(e)}")
        return None


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

def get_visible_components_list(models, uid, components, line_logger=None):
    """
    Get list of visible components (where x_studio_is_visible_in_portal_reports is True).
    Uses batch API call for performance.
    
    Args:
        models: Odoo models proxy
        uid: User ID
        components: List of component dictionaries with 'name' and 'reference' keys
        line_logger: Optional logger instance for parallel processing
        
    Returns:
        list: List of formatted strings "[reference] component_name" for visible components
    """
    log = line_logger or logger
    
    if not components:
        return []
    
    # Collect all references for batch search
    references = [c['reference'] for c in components if c.get('reference')]
    
    if not references:
        return []
    
    try:
        # Single batch search for all components
        all_component_info = models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
            'product.product', 'search_read',
            [[['x_studio_product_code', 'in', references]]],
            {'fields': ['name', 'x_studio_product_code', 'x_studio_is_visible_in_portal_reports']})
        
        # Filter visible components and build result list
        visible_components = []
        for comp_info in all_component_info:
            if comp_info.get('x_studio_is_visible_in_portal_reports'):
                comp_ref = comp_info.get('x_studio_product_code', '')
                comp_name = comp_info.get('name', '')
                visible_components.append(f"[{comp_ref}] {comp_name}")
                log.debug(f"Found visible component: [{comp_ref}] {comp_name}")
        
        log.info(f"Found {len(visible_components)} visible component(s) from {len(references)} checked")
        return visible_components
        
    except Exception as e:
        log.warning(f"Failed to batch check visibility for components: {str(e)}")
        return []


def build_order_line_description_odoo(original_product_name, width, height, visible_components):
    """
    Build order line description for Odoo orders.
    Uses original template name + dimensions + "Materiaal:" + visible components.
    
    Args:
        original_product_name: Original product template name
        width: Product width in mm
        height: Product height in mm
        visible_components: List of formatted visible component strings
        
    Returns:
        str: Description string for order line
    """
    # Convert mm to cm for display
    width_cm = width / 10
    height_cm = height / 10
    
    # Start with original product name and dimensions
    description = f"{original_product_name} ({width_cm}x{height_cm})"
    
    # Add visible components with "Materiaal:" prefix
    if visible_components:
        description += " - Materiaal: " + " - ".join(visible_components)
    
    logger.info(f"Built Odoo order line description with {len(visible_components)} visible component(s)")
    return description


def build_visible_components_suffix(visible_components):
    """
    Build the visible components suffix to append to existing descriptions.
    
    Args:
        visible_components: List of formatted visible component strings
        
    Returns:
        str: Suffix string with "Materiaal:" and visible components, or empty string if none
    """
    if visible_components:
        return "\n\nMateriaal: " + " - ".join(visible_components)
    return ""


def process_order_line_parallel(
    uid, line_index, total_lines, order_line_id,
    order_lines_by_id, products_by_id, bom_by_product, bom_by_template, sale_order_id
):
    """
    Process a single order line in parallel. Each line gets its own log buffer
    and its own Odoo connection (xmlrpc is not thread-safe).
    
    Args:
        uid: User ID
        line_index: Index of the order line (0-based)
        total_lines: Total number of order lines
        order_line_id: ID of the order line to process
        order_lines_by_id: Pre-fetched order line data dictionary
        products_by_id: Pre-fetched product data dictionary
        bom_by_product: Pre-fetched variant BOM data dictionary
        bom_by_template: Pre-fetched template BOM data dictionary
        sale_order_id: ID of the sale order
        
    Returns:
        tuple: (result_dict, log_string) where result_dict contains processing results
               and log_string contains all log messages for this order line
    """
    # Create a thread-local StringIO for capturing logs
    line_log_buffer = StringIO()
    line_handler = logging.StreamHandler(line_log_buffer)
    line_handler.setLevel(logging.DEBUG)
    line_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    
    # Create a thread-specific logger
    line_logger = logging.getLogger(f'justframeit.line.{order_line_id}')
    line_logger.handlers = []  # Clear any existing handlers
    line_logger.addHandler(line_handler)
    line_logger.setLevel(logging.DEBUG)
    line_logger.propagate = False  # Don't propagate to parent (main logger)
    
    try:
        # CRITICAL: Create a thread-local Odoo connection
        # xmlrpc.client.ServerProxy is NOT thread-safe, so each thread needs its own connection
        models = get_odoo_models()
        line_logger.info(f"--- Processing order line {line_index + 1}/{total_lines} (ID: {order_line_id}) ---")
        
        # Get order line details from pre-fetched data
        order_line = [order_lines_by_id[order_line_id]]
        product_info = [products_by_id[order_line[0]['product_id'][0]]]
        
        # Extract product details
        width = product_info[0]['x_studio_width']
        height = product_info[0]['x_studio_height']
        price = order_line[0]['price_unit']
        quantity = order_line[0]['product_uom_qty']
        current_product_name = product_info[0].get('display_name') or product_info[0]['name']
        original_product_code = product_info[0]['default_code']
        order_line_description = order_line[0].get('name', '')
        product_description_sale = product_info[0].get('description_sale', '')
        
        # Check if current product is already a PR product (re-execution scenario)
        pr_pattern = r'PR\d{6}'
        is_pr_product = bool(re.search(pr_pattern, current_product_name or ''))
        
        if is_pr_product:
            original_product_name = None
            if product_description_sale and product_description_sale.startswith('Original: '):
                original_product_name = product_description_sale[10:].strip()
                line_logger.info(f"Detected re-execution: Retrieved original template name '{original_product_name}' from product description_sale")
            
            if not original_product_name and order_line_description:
                dimension_pattern = r'^(.+?)\s*\(\d+\.?\d*x\d+\.?\d*\)'
                match = re.match(dimension_pattern, order_line_description)
                if match:
                    extracted_name = match.group(1).strip()
                    if not re.search(pr_pattern, extracted_name):
                        original_product_name = extracted_name
                        line_logger.info(f"Detected re-execution: Extracted original template name '{original_product_name}' from order line description")
            
            if not original_product_name:
                original_product_name = current_product_name
                line_logger.warning(f"Re-execution detected but couldn't recover original name. Using: {original_product_name}")
        else:
            original_product_name = current_product_name
        
        line_logger.info(f"Extracted product specs: {width}mm x {height}mm, €{price}, qty: {quantity}")
        line_logger.info(f"Current product: {current_product_name} ({original_product_code})")
        line_logger.info(f"Original template name for description: {original_product_name}")
        
        # Check if quantity is different than 1 (preset products - skip processing)
        if quantity != 1:
            line_logger.info(f"Product '{current_product_name}' has quantity {quantity} (preset). Skipping processing for this order line.")
            result = {
                'order_line_id': order_line_id,
                'original_product': current_product_name,
                'status': 'skipped',
                'reason': f'Preset product (quantity: {quantity})',
                'chatter_message': f"ℹ️ Product '{current_product_name}' has quantity {quantity} (preset). Processing has been skipped for this order line."
            }
            line_logger.info(f"--- Completed processing order line {line_index + 1}/{total_lines} ---")
            return (result, line_log_buffer.getvalue())
        
        # Check if product is updatable (variants are not updatable)
        product_updatable = order_line[0]['product_updatable']
        product_template_attribute_value_ids = order_line[0]['product_template_attribute_value_ids']
        
        if not product_updatable:
            line_logger.warning(f"Product '{original_product_name}' is a variant and not updatable. Processing blocked for this order line.")
            result = {
                'order_line_id': order_line_id,
                'original_product': original_product_name,
                'status': 'skipped',
                'reason': 'Product is unupdatable (variant)',
                'chatter_message': f"⚠️ Product '{original_product_name}' is a variant and cannot be updated on order line {order_line_id}. Processing has been blocked for this order line."
            }
            line_logger.info(f"--- Completed processing order line {line_index + 1}/{total_lines} ---")
            return (result, line_log_buffer.getvalue())
        
        # Get BOM for the existing product from pre-fetched data
        line_logger.info("Finding BOM for existing product (from batch data)")
        
        product_id = order_line[0]['product_id'][0]
        product_tmpl_id = product_info[0]['product_tmpl_id'][0]
        is_variant = len(product_template_attribute_value_ids) > 0
        
        bom_data = None
        if is_variant:
            line_logger.info(f"Product is a variant (ID: {product_id}), looking up variant-specific BOM")
            bom_data = bom_by_product.get(product_id)
        
        if not bom_data:
            line_logger.info(f"Looking up template BOM for template ID: {product_tmpl_id}")
            bom_data = bom_by_template.get(product_tmpl_id)
        
        if not bom_data:
            bom_type = "variant-specific" if is_variant else "template"
            line_logger.warning(f"No {bom_type} BOM found for product '{original_product_name}' - skipping this line")
            result = {
                'order_line_id': order_line_id,
                'original_product': original_product_name,
                'status': 'skipped',
                'reason': f'No {bom_type} BOM found'
            }
            line_logger.info(f"--- Completed processing order line {line_index + 1}/{total_lines} ---")
            return (result, line_log_buffer.getvalue())
        
        bom_type_found = "variant-specific" if bom_data.get('product_id') and bom_data['product_id'][0] == product_id else "template"
        line_logger.info(f"Found BOM ID: {bom_data['id']} ({bom_type_found} BOM)")
        
        bom_info = [bom_data]
        
        # Fetch BOM lines and component products
        line_logger.info("Copying components from existing BOM (batch fetch)")
        bom_line_ids = bom_info[0]['bom_line_ids']
        
        all_bom_lines = models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
            'mrp.bom.line', 'read',
            [bom_line_ids],
            {'fields': ['product_id', 'product_qty']})
        
        component_product_ids = [line['product_id'][0] for line in all_bom_lines]
        all_component_products = models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
            'product.product', 'read',
            [component_product_ids],
            {'fields': ['name', 'x_studio_product_code']})
        
        component_products_by_id = {prod['id']: prod for prod in all_component_products}
        
        components = []
        for bom_line in all_bom_lines:
            component_info = component_products_by_id[bom_line['product_id'][0]]
            component_data = {
                'name': component_info['name'],
                'reference': component_info['x_studio_product_code']
            }
            original_qty = bom_line.get('product_qty', 1)
            if original_qty != 1:
                component_data['qty'] = original_qty
                line_logger.debug(f"Copied component: {component_info['name']} ({component_info['x_studio_product_code']}) with original qty: {original_qty}")
            else:
                line_logger.debug(f"Copied component: {component_info['name']} ({component_info['x_studio_product_code']})")
            components.append(component_data)
        
        line_logger.info(f"Copied {len(components)} components from existing BOM")
        
        # Generate product reference
        line_logger.info("Generating product reference")
        product_name = generate_product_reference(line_logger)
        product_reference = product_name
        line_logger.info(f"New product name: {product_name}")
        line_logger.info(f"New product reference: {product_reference}")
        
        # Create product and BOM
        line_logger.info("Creating new product and BOM")
        new_product_id, bom_id, bom_components_count, bom_operations_count, skipped_components = create_product_and_bom(
            models, uid,
            product_name, product_reference,
            width, height, price, components,
            original_template_name=original_product_name,
            line_logger=line_logger
        )
        
        if skipped_components:
            line_logger.warning(f"Order line {line_index + 1}: {len(skipped_components)} component(s) were skipped")
        
        # Get the product template ID from the created product
        product_data = models.execute_kw(
            ODOO_DB, uid, ODOO_API_KEY,
            'product.product', 'read', [new_product_id], {'fields': ['product_tmpl_id']}
        )
        new_product_tmpl_id = product_data[0]['product_tmpl_id'][0]
        line_logger.info(f"New product template ID: {new_product_tmpl_id} (BOM cost will be computed at end)")
        
        # Get visible components and build order line description
        line_logger.info("Getting visible components for order line description")
        visible_components = get_visible_components_list(models, uid, components, line_logger)
        
        line_logger.info("Building order line description with original product name")
        new_order_line_description = build_order_line_description_odoo(
            original_product_name, width, height, visible_components
        )
        
        # Update existing sale order line with new product
        if product_updatable:
            line_logger.info(f"Updating order line {order_line_id} with new product")
            models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
                'sale.order', 'write',
                [sale_order_id, {
                    'order_line': [(1, order_line_id, {
                        'product_id': new_product_id,
                        'product_uom_qty': quantity,
                        'price_unit': price,
                        'name': new_order_line_description
                    })]
                }])
            line_logger.info(f"Order line {order_line_id} updated successfully with quantity: {quantity}")
        else:
            line_logger.info(f"Skipping order line {order_line_id} update because product is not updatable (variant)")
        
        # Build result
        result = {
            'order_line_id': order_line_id,
            'original_product': original_product_name,
            'original_product_code': original_product_code,
            'new_product_id': new_product_id,
            'new_product_tmpl_id': new_product_tmpl_id,
            'new_product_name': product_name,
            'new_product_reference': product_reference,
            'bom_id': bom_id,
            'width': width,
            'height': height,
            'price': price,
            'quantity': quantity,
            'components': components,
            'skipped_components': skipped_components,
            'bom_components_count': bom_components_count,
            'bom_operations_count': bom_operations_count,
            'initial_cost': 0,
            'new_cost': 0,
            'status': 'success'
        }
        
        line_logger.info(f"--- Completed processing order line {line_index + 1}/{total_lines} ---")
        return (result, line_log_buffer.getvalue())
        
    except Exception as e:
        line_logger.error(f"Error processing order line {order_line_id}: {str(e)}")
        # Include all expected keys to avoid KeyErrors downstream
        result = {
            'order_line_id': order_line_id,
            'original_product': f'Unknown (error during processing)',
            'original_product_code': '',
            'status': 'error',
            'reason': str(e)
        }
        logs = line_log_buffer.getvalue()
        line_logger.removeHandler(line_handler)
        return (result, logs)
    finally:
        try:
            line_logger.removeHandler(line_handler)
            line_log_buffer.close()
        except:
            pass


def create_product_and_bom(models, uid, product_name, product_reference, width, height, price, components, product_template_attribute_value_ids=None, original_template_name=None, line_logger=None):
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
        components: List of component dictionaries with 'name' and 'reference' keys.
                    Optionally includes 'qty' key - if provided and not equal to 1,
                    this quantity is used directly without recalculation.
        product_template_attribute_value_ids: List of attribute value IDs for variants (optional)
        original_template_name: Original template/variant name to store for reference (optional)
        line_logger: Optional logger instance for parallel processing

    Returns:
        tuple: (product_id, bom_id, bom_components_count, bom_operations_count, skipped_components)
               skipped_components is a list of dicts with 'name', 'reference', 'reason' for components not found
    """
    # Use provided logger or fall back to global logger
    log = line_logger or logger
    
    log.info("Starting product and BOM creation process")
    log.info(f"Product: {product_name} (ref: {product_reference})")
    log.info(f"Dimensions: {width}mm x {height}mm, Price: €{price}")
    log.info(f"Components: {len(components)} items")
    if original_template_name:
        log.info(f"Original template name: {original_template_name}")

    # Create product
    log.info("Creating product in Odoo")
    product_vals = {
        'name': product_name,
        'type': 'consu',
        'x_studio_width': width,
        'x_studio_height': height,
        'list_price': price,
        'default_code': product_reference,
        'categ_id': 8,  # Set category ID to 8
    }
    
    # Store original template name in description_sale field for future reference
    if original_template_name:
        product_vals['description_sale'] = f"Original: {original_template_name}"

    # Add variant attributes if provided (for variant products)
    if product_template_attribute_value_ids:
        product_vals['product_template_attribute_value_ids'] = [(6, 0, product_template_attribute_value_ids)]
        log.info(f"Including {len(product_template_attribute_value_ids)} variant attribute(s) in new product")

    log.debug(f"Product creation values: {product_vals}")
    product_id = models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
        'product.product', 'create', [product_vals])
    log.info(f"Product created with ID: {product_id}")

    # Create components and BOM
    log.info("Setting up BOM creation")
    bom_components = []
    bom_operations = []
    surface = width * height
    circumference = 2 * (width + height)

    log.info(f"Surface: {surface} mm² ({surface/1000000:.4f} m²)")
    log.info(f"Circumference: {circumference} mm ({circumference/1000:.2f} m)")

    # Convert dimensions to meters for duration rules
    surface_m2 = surface / 1000000  # Convert mm² to m²
    circumference_m = circumference / 1000  # Convert mm to m
    log.debug(f"Converted dimensions - Surface: {surface_m2} m², Circumference: {circumference_m} m")

    log.info("Processing components for BOM")
    skipped_components = []  # Track skipped components for logging
    
    # BATCH OPTIMIZATION: Fetch all components, services, and duration rules upfront
    # instead of making 2-4 API calls per component
    
    # Step 1: Collect all references and batch search_read all components
    references = [c['reference'] for c in components if c.get('reference')]
    log.info(f"Batch fetching {len(references)} components by reference")
    
    all_component_data = models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
        'product.product', 'search_read',
        [[['x_studio_product_code', 'in', references]]],
        {'fields': ['id', 'x_studio_product_code', 'x_studio_price_computation', 
                    'x_studio_associated_service', 'x_studio_associated_service_duration_rule']})
    
    # Create lookup by reference
    components_by_ref = {c['x_studio_product_code']: c for c in all_component_data}
    log.info(f"Found {len(all_component_data)} components in Odoo")
    
    # Step 2: Collect all unique service IDs and duration rule IDs for batch fetch
    service_ids = set()
    duration_rule_ids = set()
    for comp_data in all_component_data:
        if comp_data.get('x_studio_associated_service'):
            service_ids.add(comp_data['x_studio_associated_service'][0])
        if comp_data.get('x_studio_associated_service_duration_rule'):
            duration_rule_ids.update(comp_data['x_studio_associated_service_duration_rule'])
    
    # Step 3: Batch fetch all services
    services_by_id = {}
    if service_ids:
        log.info(f"Batch fetching {len(service_ids)} services")
        all_services = models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
            'x_services', 'read',
            [list(service_ids)],
            {'fields': ['x_name', 'x_studio_associated_work_center']})
        services_by_id = {s['id']: s for s in all_services}
    
    # Step 4: Batch fetch all duration rules
    duration_rules_by_id = {}
    if duration_rule_ids:
        log.info(f"Batch fetching {len(duration_rule_ids)} duration rules")
        all_duration_rules = models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
            'x_services_duration_rules', 'read',
            [list(duration_rule_ids)],
            {'fields': ['x_studio_quantity', 'x_duurtijd_totaal']})
        duration_rules_by_id = {r['id']: r for r in all_duration_rules}
    
    # Step 5: Process components using batch-fetched data
    for i, component in enumerate(components, 1):
        log.info(f"Processing component {i}/{len(components)}: {component['name']} (ref: {component['reference']})")

        # Look up component from batch data
        component_info = components_by_ref.get(component['reference'])
        
        if not component_info:
            # Log warning and skip this component instead of crashing
            skip_message = f"Component '{component['name']}' with reference '{component['reference']}' not found in Odoo - SKIPPED"
            log.warning(skip_message)
            skipped_components.append({
                'name': component['name'],
                'reference': component['reference'],
                'reason': 'Not found in Odoo'
            })
            continue  # Skip to next component

        component_id = component_info['id']
        log.info(f"Found component ID: {component_id}")

        # Check if component has an existing quantity (from original BOM) that's not 1
        # If so, use it directly without recalculation
        if 'qty' in component and component['qty'] != 1:
            quantity = component['qty']
            log.debug(f"Quantity from original BOM: {quantity} (preserved without recalculation)")
        # Otherwise, calculate quantity based on price computation method
        elif component_info.get('x_studio_price_computation') == 'Circumference':
            quantity = circumference_m
            log.debug(f"Quantity calculation: Circumference method → {quantity} m")
        elif component_info.get('x_studio_price_computation') == 'Surface':
            quantity = surface_m2
            log.debug(f"Quantity calculation: Surface method → {quantity} m²")
        else:
            quantity = 1
            log.debug("Quantity calculation: Default method → 1 unit")

        log.info(f"Component quantity: {quantity}")

        # Add to BOM components
        log.debug("Adding component to BOM")
        bom_components.append((0, 0, {
            'product_id': component_id,
            'product_qty': quantity,
        }))
        log.info("Added component to BOM")

        # Handle associated service/operation using batch-fetched data
        if component_info.get('x_studio_associated_service'):
            service_id = component_info['x_studio_associated_service'][0]
            log.info(f"Processing associated service ID: {service_id}")

            # Get service details from batch data
            service_info = services_by_id.get(service_id)
            if not service_info:
                log.warning(f"Service ID {service_id} not found in batch data - skipping operation")
                continue
                
            log.debug(f"Service info: {service_info['x_name']}")

            # Get duration rules from batch data
            duration_rule_ids_for_component = component_info.get('x_studio_associated_service_duration_rule', [])
            duration_rules = [duration_rules_by_id[rid] for rid in duration_rule_ids_for_component if rid in duration_rules_by_id]

            if not duration_rules:
                log.warning(f"No duration rules found for component {component['reference']} - skipping operation")
                continue

            # Find appropriate duration based on x_studio_quantity
            relevant_value = quantity  # Use the calculated quantity (surface, circumference, or default)
            rules_sorted = sorted(duration_rules, key=lambda x: x['x_studio_quantity'])
            matching_rule = next((rule for rule in rules_sorted if rule['x_studio_quantity'] >= relevant_value), rules_sorted[-1])
            duration_seconds = matching_rule['x_duurtijd_totaal']
            log.debug(f"Duration calculation: Quantity-based ({relevant_value}) → {duration_seconds} seconds")

            # Convert duration from seconds to minutes and calculate MM:SS format
            duration_minutes = duration_seconds / 60
            minutes = int(duration_minutes)
            seconds = int((duration_minutes - minutes) * 60)
            odoo_display = f"{minutes:02d}:{seconds:02d}"
            log.info(f"Duration for {service_info['x_name']}: {duration_seconds}s = {duration_minutes:.2f}min ({odoo_display})")

            # Add operation to BOM
            log.debug("Adding operation to BOM")
            bom_operations.append((0, 0, {
                'name': service_info['x_name'],
                'time_cycle_manual': duration_minutes,
                'workcenter_id': service_info['x_studio_associated_work_center'][0] if service_info.get('x_studio_associated_work_center') else False
            }))
            log.info("Added operation to BOM")

    # Log summary of skipped components
    if skipped_components:
        log.warning(f"⚠️ {len(skipped_components)} component(s) were SKIPPED (not found in Odoo):")
        for skipped in skipped_components:
            log.warning(f"  - {skipped['name']} (ref: {skipped['reference']}): {skipped['reason']}")
    else:
        log.info("All components were found and added to BOM")

    # Get the product template ID from the created product
    log.info("Getting product template ID")
    product_data = models.execute_kw(
        ODOO_DB, uid, ODOO_API_KEY,
        'product.product', 'read', [product_id], {'fields': ['product_tmpl_id']}
    )
    product_tmpl_id = product_data[0]['product_tmpl_id'][0]
    log.debug(f"Product template ID: {product_tmpl_id}")

    # Set route_ids for finished products (MTO and Manufacturing, exclude Buy)
    log.info("Setting route_ids for finished product (MTO and Manufacturing)")
    models.execute_kw(
        ODOO_DB, uid, ODOO_API_KEY,
        'product.template', 'write',
        [[product_tmpl_id], {'route_ids': [(6, 0, [1, 4])]}]  # 1=MTO, 4=Manufacture
    )
    log.info("Successfully set route_ids to [1, 4] (MTO and Manufacturing)")

    # Create Bill of Materials using the template ID
    log.info("Creating Bill of Materials")
    log.info(f"BOM components: {len(bom_components)}")
    log.info(f"BOM operations: {len(bom_operations)}")

    bom_vals = {
        'product_tmpl_id': product_tmpl_id,
        'product_qty': 1,
        'type': 'normal',
        'bom_line_ids': bom_components,
        'operation_ids': bom_operations,
    }

    log.debug(f"BOM creation values: {len(bom_components)} components, {len(bom_operations)} operations")
    bom_id = models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
        'mrp.bom', 'create', [bom_vals])

    log.info(f"BOM created with ID: {bom_id}")
    log.info("Product and BOM creation completed successfully")

    return product_id, bom_id, len(bom_components), len(bom_operations), skipped_components

def interpret_craft_payload(craft_payload):
    """
    Convert a complex Craft CMS payload to the simple payload format used by handle_web_order.

    Args:
        craft_payload (dict): The complex payload from Craft CMS

    Returns:
        dict: Simplified payload with customer and products (list) information
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

        # Extract customer name with fallback to address fullName if customer name is missing
        customer_name = customer_data.get('fullName')
        if not customer_name:
            customer_name = address.get('fullName', '')

        customer = {
            'name': customer_name,
            'email': customer_data.get('email', ''),
            'phone': customer_data.get('userPhone') or address.get('phone', ''),
            'street': f"{address.get('addressLine1', '')} {address.get('addressLine2', '')}".strip(),
            'city': address.get('locality', ''),
            'zip': address.get('postalCode', ''),
            'country': 'Belgium' if address.get('countryCode') == 'BE' else address.get('countryCode', '')
        }
        logger.info(f"Customer extracted: {customer['name']} ({customer['email']})")

        # Extract product information from ALL line items
        logger.info("Extracting product information from all line items")
        line_items = craft_payload.get('lineItems', [])
        logger.debug(f"Found {len(line_items)} line items")

        if not line_items:
            logger.error("No line items found in the order")
            raise ValueError("No line items found in the order")

        # Extract discounts from adjustments (one discount per line item, in order)
        adjustments = craft_payload.get('adjustments', [])
        discount_adjustments = [adj for adj in adjustments if adj.get('type') == 'discount']
        logger.info(f"Found {len(discount_adjustments)} discount adjustments")

        # Helper function to extract product code (part before dot)
        def extract_product_code(sku):
            if sku and '.' in sku:
                original = sku
                extracted = sku.split('.')[0]
                logger.debug(f"Extracted product code: {original} → {extracted}")
                return extracted
            return sku

        # Process ALL line items
        products = []
        for item_index, line_item in enumerate(line_items):
            logger.info(f"Processing line item {item_index + 1}/{len(line_items)}")
            options = line_item.get('options', {})
            configuration = options.get('configuration', {})

            logger.debug(f"Configuration keys: {list(configuration.keys()) if configuration else 'None'}")

            # Extract dimensions (convert from cm to mm)
            width_cm = configuration.get('width', 0)
            height_cm = configuration.get('height', 0)
            
            # Use unit price (excl. VAT) instead of total (incl. VAT) to avoid double VAT
            price = line_item.get('price', 0)
            
            # Extract quantity
            qty = line_item.get('qty', 1)

            # Convert cm to mm
            width = width_cm * 10
            height = height_cm * 10

            logger.info(f"Line item {item_index + 1}: {width}mm x {height}mm, Unit Price: €{price}, Qty: {qty}")

            # Extract components from priceBreakdown (contains all products with SKUs)
            logger.info(f"Extracting components from priceBreakdown for line item {item_index + 1}")
            components = []
            price_breakdown = options.get('priceBreakdown', {})

            # Define product type mappings for readable names
            product_type_names = {
                'list': 'Frame',
                'glass': 'Glass',
                'passePartout': 'Passe-Partout',
                'backCover': 'Back Cover',
                'printOption': 'Print Option',
                'glueOption': 'Glue Option',
                'glueSurface': 'Glue Surface',
                'AK.DIENST': 'Assembling Frame',
                'BF.X': 'Packaging Material',
                'IN.DIENST': 'Framing'
            }

            # Process all items in priceBreakdown
            for product_type, breakdown_data in price_breakdown.items():
                if not isinstance(breakdown_data, dict):
                    continue
                    
                products_list = breakdown_data.get('products', [])
                if not products_list:
                    logger.debug(f"No products found in priceBreakdown for '{product_type}'")
                    continue

                for product_item in products_list:
                    sku = product_item.get('sku')
                    if sku:
                        processed_sku = extract_product_code(sku)
                        # Get readable name or use the product field from breakdown, fallback to product_type
                        readable_name = product_type_names.get(product_type, product_item.get('product', product_type))
                        components.append({
                            'name': readable_name,
                            'reference': processed_sku
                        })
                        logger.info(f"Added {readable_name} component: {processed_sku} (from priceBreakdown.{product_type})")

            logger.info(f"Total components extracted for line item {item_index + 1}: {len(components)}")

            # Get description for product name
            description = line_item.get('description', f'Product {item_index + 1}')

            # Get discount percentage for this line item (if available)
            discount_percent = 0
            if item_index < len(discount_adjustments):
                discount_adj = discount_adjustments[item_index]
                # Try to get percentage from sourceSnapshot, fallback to calculating from amount
                source_snapshot = discount_adj.get('sourceSnapshot', {})
                if source_snapshot.get('percentage'):
                    discount_percent = source_snapshot['percentage']
                    logger.info(f"Line item {item_index + 1}: Discount {discount_percent}% from sourceSnapshot")
                elif discount_adj.get('amount'):
                    # Calculate percentage from amount if not in sourceSnapshot
                    # Amount is negative, and subtotal = price * qty
                    subtotal = price * qty
                    if subtotal > 0:
                        discount_percent = abs(discount_adj['amount']) / subtotal * 100
                        logger.info(f"Line item {item_index + 1}: Calculated discount {discount_percent:.2f}% from amount")
                
                discount_name = discount_adj.get('name', 'Discount')
                discount_description = discount_adj.get('description', '')
                logger.info(f"Line item {item_index + 1}: {discount_name} - {discount_description}")

            # Extract photo information if available
            photo_info = options.get('photo')
            photo_url = None
            if photo_info and isinstance(photo_info, dict):
                photo_url = photo_info.get('path')
                if photo_url:
                    logger.info(f"Found photo URL for line item {item_index + 1}: {photo_url}")

            product = {
                'width': width,
                'height': height,
                'price': price,
                'qty': qty,
                'discount': discount_percent,
                'components': components,
                'description': description,
                'photo_url': photo_url
            }
            products.append(product)

        logger.info(f"Total products extracted: {len(products)}")

        logger.info("Creating simplified payload structure")
        payload = {
            'customer': customer,
            'products': products  # Changed from 'product' to 'products' (list)
        }

        logger.info("Craft CMS payload interpretation completed successfully")
        logger.debug(f"Final payload structure: customer={bool(customer)}, products={len(products)}")

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
            # Support both 'product' (single) and 'products' (list) formats
            if 'products' not in data and 'product' not in data:
                logger.error("Simple payload missing required fields")
                return jsonify({'error': 'Missing product(s) or customer data in request'}), 400
            if 'customer' not in data:
                logger.error("Simple payload missing customer field")
                return jsonify({'error': 'Missing customer data in request'}), 400
            # Convert single product format to products list for consistency
            if 'product' in data and 'products' not in data:
                data['products'] = [data['product']]
            payload = data

        # Log the payload type for debugging
        payload_type = "Craft CMS" if is_craft_payload else "Simple"
        logger.info(f"Processing {payload_type} payload successfully")

        # Generate timestamp for unique naming if needed
        logger.info("Generating timestamp for unique naming")
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        # Get Odoo connection using existing helper functions
        logger.info("Connecting to Odoo")
        uid = get_uid()
        models = get_odoo_models()
        logger.info("Connected to Odoo successfully")

        # Get or create customer
        logger.info("Processing customer information")
        partner_id, customer_action = get_or_create_customer(models, uid, payload['customer'])

        # Process ALL products and create order lines
        products = payload['products']
        logger.info(f"Processing {len(products)} product(s)")
        
        order_lines = []
        created_products = []  # Track all created products for logging
        
        for product_index, product_data in enumerate(products):
            logger.info(f"--- Processing product {product_index + 1}/{len(products)} ---")
            
            # Auto-generate product name and reference using sequence system if not provided
            product_name = product_data.get('name', '').strip() if product_data.get('name') else ''
            product_reference = product_data.get('reference', '').strip() if product_data.get('reference') else ''

            # Generate product name (PR-XXXXXX format using base64 timestamp)
            if not product_name:
                product_name = generate_product_reference()
                logger.info(f"Auto-generated product name: {product_name}")

            # Use same reference if not provided
            if not product_reference:
                product_reference = product_name
                logger.info(f"Auto-generated product reference: {product_reference}")

            # Use shared function to create product and BOM
            logger.info(f"Creating product and BOM for product {product_index + 1}")
            product_id, bom_id, bom_components_count, bom_operations_count, skipped_components = create_product_and_bom(
                models, uid,
                product_name, product_reference,
                product_data['width'], product_data['height'],
                product_data['price'], product_data['components']
            )
            
            # Log skipped components if any
            if skipped_components:
                logger.warning(f"Product {product_index + 1}: {len(skipped_components)} component(s) were skipped")
            
            # Attach image to product if photo URL is available
            photo_url = product_data.get('photo_url')
            image_base64 = None
            image_attachment_id = None
            if photo_url:
                logger.info(f"Downloading image for product {product_id}")
                image_base64 = download_image_as_base64(photo_url)
                if image_base64:
                    # Get the product template ID
                    product_data_info = models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
                        'product.product', 'read', [product_id], {'fields': ['product_tmpl_id']})
                    product_tmpl_id = product_data_info[0]['product_tmpl_id'][0]
                    logger.info(f"Product template ID: {product_tmpl_id}")
                    
                    # Set as product variant main image (product.product)
                    try:
                        models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
                            'product.product', 'write',
                            [[product_id], {'image_1920': image_base64}])
                        logger.info(f"Successfully set main image for product.product {product_id}")
                    except Exception as e:
                        logger.error(f"Failed to set main image for product.product {product_id}: {str(e)}")
                    
                    # Set as product template main image (product.template)
                    try:
                        models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
                            'product.template', 'write',
                            [[product_tmpl_id], {'image_1920': image_base64}])
                        logger.info(f"Successfully set main image for product.template {product_tmpl_id}")
                    except Exception as e:
                        logger.error(f"Failed to set main image for product.template {product_tmpl_id}: {str(e)}")
                    
                    # Extract filename from URL or generate one
                    image_filename = photo_url.split('/')[-1] if '/' in photo_url else f"product_image_{product_index + 1}.jpg"
                    
                    # Create attachment for product variant (product.product) chatter
                    try:
                        product_attachment_vals = {
                            'name': image_filename,
                            'type': 'binary',
                            'datas': image_base64,
                            'res_model': 'product.product',
                            'res_id': product_id,
                            'mimetype': 'image/jpeg'
                        }
                        
                        image_attachment_id = models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
                            'ir.attachment', 'create', [product_attachment_vals])
                        logger.info(f"Created image attachment for product.product {product_id}: {image_filename} (ID: {image_attachment_id})")
                        
                        # Post message with image to product variant chatter
                        models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
                            'product.product', 'message_post',
                            [product_id],
                            {
                                'body': f'<p>📷 Product image attached from order</p>',
                                'body_is_html': True,
                                'message_type': 'comment',
                                'subtype_xmlid': 'mail.mt_note',
                                'attachment_ids': [image_attachment_id],
                            })
                        logger.info(f"Posted image to product.product {product_id} chatter")
                    except Exception as e:
                        logger.error(f"Failed to create image attachment for product.product {product_id}: {str(e)}")
                    
                    # Create attachment for product template (product.template) chatter
                    try:
                        template_attachment_vals = {
                            'name': image_filename,
                            'type': 'binary',
                            'datas': image_base64,
                            'res_model': 'product.template',
                            'res_id': product_tmpl_id,
                            'mimetype': 'image/jpeg'
                        }
                        
                        template_attachment_id = models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
                            'ir.attachment', 'create', [template_attachment_vals])
                        logger.info(f"Created image attachment for product.template {product_tmpl_id}: {image_filename} (ID: {template_attachment_id})")
                        
                        # Post message with image to product template chatter
                        models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
                            'product.template', 'message_post',
                            [product_tmpl_id],
                            {
                                'body': f'<p>📷 Product image attached from order</p>',
                                'body_is_html': True,
                                'message_type': 'comment',
                                'subtype_xmlid': 'mail.mt_note',
                                'attachment_ids': [template_attachment_id],
                            })
                        logger.info(f"Posted image to product.template {product_tmpl_id} chatter")
                    except Exception as e:
                        logger.error(f"Failed to create image attachment for product.template {product_tmpl_id}: {str(e)}")
            
            # Get quantity (default to 1 if not specified)
            qty = product_data.get('qty', 1)
            
            # Get discount percentage (default to 0 if not specified)
            discount = product_data.get('discount', 0)
            
            # Get visible components for later appending to description
            logger.info("Getting visible components for order line description")
            visible_components = get_visible_components_list(models, uid, product_data['components'])
            
            # Add order line with correct quantity and discount (no name - let Odoo compute default)
            order_line_vals = {
                'product_id': product_id,
                'product_uom_qty': qty,
                'price_unit': product_data['price']
            }
            
            # Only add discount if it's greater than 0
            if discount > 0:
                order_line_vals['discount'] = discount
                logger.info(f"Applying {discount}% discount to order line")
            
            order_lines.append((0, 0, order_line_vals))
            
            # Track created product info for logging
            created_products.append({
                'product_id': product_id,
                'product_name': product_name,
                'product_reference': product_reference,
                'bom_id': bom_id,
                'bom_components_count': bom_components_count,
                'bom_operations_count': bom_operations_count,
                'skipped_components': skipped_components,
                'width': product_data['width'],
                'height': product_data['height'],
                'price': product_data['price'],
                'qty': qty,
                'discount': discount,
                'components': product_data['components'],
                'visible_components': visible_components,
                'photo_url': photo_url,
                'image_base64': image_base64,
                'image_attachment_id': image_attachment_id
            })
            
            logger.info(f"Product {product_index + 1} created: ID={product_id}, BOM ID={bom_id}, Qty={qty}, Discount={discount}%")

        # Create sale order with all order lines
        logger.info("Creating sale order with all order lines")
        order_vals = {
            'partner_id': partner_id,
            'order_line': order_lines
        }

        logger.debug(f"Sale order values: partner_id={partner_id}, {len(order_lines)} order line(s)")
        order_id = models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
            'sale.order', 'create', [order_vals])

        # Update order lines to append visible components to descriptions
        logger.info("Updating order lines with visible components")
        sale_order_data = models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
            'sale.order', 'read', [order_id], {'fields': ['order_line']})
        order_line_ids = sale_order_data[0]['order_line']
        
        for line_idx, order_line_id in enumerate(order_line_ids):
            if line_idx < len(created_products):
                prod = created_products[line_idx]
                visible_comps = prod.get('visible_components', [])
                
                if visible_comps:
                    # Read current description
                    line_data = models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
                        'sale.order.line', 'read', [order_line_id], {'fields': ['name']})
                    current_description = line_data[0].get('name', '')
                    
                    # Append visible components suffix
                    components_suffix = build_visible_components_suffix(visible_comps)
                    new_description = current_description + components_suffix
                    
                    # Update order line description
                    models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
                        'sale.order.line', 'write', [[order_line_id], {'name': new_description}])
                    logger.info(f"Updated order line {order_line_id} with {len(visible_comps)} visible component(s)")

        logger.info("Web order processing completed successfully")
        logger.info(f"Results - Customer ID: {partner_id}, Products: {len(created_products)}, Order ID: {order_id}")

        # Attach product images to sale order chatter
        sale_order_image_attachment_ids = []
        for prod_idx, prod in enumerate(created_products):
            if prod.get('image_base64'):
                try:
                    # Extract filename from URL or generate one
                    image_filename = prod['photo_url'].split('/')[-1] if prod.get('photo_url') and '/' in prod['photo_url'] else f"product_{prod_idx + 1}_image.jpg"
                    
                    # Create attachment linked to the sale order
                    sale_order_image_vals = {
                        'name': image_filename,
                        'type': 'binary',
                        'datas': prod['image_base64'],
                        'res_model': 'sale.order',
                        'res_id': order_id,
                        'mimetype': 'image/jpeg'
                    }
                    
                    sale_order_image_id = models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
                        'ir.attachment', 'create', [sale_order_image_vals])
                    sale_order_image_attachment_ids.append(sale_order_image_id)
                    logger.info(f"Created image attachment for sale order {order_id}: {image_filename} (ID: {sale_order_image_id})")
                except Exception as e:
                    logger.error(f"Failed to create image attachment for sale order: {str(e)}")
        
        # Post images to sale order chatter if any
        if sale_order_image_attachment_ids:
            try:
                models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
                    'sale.order', 'message_post',
                    [order_id],
                    {
                        'body': f'<p>📷 {len(sale_order_image_attachment_ids)} product image(s) attached</p>',
                        'body_is_html': True,
                        'message_type': 'comment',
                        'subtype_xmlid': 'mail.mt_note',
                        'attachment_ids': sale_order_image_attachment_ids,
                    })
                logger.info(f"Posted {len(sale_order_image_attachment_ids)} image(s) to sale order {order_id} chatter")
            except Exception as e:
                logger.error(f"Failed to post images to sale order chatter: {str(e)}")

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
        # Build product details HTML for all products
        products_html = []
        for idx, prod in enumerate(created_products, 1):
            component_list_items = []
            for i, component in enumerate(prod['components'], 1):
                component_list_items.append(f"<li>Component {i}: {component['name']} (ref: {component['reference']})</li>")
            
            # Show discount if applicable
            discount_html = f"<li>Discount: {prod['discount']}%</li>" if prod.get('discount', 0) > 0 else ""
            
            # Show photo if available
            photo_html = f"<li>Photo: <a href='{prod['photo_url']}' target='_blank'>View Image</a></li>" if prod.get('photo_url') else ""
            
            # Show skipped components if any
            skipped_html = ""
            if prod.get('skipped_components'):
                skipped_items = []
                for skipped in prod['skipped_components']:
                    skipped_items.append(f"<li>⚠️ {skipped['name']} (ref: {skipped['reference']}) - {skipped['reason']}</li>")
                skipped_html = f"""
<p><em>⚠️ Skipped Components ({len(prod['skipped_components'])}):</em></p>
<ul>{''.join(skipped_items)}</ul>
"""
            
            products_html.append(f"""
<p><strong>Product {idx}: {prod['product_name']}</strong></p>
<ul>
<li>Product ID: {prod['product_id']} (ref: {prod['product_reference']})</li>
<li>BOM ID: {prod['bom_id']}</li>
<li>Dimensions: {prod['width']}mm x {prod['height']}mm</li>
<li>Unit Price: €{prod['price']}</li>
<li>Quantity: {prod['qty']}</li>
{discount_html}
{photo_html}
<li>Surface: {prod['width'] * prod['height']} mm² ({(prod['width'] * prod['height'])/1000000:.4f} m²)</li>
<li>Circumference: {2 * (prod['width'] + prod['height'])} mm ({2 * (prod['width'] + prod['height'])/1000:.2f} m)</li>
<li>BOM components: {prod['bom_components_count']} items</li>
<li>BOM operations: {prod['bom_operations_count']} items</li>
</ul>
<p><em>Components:</em></p>
<ul>{''.join(component_list_items)}</ul>
{skipped_html}""")

        chatter_message = f"""<p><strong>🎯 {payload_type} Order Processing Completed Successfully</strong></p>

<p><em>📎 Attachments: order_payload_{timestamp}.json, order_processing_logs_{timestamp}.txt</em></p>

<p><strong>📋 Order Summary:</strong></p>
<ul>
<li>Customer ID: {partner_id}</li>
<li>Order ID: {order_id}</li>
<li>Products Created: {len(created_products)}</li>
<li>Total Order Lines: {len(order_lines)}</li>
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

<p><strong>🛍️ Products Created:</strong></p>
{''.join(products_html)}

<p><strong>Final Return Data:</strong></p>
<ul>
<li>message: '{payload_type} order processing finished'</li>
<li>partner_id: {partner_id}</li>
<li>products_created: {len(created_products)}</li>
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
            'products_created': len(created_products),
            'product_ids': [p['product_id'] for p in created_products],
            'bom_ids': [p['bom_id'] for p in created_products],
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

        # Process all order lines
        order_line_ids = sale_order[0]['order_line']
        processed_lines = []  # Track all processed line results

        # BATCH OPTIMIZATION: Pre-fetch all order lines and products in 2 calls instead of 2N calls
        logger.info(f"Batch fetching {len(order_line_ids)} order lines and their products")
        all_order_lines = models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
            'sale.order.line', 'read',
            [order_line_ids],
            {'fields': ['product_id', 'price_unit', 'product_uom_qty', 'product_updatable', 'product_template_attribute_value_ids', 'name']})
        
        # Get all product IDs and fetch in one batch call
        all_product_ids = [line['product_id'][0] for line in all_order_lines]
        all_products = models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
            'product.product', 'read',
            [all_product_ids],
            {'fields': ['x_studio_width', 'x_studio_height', 'name', 'display_name', 'default_code', 'product_tmpl_id', 'description_sale']})
        
        # Create lookup dictionaries for O(1) access in loop
        order_lines_by_id = {line['id']: line for line in all_order_lines}
        products_by_id = {prod['id']: prod for prod in all_products}
        logger.info(f"Batch fetch complete: {len(all_order_lines)} order lines, {len(all_products)} products")

        # BATCH OPTIMIZATION: Pre-fetch all BOMs in 2 calls instead of N calls in loop
        # Collect all product_ids and product_tmpl_ids
        all_variant_product_ids = list(set(all_product_ids))  # product.product IDs for variant BOMs
        all_template_ids = list(set(prod['product_tmpl_id'][0] for prod in all_products))  # product.template IDs for template BOMs
        
        # Batch search for variant BOMs (BOMs with specific product_id set)
        logger.info(f"Batch searching BOMs for {len(all_variant_product_ids)} products and {len(all_template_ids)} templates")
        variant_bom_ids = models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
            'mrp.bom', 'search',
            [[['product_id', 'in', all_variant_product_ids]]])
        
        # Batch search for template BOMs (BOMs by product_tmpl_id)
        template_bom_ids = models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
            'mrp.bom', 'search',
            [[['product_tmpl_id', 'in', all_template_ids]]])
        
        # Fetch BOM details for both sets
        all_bom_ids = list(set(variant_bom_ids + template_bom_ids))
        all_boms = []
        if all_bom_ids:
            all_boms = models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
                'mrp.bom', 'read',
                [all_bom_ids],
                {'fields': ['product_id', 'product_tmpl_id', 'bom_line_ids']})
        
        # Build lookup dicts for O(1) access
        # bom_by_product: for variant products (product_id is set)
        # bom_by_template: for non-variant products (by product_tmpl_id)
        bom_by_product = {}
        bom_by_template = {}
        for bom in all_boms:
            if bom.get('product_id') and bom['product_id']:
                bom_by_product[bom['product_id'][0]] = bom
            if bom.get('product_tmpl_id') and bom['product_tmpl_id']:
                # Only add to template dict if not already in product dict (variant takes precedence)
                tmpl_id = bom['product_tmpl_id'][0]
                if tmpl_id not in bom_by_template:
                    bom_by_template[tmpl_id] = bom
        
        logger.info(f"BOM batch fetch complete: {len(bom_by_product)} variant BOMs, {len(bom_by_template)} template BOMs")

        # PARALLEL PROCESSING: Process order lines in parallel using ThreadPoolExecutor
        # Logs are captured per-line and output sequentially after all parallel work completes
        logger.info(f"Starting parallel processing of {len(order_line_ids)} order lines")
        
        total_lines = len(order_line_ids)
        parallel_results = [None] * total_lines  # Store results in order
        
        # Use ThreadPoolExecutor for parallel processing
        # Max workers limited to avoid overwhelming Odoo with too many concurrent requests
        max_workers = min(5, total_lines)  # Limit to 5 concurrent workers
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            # Note: Each thread creates its own Odoo connection (xmlrpc is not thread-safe)
            future_to_index = {}
            for line_index, order_line_id in enumerate(order_line_ids):
                future = executor.submit(
                    process_order_line_parallel,
                    uid, line_index, total_lines, order_line_id,
                    order_lines_by_id, products_by_id, bom_by_product, bom_by_template, sale_order_id
                )
                future_to_index[future] = line_index
            
            # Collect results as they complete
            for future in concurrent.futures.as_completed(future_to_index):
                line_index = future_to_index[future]
                try:
                    result, logs = future.result()
                    parallel_results[line_index] = (result, logs)
                except Exception as e:
                    logger.error(f"Order line {line_index + 1} raised exception: {e}")
                    parallel_results[line_index] = (
                        {'order_line_id': order_line_ids[line_index], 'status': 'error', 'reason': str(e)},
                        f"Error processing order line: {e}\n"
                    )
        
        logger.info("Parallel processing complete, outputting logs in order")
        
        # OUTPUT LOGS SEQUENTIALLY: Write logs from each order line in order
        # This keeps the logs crystal clear and not mixed
        for line_index, (result, logs) in enumerate(parallel_results):
            # Output the buffered logs for this order line
            for log_line in logs.strip().split('\n'):
                if log_line:
                    logger.info(f"[Line {line_index + 1}] {log_line}")
            
            # Add result to processed_lines
            processed_lines.append(result)
            
            # Handle chatter messages for skipped lines (must be done after parallel processing)
            if result.get('status') == 'skipped' and result.get('chatter_message'):
                try:
                    models.execute_kw(ODOO_DB, uid, ODOO_API_KEY,
                        'sale.order', 'message_post',
                        [sale_order_id],
                        {'body': result['chatter_message']})
                except Exception as e:
                    logger.warning(f"Failed to post chatter message: {e}")

        # BATCH BOM COST COMPUTATION: Compute costs for all created products at once
        # This is more efficient than computing during the loop (especially with many components)
        successful_lines_for_cost = [line for line in processed_lines if line['status'] == 'success']
        if successful_lines_for_cost:
            logger.info(f"Computing BOM costs for {len(successful_lines_for_cost)} product template(s)")
            
            # Collect all template IDs for batch processing
            template_ids_to_process = [line['new_product_tmpl_id'] for line in successful_lines_for_cost]
            
            # Get initial costs in one batch call
            logger.info("Fetching initial costs for all templates")
            initial_costs_data = models.execute_kw(
                ODOO_DB, uid, ODOO_API_KEY,
                'product.template', 'read',
                [template_ids_to_process],
                {'fields': ['id', 'standard_price']}
            )
            initial_costs_by_id = {item['id']: item['standard_price'] for item in initial_costs_data}
            
            # Compute BOM cost for each template
            for tmpl_id in template_ids_to_process:
                try:
                    logger.info(f"Computing BOM cost for product template ID: {tmpl_id}")
                    models.execute_kw(
                        ODOO_DB, uid, ODOO_API_KEY,
                        'product.template', 'button_bom_cost',
                        [[tmpl_id]]
                    )
                except Exception as e:
                    # This exception is expected - the method completes successfully despite raising it
                    logger.info(f"BOM cost computation completed for Product Template ID {tmpl_id}")
            
            # Get new costs in one batch call
            logger.info("Fetching new costs for all templates")
            new_costs_data = models.execute_kw(
                ODOO_DB, uid, ODOO_API_KEY,
                'product.template', 'read',
                [template_ids_to_process],
                {'fields': ['id', 'standard_price']}
            )
            new_costs_by_id = {item['id']: item['standard_price'] for item in new_costs_data}
            
            # Update processed_lines with the computed costs
            for line in processed_lines:
                if line['status'] == 'success':
                    tmpl_id = line['new_product_tmpl_id']
                    line['initial_cost'] = initial_costs_by_id.get(tmpl_id, 0)
                    line['new_cost'] = new_costs_by_id.get(tmpl_id, 0)
                    if line['new_cost'] != line['initial_cost']:
                        logger.info(f"Template {tmpl_id}: Cost changed €{line['initial_cost']} → €{line['new_cost']}")
                    else:
                        logger.info(f"Template {tmpl_id}: No cost change (€{line['new_cost']})")

        # Trigger price update based on pricelist (once for entire order)
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

        # Calculate summary statistics
        successful_lines = [line for line in processed_lines if line['status'] == 'success']
        skipped_lines = [line for line in processed_lines if line['status'] == 'skipped']

        logger.info("Odoo order processing completed successfully")
        logger.info(f"Results - Processed {len(successful_lines)} lines, Skipped {len(skipped_lines)} lines, Updated Order ID: {sale_order_id}")

        # Generate timestamp for attachments
        final_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        # Save original payload as attachment
        attachment_ids = []
        try:
            payload_json = json.dumps(data, indent=2, ensure_ascii=False)
            attachment_name = f"odoo_order_payload_{final_timestamp}.json"

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

            log_attachment_name = f"odoo_order_processing_logs_{final_timestamp}.txt"

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

        # Build HTML sections for each processed line
        processed_lines_html = []
        for i, line in enumerate(processed_lines, 1):
            if line['status'] == 'success':
                component_list_items = []
                for j, component in enumerate(line['components'], 1):
                    component_list_items.append(f"<li>Component {j}: {component['name']} (ref: {component['reference']})</li>")

                # Show skipped components if any
                skipped_html = ""
                if line.get('skipped_components'):
                    skipped_items = []
                    for skipped in line['skipped_components']:
                        skipped_items.append(f"<li>⚠️ {skipped['name']} (ref: {skipped['reference']}) - {skipped['reason']}</li>")
                    skipped_html = f"""
<p><em>⚠️ Skipped Components ({len(line['skipped_components'])}):</em></p>
<ul>{''.join(skipped_items)}</ul>
"""

                line_html = f"""
<p><strong>📦 Line {i}: {line['original_product']}</strong></p>
<ul>
<li>Original: {line['original_product']} ({line['original_product_code']})</li>
<li>New Product: {line['new_product_name']} (ref: {line['new_product_reference']})</li>
<li>New Product ID: {line['new_product_id']}</li>
<li>New BOM ID: {line['bom_id']}</li>
<li>Dimensions: {line['width']}mm x {line['height']}mm</li>
<li>Quantity: {line['quantity']}</li>
<li>Price: €{line['price']}</li>
<li>Surface: {line['width'] * line['height']} mm² ({(line['width'] * line['height'])/1000000:.4f} m²)</li>
<li>Circumference: {2 * (line['width'] + line['height'])} mm ({2 * (line['width'] + line['height'])/1000:.2f} m)</li>
<li>BOM components: {line['bom_components_count']}, Operations: {line['bom_operations_count']}</li>
<li>Initial cost: €{line['initial_cost']}, New cost: €{line['new_cost']}</li>
</ul>
<p><em>Components:</em></p>
<ul>{''.join(component_list_items)}</ul>
{skipped_html}"""
                processed_lines_html.append(line_html)
            else:
                line_html = f"""
<p><strong>⚠️ Line {i}: {line['original_product']}</strong></p>
<ul>
<li>Status: Skipped</li>
<li>Reason: {line['reason']}</li>
</ul>
"""
                processed_lines_html.append(line_html)

        # Build summary of product IDs and BOM IDs
        product_ids = [line['new_product_id'] for line in successful_lines]
        bom_ids_list = [line['bom_id'] for line in successful_lines]

        chatter_message = f"""<p><strong>🔄 Odoo Order Processing Completed Successfully</strong></p>

<p><em>📎 Attachments: odoo_order_payload_{final_timestamp}.json, odoo_order_processing_logs_{final_timestamp}.txt</em></p>

<p><strong>📋 Order Summary:</strong></p>
<ul>
<li>Original Order ID: {sale_order_id}</li>
<li>Total Order Lines: {len(order_line_ids)}</li>
<li>Successfully Processed: {len(successful_lines)}</li>
<li>Skipped: {len(skipped_lines)}</li>
<li>New Product IDs: {product_ids}</li>
<li>New BOM IDs: {bom_ids_list}</li>
<li>Status: success</li>
</ul>

<p><strong>📝 Processing Details by Line:</strong></p>
{''.join(processed_lines_html)}

<p><strong>Order Update:</strong></p>
<ul>
<li>Updated all order lines with new products</li>
<li>Triggered price update based on pricelist</li>
</ul>

<p><strong>Final Return Data:</strong></p>
<ul>
<li>message: 'Odoo order processing finished'</li>
<li>processed_lines: {len(successful_lines)}</li>
<li>skipped_lines: {len(skipped_lines)}</li>
<li>product_ids: {product_ids}</li>
<li>bom_ids: {bom_ids_list}</li>
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
            'processed_lines': len(successful_lines),
            'skipped_lines': len(skipped_lines),
            'product_ids': product_ids,
            'bom_ids': bom_ids_list,
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


