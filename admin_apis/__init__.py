import psycopg2
from flask import jsonify, request
from app import app
from app import handle_exceptions

from settings import set_connection, setup_logger


from psycopg2.extras import execute_values
from werkzeug.security import generate_password_hash

logger = setup_logger('__name__', 'app.log')


@app.route('/app/v1/products/create_product', methods=['POST'])
@handle_exceptions
def create_product():
    data = request.json
    product_name = data.get('product_name')
    sku = data.get('sku')
    description = data.get('description')
    price = data.get('price')
    discount_id = data.get('discount_id')
    capacity = data.get('capacity')
    units = data.get('units')
    available_qty = data.get('available_qty')
    featured = data.get('featured')
    is_active = data.get('is_active')
    vendor_id = data.get('vendor_id')
    in_order = data.get('in_order')
    image_urls = data.get('image_urls')
    tags = data.get('tags')

    cur, conn = set_connection()
    cur.execute("""INSERT INTO Products (product_name, sku, description, price, discount_id, capacity, units,
                   available_qty, featured, is_active, vendor_id, in_order, image_urls, tags)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (product_name, sku, description, price, discount_id, capacity, units, available_qty, featured,
                 is_active, vendor_id, in_order, image_urls, tags))
    conn.commit()
    logger.debug(f"Created product {product_name} with ID {cur.lastrowid}")
    return jsonify({'product_id': cur.lastrowid}), 201


@app.route('/app/v1/products/update_product', methods=['PUT'])
@handle_exceptions
def update_product():
    data = request.json
    product_id = data.get('product_id')
    product_name = data.get('product_name')
    sku = data.get('sku')
    description = data.get('description')
    price = data.get('price')
    discount_id = data.get('discount_id')
    capacity = data.get('capacity')
    units = data.get('units')
    available_qty = data.get('available_qty')
    featured = data.get('featured')
    is_active = data.get('is_active')
    vendor_id = data.get('vendor_id')
    in_order = data.get('in_order')
    image_urls = data.get('image_urls')
    tags = data.get('tags')

    cur, conn = set_connection()
    cur.execute('SELECT * FROM Products WHERE product_id = %s;', (product_id,))
    row = cur.fetchone()
    if not row:
        return jsonify({'error': f'Product with ID {product_id} does not exist'}), 404
    cur.execute("""UPDATE Products SET product_name = %s, sku = %s, description = %s, price = %s,
                   discount_id = %s, capacity = %s, units = %s, available_qty = %s, featured = %s, 
                   is_active = %s, vendor_id = %s, in_order = %s, image_urls = %s, tags = %s,
                   updated_at = NOW() WHERE product_id = %s;""",
                (product_name, sku, description, price, discount_id, capacity, units, available_qty, featured,
                 is_active, vendor_id, in_order, image_urls, tags, product_id))
    conn.commit()
    logger.debug(f"Updated product with ID {product_id}")
    return 'Updated product successfully', 200
