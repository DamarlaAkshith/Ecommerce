import psycopg2
from flask import Flask, jsonify, request
from werkzeug.security import generate_password_hash

from settings import set_connection, setup_logger
from psycopg2.extras import execute_values

app = Flask(__name__)


def handle_exceptions(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except psycopg2.Error as e:
            conn = kwargs.get('conn')
            if conn:
                conn.rollback()
            logger.error(str(e))
            return jsonify({"error": "Database error"})
        except Exception as e:
            logger.error(str(e))
            return jsonify({"error": "Internal server error"})
        finally:
            conn = kwargs.get('conn')
            cur = kwargs.get('cur')
            if cur:
                cur.close()
            if conn:
                conn.close()

    return wrapper


@app.route('/app/v1/filters/update_filter', methods=['PUT'])
@handle_exceptions
def update_filter():
    data = request.get_json()
    filter_id = data.get('filter_id')
    filter_name = data.get('filter_name')
    category_name = data.get('category_name')
    filter_type = data.get('filter_type')
    options = data.get('options')
    if not all([filter_id, filter_name, filter_type]):
        return jsonify({'error': 'Missing required fields'}), 400
    cur, conn = set_connection()
    cur.execute('SELECT * FROM Filter WHERE filter_id = %s;', (filter_id,))
    row = cur.fetchone()
    if not row:
        return jsonify({'error': f'Filter with ID {filter_id} does not exist'}), 404
    # Get category_id for the given category_name
    cur.execute('SELECT category_id FROM Category WHERE category_name = %s', (category_name,))
    category_row = cur.fetchone()
    if not category_row:
        return jsonify({'error': f'Category with name {category_name} does not exist'}), 404
    category_id = category_row[0]
    cur.execute(
        'UPDATE Filter SET filter_name=%s, filter_type=%s, category_id=%s WHERE filter_id=%s;',
        (filter_name, filter_type, category_id, filter_id))
    cur.execute('DELETE FROM FilterOption WHERE filter_id=%s;', (filter_id,))
    cur.executemany('INSERT INTO FilterOption(filter_id, option_value) VALUES(%s, %s);',
                    [(filter_id, option) for option in options])
    conn.commit()
    cur.execute('SELECT Filter.filter_id, filter_name, category_id, category_name, filter_type, '
                'array_agg(option_value) FROM Filter '
                'JOIN Category ON Filter.category_id = Category.category_id '
                'LEFT JOIN FilterOption ON Filter.filter_id = FilterOption.filter_id '
                'WHERE Filter.filter_id = %s GROUP BY Filter.filter_id, filter_name, category_id, category_name, filter_type;',
                (filter_id,))
    row = cur.fetchone()
    # if not row:
    #

    filter_data = {
        'filter_id': row[0],
        'filter_name': row[1],
        'category_id': row[2],
        'category_name': row[3],
        'filter_type': row[4],
        'options': row[5]
    }
    logger.debug(f"Updated filter with ID {filter_id}")
    return jsonify({'filter': filter_data}), 200


@app.route('/app/v1/products/filter_products', methods=['GET'])
@handle_exceptions
def filter_products():
    data = request.get_json()
    category_id = data.get('category')
    filter_options = data.get('filter_options')
    cur, conn = set_connection()

    # Construct the SQL query
    sql = "select p.* from products p inner join product_category pc on p.product_id = pc.product_id where pc.category_id = %s "
    params = [category_id]

    for option_value in filter_options:
        cur.execute(
            "select fo.option_id, fo.filter_id from filteroption fo inner join filter_category fc on fo.filter_id = fc.filter_id where fo.option_value = %s and fc.category_id = %s ",
            (option_value, category_id))
        row = cur.fetchone()
        if row is not None:
            option_id = row[0]
            filter_id = row[1]
            sql += "and exists (select 1 from filteroption fo inner join filter_category fc on fo.filter_id = fc.filter_id where fo.option_id = %s and fc.category_id = %s and fo.filter_id = %s) "
            params.append(option_id)
            params.append(category_id)
            params.append(filter_id)

    cur.execute(sql, params)

    # Construct the response as a JSON array
    products = []
    for row in cur:
        product = {
            'id': row[0],
            'name': row[1],
            'description': row[3],
            'price': str(row[4]),
            'featured': row[9]
        }
        products.append(product)

    conn.close()

    logger.debug(
        f"Retrieved {len(products)} products from the database with filter options {filter_options} for category {category_id}")
    return jsonify(products)


# API endpoint for getting all products

@app.route('/app/v1/products/get_products', methods=['GET'])
@handle_exceptions
def get_products():
    cur, conn = set_connection()
    cur.execute('select product_id, product_name, description, price, image_urls from products;')
    rows = cur.fetchall()
    products = []
    for row in rows:
        product = {
            'product_id': row[0],
            'product_name': row[1],
            'description': row[2],
            'price': row[3],
            'image_urls': row[4]
        }
        products.append(product)

    logger.debug(f"Retrieved {len(products)} products from the database")
    return jsonify({'products': products})


@app.route('/app/v1/products/get_product', methods=['GET'])
@handle_exceptions
def get_product():
    product_id = request.json.get('product_id')
    cur, conn = set_connection()
    cur.execute('select product_id, product_name, description, price, image_urls from products WHERE id = %s;',
                (product_id,))
    row = cur.fetchone()
    product = {
        'id': row[0],
        'name': row[1],
        'description': row[2],
        'price': row[3],
        'image_urls': row[4]
    }

    logger.debug(f"Retrieved product with id {product_id} from the database")
    return jsonify({'product': product})


@app.route('/app/v1/products/search_products', methods=['GET'])
@handle_exceptions
def search_products():
    query = request.json.get('query')
    cur, conn = set_connection()
    # Construct the SQL query
    sql = "select product_id, product_name, description, price, image_urls from products where product_name ilike %s or description ilike %s;"
    params = [f"%{query}%", f"%{query}%"]
    cur.execute(sql, params)
    # Construct the response as a JSON array
    products = []
    for row in cur:
        product = {
            'product_id': row[0],
            'product_name': row[1],
            'description': row[2],
            'price': row[3],
            'image_urls': row[4]
        }
        products.append(product)

    conn.close()

    # Log the number of matching products
    logger.debug(f"Found {len(products)} products matching query '{query}'")

    return jsonify({'products': products})


@app.route('/app/v1/products/get_featured_products', methods=['GET'])
@handle_exceptions
def get_featured_products():
    cur, conn = set_connection()
    cur.execute('select product_id, product_name, description, price, image_urls from products where featured = true;')
    rows = cur.fetchall()
    products = []
    for row in rows:
        product = {
            'product_id': row[0],
            'product_name': row[1],
            'description': row[2],
            'price': row[3],
            'image_urls': row[4]
        }
        products.append(product)

    logger.debug(f"Retrieved {len(products)} featured products from the database")
    return jsonify({'products': products})


# API endpoint for getting all filters
@app.route('/app/v1/filters/get_filters', methods=['GET'])
@handle_exceptions
def get_filters():
    cur, conn = set_connection()
    cur.execute('SELECT filter_id, filter_name, category_id, filter_type FROM Filter;')
    rows = cur.fetchall()
    filters = []
    for row in rows:
        filter_id, filter_name, category_id, filter_type = row
        cur.execute('SELECT option_id, option_value FROM FilterOption WHERE filter_id = %s', (filter_id,))
        option_rows = cur.fetchall()
        options = [{'option_id': option_row[0], 'option_value': option_row[1]} for option_row in option_rows]
        fil = {'filter_id': filter_id, 'filter_name': filter_name, 'category_id': category_id,
               'filter_type': filter_type, 'options': options}
        filters.append(fil)

    logger.debug(f"Retrieved {len(filters)} filters from the database")
    return jsonify({'filters': filters})


# API endpoint for getting a specific filter by id
@app.route('/app/v1/filters/get_filter', methods=['GET'])
@handle_exceptions
def get_filter():
    filter_id = request.json.get('filter_id')
    cur, conn = set_connection()
    cur.execute('SELECT filter_id, filter_name, category_id, filter_type FROM Filter WHERE filter_id = %s;',
                (filter_id,))
    row = cur.fetchone()
    if row is None:
        return jsonify({'error': f"No filter found with id {filter_id}"}), 404
    filter_id, filter_name, category_id, filter_type = row
    cur.execute('SELECT option_id, option_value FROM FilterOption WHERE filter_id = %s', (filter_id,))
    option_rows = cur.fetchall()
    options = [{'option_id': option_row[0], 'option_value': option_row[1]} for option_row in option_rows]
    fil = {'filter_id': filter_id, 'filter_name': filter_name, 'category_id': category_id, 'filter_type': filter_type,
           'options': options}

    logger.debug(f"Retrieved filter {filter_id} from the database")
    return jsonify({'filter': fil})


@app.route('/app/v1/filters/create_filter', methods=['POST'])
@handle_exceptions
def create_filter():
    filter_name = request.json.get('filter_name')
    category_name = request.json.get('category_name')
    filter_type = request.json.get('filter_type')
    filter_options = request.json.get('filter_options', [])

    cur, conn = set_connection()

    # Get category_id for the given category_name
    cur.execute('SELECT category_id FROM Category WHERE category_name = %s', (category_name,))
    category_row = cur.fetchone()
    if not category_row:
        return jsonify({'error': f'Category with name {category_name} does not exist'}), 404
    category_id = category_row[0]

    # Insert new filter into the Filter table
    cur.execute('INSERT INTO Filter (filter_name, category_id, filter_type) VALUES (%s, %s, %s) RETURNING filter_id',
                (filter_name, category_id, filter_type))
    filter_id = cur.fetchone()[0]

    # Insert filter options into FilterOption table
    # for option_value in filter_options:
    #     cur.execute('INSERT INTO FilterOption (filter_id, option_value) VALUES (%s, %s)', (filter_id, option_value))

    # Construct the SQL query
    sql = 'INSERT INTO FilterOption (filter_id, option_value) VALUES %s'

    # Use execute_values to insert multiple rows at once
    execute_values(cur, sql, filter_options)

    conn.commit()

    logger.debug(f"Created filter with ID {filter_id}")
    return jsonify({'filter_id': filter_id}), 201


# API endpoint for deleting an existing filter
@app.route('/app/v1/filters/delete_filter', methods=['DELETE'])
@handle_exceptions
def delete_filter():
    filter_id = request.json.get('filter_id')
    if not filter_id:
        return jsonify({'error': 'Missing filter_id field'}), 400
    cur, conn = set_connection()
    cur.execute('SELECT * FROM Filter WHERE filter_id = %s;', (filter_id,))
    row = cur.fetchone()
    if not row:
        return jsonify({'error': f'Filter with ID {filter_id} does not exist'}), 404
    cur.execute('DELETE FROM Filter WHERE filter_id=%s;', (filter_id,))
    conn.commit()
    logger.debug(f"Deleted filter with ID {filter_id}")
    return 'Deleted filter successfully', 204


# API endpoint for getting a list of all categories
@app.route('/app/v1/categories/get_categories')
def get_categories():
    cur, conn = set_connection()
    cur.execute(
        'SELECT category_id,name,description,parent_category_id FROM Category WHERE deleted_at IS NULL ORDER BY category_id;')
    rows = cur.fetchall()
    categories = []
    for row in rows:
        category = {
            'category_id': row[0],
            'name': row[1],
            'description': row[2],
            'parent_category_id': row[3]
            # 'created_at': row[4].strftime('%Y-%m-%d %H:%M:%S'),
            # 'updated_at': row[5].strftime('%Y-%m-%d %H:%M:%S')
        }
        categories.append(category)
    logger.debug(f"Retrieved {len(categories)} categories")
    return jsonify(categories), 200


# API endpoint for getting details of a specific category
@app.route('/app/v1/categories/get_category')
def get_category():
    category_name = request.json.get("category_name")
    cur, conn = set_connection()
    cur.execute('SELECT category_id FROM Category WHERE category_name=%s AND deleted_at IS NULL;', (category_name,))
    category_id = cur.fetchone()
    if not category_id:
        logger.debug(f"Category with name {category_name} not found")
        return jsonify({'error': f'Category with name {category_name} not found'}), 404

    cur.execute('SELECT * FROM Category WHERE category_id=%s AND deleted_at IS NULL;', (category_id,))
    row = cur.fetchone()
    if not row:
        logger.debug(f"Category with ID {category_id} not found")
        return jsonify({'error': f'Category with ID {category_id} not found'}), 404
    category = {
        'category_id': row[0],
        'name': row[1],
        'description': row[2],
        'parent_category_id': row[3],
        # 'created_at': row[4].strftime('%Y-%m-%d %H:%M:%S'),
        # 'updated_at': row[5].strftime('%Y-%m-%d %H:%M:%S')
    }
    logger.debug(f"Retrieved category with ID {category_id}")
    return jsonify(category), 200


# API endpoint for creating a new category
@app.route('/app/v1/categories/create_category', methods=['POST'])
def create_category():
    name = request.json.get('name')
    description = request.json.get('description')
    parent_category_id = request.json.get('parent_category_id')
    if not name or not description:
        logger.debug("Missing  field in request")
        return jsonify({'error': 'Missing field'}), 400
    cur, conn = set_connection()
    cur.execute(
        'INSERT INTO Category (name, description, parent_category_id) VALUES (%s, %s, %s) RETURNING category_id;',
        (name, description, parent_category_id))
    category_id = cur.fetchone()[0]
    conn.commit()
    logger.debug(f"Created category with ID {category_id}")
    return jsonify({'category_id': category_id}), 201


# API endpoint for updating an existing category
@app.route('/app/v1/categories/update_category', methods=['PUT'])
@handle_exceptions
def update_category():
    data = request.json
    category_name = data.get('category_name')
    description = data.get('description')
    parent_category_id = data.get('parent_category_id')

    cur, conn = set_connection()
    cur.execute('SELECT category_id FROM Category WHERE category_name = %s;', (category_name,))
    category_id = cur.fetchone();
    # cur.execute('SELECT * FROM Category WHERE category_id = %s;', (category_id,))
    # row = cur.fetchone()
    if not category_id:
        return jsonify({'error': f'Category with ID {category_name} does not exist'}), 404

    cur.execute("""UPDATE Category SET category_name = %s,description = %s,parent_category_id = %s,updated_at = 
    NOW() WHERE category_id = %s;""", (category_name, description, parent_category_id, category_id))
    conn.commit()
    logger.debug(f"Updated category with ID {category_id}")
    return 'Updated category successfully', 200


# API endpoint for deleting a category
@app.route('/app/v1/categories/delete_category', methods=['DELETE'])
@handle_exceptions
def delete_category():
    category_name = request.get_json("category_name")
    cur, conn = set_connection()
    cur.execute('SELECT category_id FROM Category WHERE category_name = %s;', (category_name,))
    category_id = cur.fetchone();
    # cur.execute('SELECT * FROM Category WHERE category_id = %s;', (category_id,))
    # row = cur.fetchone()
    if not category_id:
        return jsonify({'error': f'Category with ID {category_name} does not exist'}), 404
    # cur.execute('SELECT * FROM Category WHERE category_id = %s;', (category_id,))
    # row = cur.fetchone()
    cur.execute("UPDATE Category SET deleted_at = NOW() WHERE category_id = %s", (category_id,))
    conn.commit()
    logger.debug(f"Deleted category with ID {category_id}")
    return 'Deleted category successfully', 204


@app.route('/app/v1/customers/get_customers', methods=['GET'])
@handle_exceptions
def get_customers():
    cur, conn = set_connection()
    cur.execute(
        'SELECT customer_id, customer_fname, customer_lname, email, phone_number, address, points_balance, points_redeemed FROM Customer;')
    rows = cur.fetchall()
    customers = []
    for row in rows:
        customer = {
            'customer_id': row[0],
            'customer_fname': row[1],
            'customer_lname': row[2],
            'email': row[3],
            'phone_number': row[4],
            'address': row[5],
            'points_balance': row[6],
            'points_redeemed': row[7]
        }
        customers.append(customer)
    logger.debug(f"Retrieved {len(customers)} customers from the database")
    return jsonify(customers), 200


@app.route('/app/v1/customers/get_customer', methods=['GET'])
@handle_exceptions
def get_customer():
    customer_name = request.json.get("customer_name")
    cur, conn = set_connection()
    cur.execute('SELECT customer_id FROM Customer WHERE customer_name = %s;', (customer_name,))
    customer_id = cur.fetchone();
    if not customer_id:
        return jsonify({'error': f'Customer with name {customer_name} does not exist'}), 404
    cur, conn = set_connection()
    cur.execute(
        'SELECT customer_id, customer_fname, customer_lname, email, phone_number, address, points_balance, points_redeemed  FROM Customer WHERE customer_id = %s;',
        (customer_id,))
    row = cur.fetchone()

    customer = {
        'customer_id': row[0],
        'customer_fname': row[1],
        'customer_lname': row[2],
        'email': row[3],
        'phone_number': row[4],
        'address': row[5],
        'points_balance': row[6],
        'points_redeemed': row[7]
    }
    logger.debug(f"Retrieved customer with ID {customer_id}")
    return jsonify(customer), 200


@app.route('/app/v1/customers/create_customer', methods=['POST'])
@handle_exceptions
def create_customer():
    data = request.json
    customer_fname = data.get('customer_fname')
    customer_lname = data.get('customer_lname')
    email = data.get('email')
    password = data.get('password')
    phone_number = data.get('phone_number')
    address = data.get('address')

    # Check if email already exists
    cur, conn = set_connection()
    cur.execute('SELECT customer_id FROM Customer WHERE email = %s;', (email,))
    result = cur.fetchone()
    if result is not None:
        return jsonify({'error': f'Email {email} already exists'}), 400

    # Hash the password
    hashed_password = generate_password_hash(password)

    # Insert the new customer into the database
    cur.execute("""INSERT INTO Customer (customer_fname, customer_lname, email, password, phone_number, address)
                    VALUES (%s, %s, %s, %s, %s, %s);""",
                (customer_fname, customer_lname, email, hashed_password, phone_number, address))
    conn.commit()
    logger.debug(f"Created customer with email {email}")
    return jsonify({'message': 'Customer created successfully'}), 201


@app.route('/app/v1/customers/update_customer', methods=['PUT'])
@handle_exceptions
def update_customer(customer_id):
    data = request.json
    customer_id = data.get('customer_id')
    customer_fname = data.get('customer_fname')
    customer_lname = data.get('customer_lname')
    email = data.get('email')
    password = data.get('password')
    phone_number = data.get('phone_number')
    address = data.get('address')

    # Check if the customer exists
    cur, conn = set_connection()
    cur.execute('SELECT * FROM Customer WHERE customer_id = %s;', (customer_id,))
    result = cur.fetchone()
    if result is None:
        return jsonify({'error': f'Customer with ID {customer_id} does not exist'}), 404

    # Check if email is already taken by another customer
    cur.execute('SELECT customer_id FROM Customer WHERE email = %s AND customer_id != %s;', (email, customer_id))
    result = cur.fetchone()
    if result is not None:
        return jsonify({'error': f'Email {email} is already taken by another customer'}), 400

    # Hash the password if provided
    if password:
        hashed_password = generate_password_hash(password)
    else:
        hashed_password = result['password']

    # Update the customer in the database
    cur.execute("""UPDATE Customer SET customer_fname = %s, customer_lname = %s, email = %s, password = %s,
                    phone_number = %s, address = %s, updated_at = NOW() WHERE customer_id = %s;""",
                (customer_fname, customer_lname, email, hashed_password, phone_number, address, customer_id))
    conn.commit()
    logger.debug(f"Updated customer with ID {customer_id}")
    return jsonify({'message': 'Customer updated successfully'}), 200


@app.route('/app/v1/customers/delete_customer', methods=['DELETE'])
# @jwt_required
@handle_exceptions
def delete_customer():
    customer_id = request.json.get('customer_id')
    cur, conn = set_connection()
    cur.execute('SELECT * FROM Customer WHERE customer_id = %s AND deleted_at IS NULL;', (customer_id,))
    row = cur.fetchone()
    if not row:
        return jsonify({'error': f'Customer with ID {customer_id} does not exist'}), 404

    cur.execute('UPDATE Customer SET deleted_at = NOW() WHERE customer_id = %s;', (customer_id,))
    conn.commit()
    logger.debug(f"Deleted customer with ID {customer_id}")
    return 'Deleted customer successfully', 200


if __name__ == '__main__':
    app.run(debug=True)
