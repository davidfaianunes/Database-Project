#!/usr/bin/python3
from logging.config import dictConfig

import psycopg
from flask import flash
from flask import Flask
from flask import jsonify
from flask import redirect
from flask import render_template
from flask import request
from flask import url_for
from psycopg.rows import namedtuple_row
from psycopg_pool import ConnectionPool


# postgres://{user}:{password}@{hostname}:{port}/{database-name}
DATABASE_URL = "postgres://db:db@postgres/db"

pool = ConnectionPool(conninfo=DATABASE_URL)
# the pool starts connecting immediately.

dictConfig(
    {
        "version": 1,
        "formatters": {
            "default": {
                "format": "[%(asctime)s] %(levelname)s in %(module)s:%(lineno)s - %(funcName)20s(): %(message)s",
            }
        },
        "handlers": {
            "wsgi": {
                "class": "logging.StreamHandler",
                "stream": "ext://flask.logging.wsgi_errors_stream",
                "formatter": "default",
            }
        },
        "root": {"level": "INFO", "handlers": ["wsgi"]},
    }
)

app = Flask(__name__)
log = app.logger

def execute_query(query, params=None):
    with pool.connection() as conn:
        conn.autocommit = False
        with conn.cursor(row_factory=namedtuple_row) as cursor:
            cursor.execute(query, params)
            try:
                result = cursor.fetchall()
            except:
                result = None
        conn.commit()
    return result

def execute_multiple_queries(query, params=[]):
    with pool.connection() as conn:
        conn.autocommit = False
        with conn.cursor(row_factory=namedtuple_row) as cursor:
            statements = query.split(";")
            for i in range(len(statements)):
                if statements[i].strip():
                    cursor.execute(statements[i], params[i] if i < len(params) else None)
            try:
                result = cursor.fetchall()
            except:
                result = None
        conn.commit()
    return result

@app.route("/", methods=("GET",))
@app.route("/home", methods=("GET",))
def home():
    try:
        return render_template("index.html")
    except Exception as e:
        return render_template("error.html", error=e)

@app.route("/customers", methods=("GET",))
def customers():
    try:
        page = request.args.get('page', default=1, type=int)
        items_per_page = 5
        offset = (page - 1) * items_per_page
        query = "SELECT * FROM customer ORDER BY cust_no LIMIT %s OFFSET %s"
        customer_list = execute_query(query, (items_per_page, offset))
        count_query = "SELECT COUNT(*) FROM customer"
        total_count = execute_query(count_query)[0][0]
        total_pages = (total_count // items_per_page) + (total_count % items_per_page > 0)
        return render_template("customer.html", customers=customer_list, total_pages=total_pages, current_page=page)
    except Exception as e:
        return render_template("error.html", error=e)
    
@app.route("/customer/register", methods=("POST", "GET"))
def register_customer():
    try:
        max_num = execute_query("SELECT MAX(cust_no) FROM customer")
        if max_num[0][0] is not None:
            cust_no = max_num[0][0] + 1
        else:
            cust_no = 1
        name = request.form.get("name")
        email = request.form.get("email")
        address = request.form.get("address")
        phone = request.form.get("phone")
        execute_query("INSERT INTO customer (cust_no, name, email, address, phone) VALUES (%s, %s, %s, %s, %s)", (cust_no, name, email, address, phone))
    except Exception as e:
        return render_template("error.html", error=e)
    return redirect(url_for("customers"))

@app.route("/customer/remove/<int:customer_id>", methods=("POST", "GET"))
def remove_customer(customer_id):
    try:
        orders = execute_query("SELECT order_no FROM orders WHERE cust_no = %s", (customer_id,))
        n_orders = len(orders)
        for i in range(n_orders):
            order_no = orders[i][0]
            execute_multiple_queries("""
                BEGIN;
                DELETE FROM contains WHERE order_no = %s;
                DELETE FROM process WHERE order_no = %s;
                DELETE FROM pay WHERE order_no = %s;
                DELETE FROM orders WHERE order_no = %s;
                COMMIT;
            """, ((), (order_no,),  (order_no,), (order_no,), (order_no,), ()))
        execute_query("DELETE FROM customer WHERE cust_no = %s", (customer_id,))

    except Exception as e:
        execute_query("ROLLBACK;")
        return render_template("error.html", error=e)
    return redirect(url_for("customers"))

@app.route("/suppliers", methods=("GET",))
def suppliers():
    try:
        page = request.args.get('page', default=1, type=int)
        items_per_page = 5
        offset = (page - 1) * items_per_page
        query = "SELECT * FROM supplier ORDER BY TIN LIMIT %s OFFSET %s"
        supplier_list = execute_query(query, (items_per_page, offset))
        count_query = "SELECT COUNT(*) FROM supplier"
        total_count = execute_query(count_query)[0][0]
        total_pages = (total_count // items_per_page) + (total_count % items_per_page > 0)
    except Exception as e:
        return render_template("error.html", error=e)
    return render_template("supplier.html", suppliers=supplier_list, total_pages=total_pages, current_page=page)
    
@app.route("/supplier/register", methods=("POST", "GET"))
def register_supplier():
    try:
        tin = request.form.get("tin")
        name = request.form.get("name")
        address = request.form.get("address")
        sku = request.form.get("sku")
        date = request.form.get("date")
        execute_query("INSERT INTO supplier (TIN, name, address, SKU, date) VALUES (%s, %s, %s, %s, %s)", (tin, name, address, sku, date))
    except Exception as e:
        return render_template("error.html", error=e)
    return redirect(url_for("suppliers"))

@app.route("/supplier/remove/<string:tin>", methods=("POST", "GET"))
def remove_supplier(tin):
    try:
        execute_multiple_queries("""
            BEGIN;
            DELETE FROM delivery WHERE TIN = %s;
            DELETE FROM supplier WHERE TIN = %s;
            COMMIT;
            """, ((), (tin,), (tin,), ()))
    except Exception as e:
        return render_template("error.html", error=e)
    return redirect(url_for("suppliers"))

@app.route("/products", methods=("GET", "POST"))
def products():
    try:
        page = request.args.get('page', default=1, type=int)
        items_per_page = 5
        offset = (page - 1) * items_per_page
        query = "SELECT * FROM product ORDER BY SKU LIMIT %s OFFSET %s"
        product_list = execute_query(query, (items_per_page, offset))
        count_query = "SELECT COUNT(*) FROM product"
        total_count = execute_query(count_query)[0][0]
        total_pages = (total_count // items_per_page) + (total_count % items_per_page > 0)
    except Exception as e:
        return render_template("error.html", error=e)
    return render_template("product.html", products=product_list, total_pages=total_pages, current_page=page)
    
@app.route("/product/register", methods=("POST", "GET"))
def register_product():
    try:
        sku = request.form.get("sku")
        name = request.form.get("name")
        description = request.form.get("description")
        price = request.form.get("price")
        ean = request.form.get("ean")
        if ean == "":
            ean = None
        execute_query("INSERT INTO product (SKU, name, description, price, ean) VALUES (%s, %s, %s, %s, %s)", (sku, name, description, price, ean))
    except Exception as e:
        return render_template("error.html", error=e)
    return redirect(url_for("products"))

@app.route("/product/remove/<string:sku>", methods=("POST", "GET"))
def remove_product(sku):
    try:
        order_nos = execute_query("SELECT order_no FROM contains WHERE SKU = %s", (sku,))
        for order_no in order_nos:
            execute_query("DELETE FROM process WHERE order_no = %s", (order_no[0],))
            execute_query("DELETE FROM pay WHERE order_no = %s", (order_no[0],))
            try:
                execute_query("DELETE FROM contains WHERE order_no = %s", (order_no[0],))
                quantity_result = execute_query("SELECT SUM(qty) FROM contains WHERE order_no = %s", (order_no[0],))
                if quantity_result[0][0] is None:
                    execute_query("DELETE FROM orders WHERE order_no = %s", (order_no[0],))
                elif quantity_result[0][0] == 0:
                    execute_multiple_queries("""
                        BEGIN;
                        DELETE FROM contains WHERE order_no = %s;
                        DELETE FROM orders WHERE order_no = %s;
                        COMMIT;
                        """, ((), (order_no[0],), (order_no[0],), ()))
            except:
                execute_multiple_queries("""
                    BEGIN;
                    DELETE FROM contains WHERE order_no = %s;
                    DELETE FROM orders WHERE order_no = %s;
                    DELETE FROM process WHERE order_no = %s;
                    COMMIT;
                    """, ((), (order_no[0],), (order_no[0],), (order_no[0]), ()))
        tin = execute_query("SELECT TIN FROM supplier WHERE SKU = %s", (sku,))
        if tin:
            execute_multiple_queries("""
                BEGIN;
                DELETE FROM delivery WHERE TIN = %s;
                DELETE FROM supplier WHERE SKU = %s;
                DELETE FROM product WHERE SKU = %s;
                COMMIT;
                """, ((), (tin[0][0],), (sku,), (sku,), ()))
        else:
            execute_multiple_queries("""
                BEGIN;
                DELETE FROM supplier WHERE SKU = %s;
                DELETE FROM product WHERE SKU = %s;
                COMMIT;
                """, ((), (sku,), (sku,), ()))

    except Exception as e:
        execute_query("ROLLBACK;")
        return render_template("error.html", error=e)
    return redirect(url_for("products"))

@app.route("/product/edit/<string:sku>", methods=("GET", "POST"))
def edit_product(sku):
    try:
        description = request.form.get("description")
        price = request.form.get("price")
        execute_query("UPDATE product SET description = %s, price = %s WHERE SKU = %s", (description, price, sku))
    except Exception as e:
        return render_template("error.html", error=e)
    return redirect(url_for("products"))

@app.route("/login", methods=("POST", "GET"))
def login():
    try:
        page = request.args.get('page', default=1, type=int)
        items_per_page = 5
        offset = (page - 1) * items_per_page
        query = "SELECT * FROM customer ORDER BY cust_no LIMIT %s OFFSET %s"
        customer_list = execute_query(query, (items_per_page, offset))
        count_query = "SELECT COUNT(*) FROM orders"
        total_count = execute_query(count_query)[0][0]
        total_pages = (total_count // items_per_page) + (total_count % items_per_page > 0)
    except Exception as e:
        return render_template("error.html", error=e)
    return render_template("login.html", customers=customer_list, total_pages=total_pages, current_page=page)

@app.route("/orders", methods=("GET",))
def orders():
    try:
        page = request.args.get('page', default=1, type=int)
        items_per_page = 5
        offset = (page - 1) * items_per_page
        query = "SELECT * FROM orders ORDER BY order_no LIMIT %s OFFSET %s"
        order_list = execute_query(query, (items_per_page, offset))
        count_query = "SELECT COUNT(*) FROM orders"
        total_count = execute_query(count_query)[0][0]
        total_pages = (total_count // items_per_page) + (total_count % items_per_page > 0)
        total_price, num_products = [], []
        if order_list is None:
            order_list = []
        for order in order_list:
            price_query = "SELECT SUM(price * qty) FROM contains NATURAL JOIN product WHERE order_no = %s"
            price_result = execute_query(price_query, (order[0],))
            total_price += [price_result[0][0] if price_result[0][0] is not None else 0]
            
            quantity_query = "SELECT SUM(qty) FROM contains WHERE order_no = %s"
            quantity_result = execute_query(quantity_query, (order[0],))
            num_products += [quantity_result[0][0] if quantity_result[0][0] is not None else 0]
            
    except Exception as e:
        return render_template("error.html", error=e)

    total_price = total_price or []  # Ensure total_price is a list even if no results were found
    num_products = num_products or []  # Ensure num_products is a list even if no results were found

    return render_template("order.html", customer_number=None, orders=order_list, total_price=total_price, num_products=num_products, total_pages=total_pages, current_page=page)

@app.route("/orders/customer=<string:cust_no>", methods=("GET",))
def order(cust_no):
    try:
        page = request.args.get('page', default=1, type=int)
        items_per_page = 5
        offset = (page - 1) * items_per_page
        query = "SELECT * FROM orders WHERE cust_no = %s ORDER BY order_no LIMIT %s OFFSET %s"
        order_list = execute_query(query, (cust_no, items_per_page, offset))
        count_query = "SELECT COUNT(*) FROM orders WHERE cust_no = %s"
        total_count = execute_query(count_query, (cust_no,))[0][0]
        total_pages = (total_count // items_per_page) + (total_count % items_per_page > 0)
        payed_orders = execute_query("SELECT order_no FROM pay WHERE cust_no = %s", (cust_no,))
        if payed_orders:
            payed_orders = [order[0] for order in payed_orders]
        else:
            payed_orders = []
        total_price, num_products = [], []
        
        for order in order_list:
            total_price += [execute_query("SELECT SUM(price * qty) FROM contains NATURAL JOIN product WHERE order_no = %s", (order[0],))[0][0]]
            num_products += [execute_query("SELECT SUM(qty) FROM contains WHERE order_no = %s", (order[0],))[0][0]]
    except Exception as e:
        return render_template("error.html", error=e)
    return render_template("order.html", customer_number=cust_no, payed_orders=payed_orders, orders=order_list, total_price=total_price, num_products=num_products, total_pages=total_pages, current_page=page)

@app.route("/order/register", methods=("POST", "GET"))
def register_order():
    try:        
        first = True
        n_products = int(request.form.get("numberProducts"))
        order_no = request.form.get("order_no")
        cust_no = request.form.get("cust_no")
        date = request.form.get("date")
        for i in range(1, n_products+1):
            sku = request.form.get("sku_"+str(i))
            qty = request.form.get("qty_"+str(i))
            if first:
                execute_multiple_queries("""
                BEGIN;
                INSERT INTO orders (order_no, cust_no, date) VALUES (%s, %s, %s);
                INSERT INTO contains (order_no, SKU, qty) VALUES (%s, %s, %s);
                COMMIT;
                """, ((), (order_no, cust_no, date), (order_no, sku, qty), ()))
                first = False
            else:
                execute_query("INSERT INTO contains (order_no, SKU, qty) VALUES (%s, %s, %s)", (order_no, sku, qty))

    except Exception as e:
        execute_query("ROLLBACK;")  # Rollback the transaction in case of an exception
        return render_template("error.html", error=e)

    return redirect(url_for("order", cust_no=cust_no))

@app.route("/order/remove/<string:order_no>", methods=("POST", "GET"))
def remove_order(order_no):
    try:
        cust_no = execute_query("SELECT cust_no FROM orders WHERE order_no = %s", (order_no,))[0][0]
        execute_multiple_queries("""
        BEGIN;
        DELETE FROM contains WHERE order_no = %s;
        DELETE FROM process WHERE order_no = %s;
        DELETE FROM pay WHERE order_no = %s;
        DELETE FROM orders WHERE order_no = %s;
        COMMIT;
        """, ((), (order_no,), (order_no,), (order_no,), (order_no,), ()))
    except Exception as e:
        return render_template("error.html", error=e)
    return redirect(url_for("order", cust_no=cust_no))

@app.route("/order/pay/<string:order_no>", methods=("POST", "GET"))
def pay_order(order_no):
    try:
        cust_no = execute_query("SELECT cust_no FROM orders WHERE order_no = %s", (order_no,))[0][0]
        execute_query("INSERT INTO pay (order_no, cust_no) VALUES (%s, %s)", (order_no, cust_no))
    except Exception as e:
        return render_template("error.html", error=e)
    return redirect(url_for("order", cust_no=cust_no))

@app.route("/ping", methods=("GET",))
def ping():
    log.debug("ping!")
    return jsonify({"message": "pong!", "status": "success"})



if __name__ == "__main__":
    app.run()
