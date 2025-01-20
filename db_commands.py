import sqlite3


def main(query: str):
    connection = sqlite3.connect("target.db")
    cursor = connection.cursor()

    try:
        cursor.execute(query)
        connection.commit()
    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    query = """
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL,
            amount REAL NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users (id)
        )
    """
    # query = "UPDATE orders SET amount = 100000.0 WHERE user_id = 1"
    # query = "DROP TABLE orders"
    
    main(query=query)
