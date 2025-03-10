import psycopg2
from datetime import date

# Database connection parameters
conn_params = {
    'dbname': 'hedge_fund',
    'user': 'admin',
    'password': 'admin',
    'host': 'localhost',  # or your database host
    'port': 5432          # default PostgreSQL port
}

class Database:
    def __init__(self):
        # Establish a connection
        try:
            self.connection = psycopg2.connect(**conn_params)
            self.cursor = self.connection.cursor()
            print("Connected to the database!")
        except Exception as e:
            print(f"Unable to connect to the database: {e}")
        
    def get_financial_metrics(self, ticker: str, limit: int = 10) -> list[dict[str, any]] | None:
        query = """
        SELECT * FROM financial_metrics
        WHERE ticker = %s
        ORDER BY report_period DESC
        LIMIT %s;
        """
        try:
            self.cursor.execute(query, (ticker, limit))
            records = self.cursor.fetchall()
            # Get column names from cursor.description
            column_names = [desc[0] for desc in self.cursor.description]

            # Convert rows to a list of dictionaries
            result = []
            for row in records:
                row_dict = dict(zip(column_names, row))
                # Convert report_period to string if it is a date object
                if isinstance(row_dict['report_period'], date):
                    row_dict['report_period'] = row_dict['report_period'].strftime('%Y-%m-%d')
                result.append(row_dict)
            
            print(result)
            return result
        except Exception as e:
            print(f"Error fetching financial metrics: {e}")
            return None

    def set_financial_metrics(self, ticker: str, metrics: list[dict]):
        for metric in metrics:
            # Generate the INSERT query dynamically
            columns = ', '.join(metric.keys())  # Get column names
            placeholders = ', '.join(['%s'] * len(metric))  # Create placeholders (%s, %s, ...)
            query = f"""
                INSERT INTO financial_metrics ({columns})
                VALUES ({placeholders});
            """
            # Extract values from the dictionary in the correct order
            values = tuple(metric.values())
            # Establish a connection
            try:
                # Execute the INSERT statement
                self.cursor.execute(query, values)

                # Commit the transaction
                self.connection.commit()
                print("Data inserted successfully!")

            except Exception as e:
                self.connection.rollback()
                print(f"Error inserting data: {e}")

_db = Database()
def get_database() -> Database:
    """Get the database connection."""
    return _db
