
    # Example of creating a task that calls a common CREATE TABLE sql command.
create_table_sqlite_task = SQLExecuteQueryOperator(
    task_id="create_table_sqlite",
    sql=r"""
    CREATE TABLE Customers (
        customer_id INT PRIMARY KEY,
        first_name TEXT,
        last_name TEXT
    );
    """,
)
