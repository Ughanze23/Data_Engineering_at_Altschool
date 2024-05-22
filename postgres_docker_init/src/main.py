from db_setup import _db_connection_creds, query_db, connect_to_db


if __name__ == "__main__":
    conn = connect_to_db()
    # return the row count of the table setup in the init.sql script
    query_str = """select count(*) as no_of_rows  
                    from staging.usa_names;"""

    query_db(connection=conn, query_str=query_str)
