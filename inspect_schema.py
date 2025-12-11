import duckdb

con = duckdb.connect('dwelling_fires.duckdb')

with open('schema_info.txt', 'w') as f:
    # List tables
    tables = con.execute("SHOW TABLES").fetchall()
    f.write("TABLES:\n")
    for t in tables:
        f.write(f"- {t[0]}\n")
    f.write("\n")

    # Describe each table
    for t in tables:
        table_name = t[0]
        f.write(f"--- SCHEMA FOR {table_name} ---\n")
        columns = con.execute(f"DESCRIBE {table_name}").fetchall()
        for col in columns:
            f.write(f"{col[0]} ({col[1]})\n")
        f.write("\n")
        
        # Sample data
        f.write(f"--- SAMPLE DATA FOR {table_name} ---\n")
        try:
            sample = con.execute(f"SELECT * FROM {table_name} LIMIT 3").df()
            f.write(sample.to_string())
        except Exception as e:
            f.write(f"Error getting sample: {e}")
        f.write("\n\n")
