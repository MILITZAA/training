from modules.mysql_connector import MySQLConnector

# Ruta al archivo YAML
cred_file = "cred/db_credentials.yaml"

# Crea una instancia de MySQLConnector
connector = MySQLConnector(cred_file)

# Ejecuta una consulta
query = "SELECT * FROM actor LIMIT 5;"
df = connector.query(query)

# Imprime el DataFrame
print(df)

# Cierra la conexión
connector.close_connection()
