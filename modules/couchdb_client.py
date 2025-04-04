import couchdb
import yaml
import logging
from typing import Dict, List, Any, Optional

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

class CouchDBClient:
    """
    Una clase de Python para interactuar con CouchDB.
    Carga la configuración desde un archivo YAML.
    """

    def __init__(self, config_file: str):
        """
        Inicializa la clase CouchDBClient cargando la configuración desde un archivo YAML.

        Args:
            config_file: La ruta al archivo YAML de configuración.
        """
        try:
            with open(config_file, 'r') as f:
                config = yaml.safe_load(f)
            couchdb_config = config['couchdb']
            url = couchdb_config['url']
            db_name = couchdb_config['db_name']
            username = couchdb_config.get('username')
            password = couchdb_config.get('password')

            if username and password:
                self.couch = couchdb.Server(f'http://{username}:{password}@{url.split("//")[1]}')
            else:
                self.couch = couchdb.Server(url)

            if db_name in self.couch:
                self.db = self.couch[db_name]
                logging.info(f"Conectado a la base de datos existente: {db_name}")
            else:
                self.db = self.couch.create(db_name)
                logging.info(f"Creada y conectada a la base de datos: {db_name}")

        except FileNotFoundError:
            logging.error(f"Archivo de configuración no encontrado: {config_file}")
            raise
        except KeyError as e:
            logging.error(f"Falta una clave requerida en el archivo de configuración: {e}")
            raise
        except couchdb.http.ResourceNotFound:
            logging.error(f"Base de datos no encontrada en el servidor.")
            raise
        except Exception as e:
            logging.error(f"Error al conectar con CouchDB: {e}")
            raise

    def run_mango_query(self, query: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Ejecuta una Mango Query en la base de datos CouchDB.
        """
        try:
            result = list(self.db.find(query))
            logging.info(f"Mango Query ejecutada correctamente. Resultado: {len(result)} documentos.")
            return result
        except couchdb.http.ResourceNotFound:
            logging.error(f"Base de datos '{self.db.name}' no encontrada para Mango query.")
            raise
        except Exception as e:
            logging.error(f"Error al ejecutar Mango Query: {e}")
            return []

    def run_temp_view(self, map_fun: str, reduce_fun: Optional[str] = None) -> List[Any]:
        """
        Ejecuta una vista temporal (función MapReduce) en la base de datos.

        Args:
            map_fun: La función Map de JavaScript como una cadena.
            reduce_fun: (Opcional) La función Reduce de JavaScript como una cadena.

        Returns:
            Una lista de resultados de la vista.
            Devuelve una lista vacía si ocurre un error.
        """
        try:
            result = list(self.db.view(map=map_fun, reduce=reduce_fun, ddoc=None))  # Explicitly set ddoc to None
            logging.info(f"Vista temporal ejecutada correctamente. Resultado: {len(result)} filas.")
            return result
        except couchdb.http.ResourceNotFound:
            logging.error(f"Base de datos '{self.db.name}' no encontrada para la vista temporal.")
            raise
        except Exception as e:
            logging.error(f"Error al ejecutar la vista temporal: {e}")
            return []

    def create_document(self, document: Dict[str, Any]) -> str:
        """
        Crea un nuevo documento en la base de datos.
        """
        try:
            doc_id, doc_rev = self.db.save(document)
            logging.info(f"Documento creado con ID: {doc_id}")
            return doc_id
        except Exception as e:
            logging.error(f"Error al crear documento: {e}")
            raise

    def get_document(self, doc_id: str) -> Optional[Dict[str, Any]]:
        """
        Obtiene un documento por su ID.
        """
        try:
            return self.db.get(doc_id)
        except couchdb.http.ResourceNotFound:
            logging.warning(f"Documento con ID: {doc_id} no encontrado.")
            return None
        except Exception as e:
            logging.error(f"Error al obtener el documento: {e}")
            return None