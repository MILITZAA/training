import couchdb
import json
import logging
from typing import Dict, List, Any, Optional

# Configurar logging (opcional, puedes personalizarlo)
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

class CouchDBClient:
    """
    Una clase de Python para interactuar con CouchDB.

    Esta clase maneja la conexión al servidor CouchDB, la ejecución de Mango Queries
    y la realización de peticiones de vistas temporales.
    """

    def __init__(self, url: str, db_name: str, username: Optional[str] = None, password: Optional[str] = None):
        """
        Inicializa la clase CouchDBClient.

        Args:
            url: La URL del servidor CouchDB (por ejemplo, 'http://localhost:5984').
            db_name: El nombre de la base de datos con la que se interactúa.
            username: (Opcional) Nombre de usuario para la autenticación de CouchDB.
            password: (Opcional) Contraseña para la autenticación de CouchDB.
        """
        try:
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
        except couchdb.http.ResourceNotFound:
            logging.error(f"Base de datos '{db_name}' no encontrada en el servidor.")
            raise
        except Exception as e:
            logging.error(f"Error al conectar con CouchDB: {e}")
            raise

    def run_mango_query(self, query: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Ejecuta una Mango Query en la base de datos CouchDB.

        Args:
            query: La Mango Query en formato de diccionario.

        Returns:
            Una lista de documentos que coinciden con la consulta.
            Devuelve una lista vacía si no hay documentos coincidentes o si
            ocurre un error.
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
            result = list(self.db.view(map=map_fun, reduce=reduce_fun))
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

        Args:
            document: El documento a crear como un diccionario.

        Returns:
            El ID del documento creado.
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

        Args:
            doc_id: El ID del documento a obtener.

        Returns:
            El documento como un diccionario, o None si no se encuentra.
        """
        try:
            return self.db.get(doc_id)
        except couchdb.http.ResourceNotFound:
            logging.warning(f"Documento con ID: {doc_id} no encontrado.")
            return None
        except Exception as e:
            logging.error(f"Error al obtener el documento: {e}")
            return None