# Training Repository

This repository is for training purposes and contains Python modules and Jupyter notebooks.

## Initial Setup

1. Clone the repository.
2. Create a virtual environment.
3. Install dependencies from `requirements.txt`.

## Implement Python Class for MySQL Database Connection

1. Create the YAML file and save the MySQL connection credentials.
2. Implement the MySQLConnector class inside `mysql_connector.py`.
3. Add necessary libraries to `requirements.txt`.
4. Ensure the `.gitignore` file is properly set up to exclude sensitive credential files

```bash
git clone git@github.com:MILITZAA/training.git
python3.9 -m venv venv
source venv/bin/activate  # On Windows: .\venv\Scripts\Activate
pip install -r requirements.txt
pip install SQLAlchemy
pip install PyMySQL
pip install cryptography

