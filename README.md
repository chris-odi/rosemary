1. **Step 1: Install psycopg2**

    `pip3 install psycopg2==2.9.9`
    
    OR
    
    `pip3 install psycopg2-binary==2.9.9`

    Install with poetry:

    `poetry add psycopg2=2.9.9`
    
    OR

    `poetry add psycopg2-binary=2.9.9`

2. **Step 2: Install with pip:**

    `pip3 install rosemary`
    
    Install with poetry:
    
    `poetry add rosemary`

**Requirements:**
* python = "^3.11"
* pydantic = "^2.5.2"
* sqlalchemy = "^2.0.23"
* asyncpg = "^0.29.0"
* asyncio = "^3.4.3"
* logger = "^1.4"
* importlib = "^1.0.4"
* setuptools = "^69.0.2"
* alembic = "^1.13.0"
* psycopg2-binary = "^2.9.9" or psycopg2 = "^2.9.9"