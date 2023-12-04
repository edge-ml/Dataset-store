from fastapi.testclient import TestClient

import sys
print(sys.path)

from main import app


client = TestClient(app)

