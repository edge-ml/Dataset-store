from fastapi.param_functions import Depends
from jwt import decode, InvalidSignatureError, ExpiredSignatureError
from bson.objectid import ObjectId
from fastapi import status, Header, HTTPException
from db.project import ProjectDBManager
from internal.config import SECRET_KEY
from db.deviceAPi import DeviceApiManager

project_dbm = ProjectDBManager()
deviceApi_dbm = DeviceApiManager()

async def extract_project_id(project: str = Header(...)):
    return project

async def validate_user(Authorization: str = Header(...), project_id=Depends(extract_project_id)):
    try:
        token = Authorization.split(" ")[1]
        decoded = decode(token, SECRET_KEY, algorithms=["HS256"])
        if "exp" not in decoded:
            raise ExpiredSignatureError
        user_id = ObjectId(decoded["id"])
        sub_level = decoded.get("subscriptionLevel")
        project = project_dbm.get_project(project_id)
        if not project:
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="Authentication failed")
        if str(project['admin']) != str(user_id) and str(user_id) not in project['users']:
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="User unauthorized on project")
        return (user_id, token, sub_level)
    except InvalidSignatureError as e:
        print(e)
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="Authentication failed")
    except ExpiredSignatureError:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="Token expired")
    

class validateApiKey:
    def __init__(self, access_type):
        self.access_type = access_type
    
    def __call__(self, api_key):
        res = deviceApi_dbm.get(api_key)
        if res is None:
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="Authentication failed")
        if res['access_type'] != self.access_type:
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail=f"Used Api Key does not grant {self.access_type} access")
        return {"projectId": res["projectId"], "userId": res["userId"]}