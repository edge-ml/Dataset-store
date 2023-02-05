from fastapi.param_functions import Depends
import jwt
from bson.objectid import ObjectId
from fastapi import status, Header, HTTPException
from app.db.project import ProjectDBManager
from app.internal.config import SECRET_KEY

project_dbm = ProjectDBManager()

async def extract_project_id(project: str = Header(...)):
    return project

async def validate_user(Authorization: str = Header(...), project_id=Depends(extract_project_id)):
    try:
        token = Authorization.split(" ")[1]
        decoded = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        if "exp" not in decoded:
            raise jwt.ExpiredSignatureError
        user_id = ObjectId(decoded["id"])
        sub_level = decoded.get("subscriptionLevel")
        project = project_dbm.get_project(project_id)
        if project['admin'] != user_id and user_id not in project['users']:
            print('not in db')
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="User unauthorized on project")
        return (user_id, token, sub_level)
    except jwt.ExpiredSignatureError:
       raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="Authentication failed")
