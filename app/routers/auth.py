"""Auth router.

Implements the endpoints the frontend expects under AUTH_URI (`/auth/`),
previously served by the standalone authentication Node service:
login/refresh/register/unregister, user management and GitHub OAuth.
"""
from typing import Any, Dict, List, Optional

import jwt as pyjwt
import requests as http_client
from fastapi import APIRouter, Depends, Header, HTTPException, Query, status
from fastapi.responses import JSONResponse, RedirectResponse

import internal.config as config
from controller.auth_controller import (
    AuthError,
    change_mail,
    change_password,
    change_user_name,
    create_access_token,
    create_refresh_token,
    delete_user,
    get_current_user,
    get_user,
    login,
    map_ids_to_user_names,
    map_user_names_to_ids,
    register_user,
    suggest_user_names,
    users_dbm,
)
from db.users import DuplicateUserError

router = APIRouter()


def _auth_error(error: AuthError) -> HTTPException:
    return HTTPException(status_code=error.status_code, detail=error.message)


def require_user(authorization: Optional[str] = Header(None)) -> Dict[str, Any]:
    user = get_current_user(authorization)
    if not user:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")
    return user


# --------------------------------------------------------------------- #
# Registration & session
# --------------------------------------------------------------------- #
@router.post("/register", status_code=status.HTTP_201_CREATED)
def register(body: Dict[str, Any]):
    try:
        register_user(body)
    except AuthError as error:
        raise _auth_error(error)
    return {"message": "Successfully created user!"}


@router.post("/login")
def login_route(body: Dict[str, Any]):
    try:
        tokens = login(body.get("email"), body.get("password"))
    except AuthError as error:
        raise _auth_error(error)
    response = JSONResponse(tokens)
    # also set the cookie so classic cookie-based flows keep working
    response.set_cookie("jwt", tokens["access_token"], httponly=True, samesite="lax")
    return response


@router.get("/logout")
def logout():
    response = JSONResponse({"success": True})
    response.delete_cookie("jwt")
    return response


@router.get("/user")
def current_user(user: Dict[str, Any] = Depends(require_user)):
    return get_user(user)


# --------------------------------------------------------------------- #
# User management
# --------------------------------------------------------------------- #
@router.delete("/unregister")
def unregister(body: Dict[str, Any], user: Dict[str, Any] = Depends(require_user)):
    try:
        message = delete_user(user, body.get("email"))
    except AuthError as error:
        raise _auth_error(error)
    return {"message": message}


@router.put("/changeMail")
def change_mail_route(body: Dict[str, Any], user: Dict[str, Any] = Depends(require_user)):
    try:
        message = change_mail(user, body.get("email"))
    except AuthError as error:
        raise _auth_error(error)
    return {"message": message}


@router.put("/changePassword")
def change_password_route(body: Dict[str, Any], user: Dict[str, Any] = Depends(require_user)):
    try:
        message = change_password(user, body.get("password"), body.get("newPassword"))
    except AuthError as error:
        raise _auth_error(error)
    return {"message": message}


@router.put("/changeUserName")
def change_user_name_route(body: Dict[str, Any], user: Dict[str, Any] = Depends(require_user)):
    try:
        message = change_user_name(user, body.get("userName"))
    except AuthError as error:
        raise _auth_error(error)
    return {"message": message}


@router.post("/id")
def usernames_to_ids(
    body: Optional[List[Any]] = None, user: Dict[str, Any] = Depends(require_user)
):
    try:
        return map_user_names_to_ids(body or [])
    except AuthError as error:
        raise _auth_error(error)


@router.post("/userName")
def ids_to_usernames(body: List[Any], user: Dict[str, Any] = Depends(require_user)):
    try:
        return map_ids_to_user_names(body)
    except AuthError as error:
        raise _auth_error(error)


@router.post("/userNameSuggest")
def username_suggestions(body: Dict[str, Any], user: Dict[str, Any] = Depends(require_user)):
    return suggest_user_names(body.get("userName"))


# --------------------------------------------------------------------- #
# GitHub OAuth
# --------------------------------------------------------------------- #
@router.get("/login/oauth")
def oauth_login(provider: str = Query("github")):
    if provider != "github":
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail="Unsupported OAuth provider")
    if not config.GITHUB_CLIENT_ID:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, detail="GitHub OAuth is not configured")
    redirect_uri = config.GITHUB_CALLBACK_URL
    url = (
        "https://github.com/login/oauth/authorize"
        f"?client_id={config.GITHUB_CLIENT_ID}&scope=user:email&prompt=select_account"
    )
    if redirect_uri:
        url += f"&redirect_uri={redirect_uri}"
    return RedirectResponse(url)


@router.get("/login/callback")
def oauth_callback(code: str = Query(...)):
    if not config.GITHUB_CLIENT_ID or not config.GITHUB_CLIENT_SECRET:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, detail="GitHub OAuth is not configured")

    token_res = http_client.post(
        "https://github.com/login/oauth/access_token",
        json={
            "client_id": config.GITHUB_CLIENT_ID,
            "client_secret": config.GITHUB_CLIENT_SECRET,
            "code": code,
            "redirect_uri": config.GITHUB_CALLBACK_URL,
        },
        headers={"Accept": "application/json"},
        timeout=15,
    )
    access_token = token_res.json().get("access_token")
    if not access_token:
        return RedirectResponse(url=f"{config.HOST}/login")

    headers = {"Authorization": f"Bearer {access_token}"}
    profile = http_client.get("https://api.github.com/user", headers=headers, timeout=15).json()
    emails = http_client.get("https://api.github.com/user/emails", headers=headers, timeout=15).json()
    email = None
    if isinstance(emails, list):
        primary = next((e for e in emails if e.get("primary")), None)
        email = (primary or (emails[0] if emails else {})).get("email")

    user = users_dbm.upsert_oauth_user(
        provider="github",
        provider_id=str(profile.get("id")),
        email=email,
        user_name=profile.get("username") or f"github-{profile.get('id')}",
    )
    refresh_token = user.get("refreshToken") or create_refresh_token(user["_id"])
    if not user.get("refreshToken"):
        users_dbm.update_user(user["_id"], {"refreshToken": refresh_token})

    response = RedirectResponse(url=config.HOST)
    response.set_cookie("jwt", create_access_token(user), httponly=True, samesite="lax")
    return response
