"""
render_proxy/proxy.py
=============================================================
RENDER — Proxy léger (~100 MB RAM)
=============================================================
Rôle UNIQUEMENT :
  1. Vérifier le token Firebase JWT (sécurité)
  2. Transférer la requête vers le Pi5/PC
  3. Retourner la réponse en streaming

Le LLM tourne sur le Pi5/PC — PAS sur Render.
=============================================================
"""

import os
import json
import httpx
import asyncio
from fastapi import FastAPI, HTTPException, Depends, Header, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

import firebase_admin
from firebase_admin import credentials, auth as fb_auth


# ──────────────────────────────────────────────────────────
# URL du Pi5/PC — chargée depuis variable d'environnement
# ──────────────────────────────────────────────────────────
# Sur Render Dashboard → Environment → PI5_URL
# Valeur : https://xxxx.ngrok.io  (depuis ngrok sur le Pi5)
PI5_URL = os.getenv("PI5_URL", "http://localhost:8001")


# ──────────────────────────────────────────────────────────
# Firebase Admin init
# ──────────────────────────────────────────────────────────
def init_firebase():
    if firebase_admin._apps:
        return
    sa_json = os.getenv("FIREBASE_SERVICE_ACCOUNT")
    if not sa_json:
        raise RuntimeError(
            "Variable FIREBASE_SERVICE_ACCOUNT manquante sur Render"
        )
    cred = credentials.Certificate(json.loads(sa_json))
    firebase_admin.initialize_app(cred)
    print("✅ Firebase Admin initialisé")


# ──────────────────────────────────────────────────────────
# App FastAPI
# ──────────────────────────────────────────────────────────
app = FastAPI(title="CarManual Proxy", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup():
    init_firebase()
    print(f"🚀 Proxy démarré → Pi5/PC : {PI5_URL}")


# ──────────────────────────────────────────────────────────
# Vérification JWT Firebase
# ──────────────────────────────────────────────────────────
async def verify_token(authorization: str = Header(None)) -> dict:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "Token manquant ou format invalide")
    token = authorization.split(" ")[1]
    try:
        return fb_auth.verify_id_token(token)
    except fb_auth.ExpiredIdTokenError:
        raise HTTPException(401, "Token expiré — reconnecter l'utilisateur")
    except Exception as e:
        raise HTTPException(401, f"Token invalide : {str(e)}")


# ──────────────────────────────────────────────────────────
# Endpoints
# ──────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    """Public — vérifie que le proxy tourne"""
    # Tester aussi la connexion vers Pi5
    try:
        async with httpx.AsyncClient(timeout=5) as c:
            r = await c.get(f"{PI5_URL}/health")
            pi5_ok = r.status_code == 200
    except Exception:
        pi5_ok = False

    return {
        "proxy"   : "ok",
        "pi5_url" : PI5_URL,
        "pi5_ok"  : pi5_ok,
    }


@app.get("/vehicles")
async def vehicles(user: dict = Depends(verify_token)):
    """Liste des véhicules — proxy vers Pi5"""
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.get(f"{PI5_URL}/vehicles")
        return r.json()
    except Exception as e:
        raise HTTPException(503, f"Pi5 inaccessible : {e}")


class AskRequest(BaseModel):
    question  : str
    vehicle_id: str
    language  : str = "fr"


@app.post("/ask")
async def ask(req: AskRequest, user: dict = Depends(verify_token)):
    """
    Question → proxy en streaming vers Pi5.
    Le JWT est vérifié ici sur Render.
    Le Pi5 reçoit la requête sans avoir besoin de Firebase.
    """
    uid = user.get("uid", "?")
    print(f"[{uid[:8]}] Q: {req.question[:50]}")

    async def stream():
        try:
            async with httpx.AsyncClient(timeout=120) as c:
                async with c.stream(
                    "POST",
                    f"{PI5_URL}/ask",
                    json={
                        "question"  : req.question,
                        "vehicle_id": req.vehicle_id,
                        "language"  : req.language,
                        "uid"       : uid,  # Optionnel : pour logs Pi5
                    },
                    headers={"Content-Type": "application/json"},
                ) as r:
                    async for chunk in r.aiter_bytes():
                        yield chunk
        except httpx.ConnectError:
            error = json.dumps({
                "type"   : "error",
                "message": "Pi5 non joignable. Vérifier ngrok."
            })
            yield f"data: {error}\n\n".encode()
        except Exception as e:
            error = json.dumps({"type": "error", "message": str(e)})
            yield f"data: {error}\n\n".encode()

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
    )


@app.post("/transcribe")
async def transcribe(request: Request,
                     user: dict = Depends(verify_token)):
    """Audio → texte — proxy vers Pi5"""
    body = await request.body()
    try:
        async with httpx.AsyncClient(timeout=60) as c:
            r = await c.post(
                f"{PI5_URL}/transcribe",
                content=body,
                headers={"Content-Type": "application/json"},
            )
        return r.json()
    except Exception as e:
        raise HTTPException(503, f"Erreur transcription : {e}")
