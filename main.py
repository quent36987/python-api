from typing import List
import os
import uuid
import jwt
import csv
import shutil
import glob
import zipfile
import logging
from datetime import datetime, timedelta

from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks, Body
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel

from state import STATUS, Progress
from worker import run_calculation
from azure.storage.blob import BlobClient

# -- Configuration et constantes --
JWT_SECRET       = "f35dc5eb141b0c88a488550fca349ac8c616de08c23e1e5e2b23fff428c97845"
CLIENT_ID        = os.getenv("CLIENT_ID")
CLIENT_SECRET    = os.getenv("CLIENT_SECRET")
LOCAL_TMP_ROOT   = os.getenv("TMP_FILE")
ALGO             = "HS256"

app = FastAPI(title="MindService")
security = HTTPBearer()

# ───────── Modèles Pydantic ─────────────────────────────────────────────────

class Token(BaseModel):
    access_token: str
    token_type: str = "bearer"

class TokenRequest(BaseModel):
    client_id: str
    client_secret: str

class RunRequest(BaseModel):
    calculationName: str
    inputZipUrl: str
    inputFiles: List[str]
    outputFiles: List[str]

class RunResponse(BaseModel):
    runId: str

class UploadCloseRequest(BaseModel):
    runId: str
    resultSas: str

# ───────── Auth helpers ──────────────────────────────────────────────────────

def verify_token(cred: HTTPAuthorizationCredentials = Depends(security)) -> str:
    try:
        payload = jwt.decode(cred.credentials, JWT_SECRET, algorithms=[ALGO])
        return payload["sub"]
    except jwt.PyJWTError:
        raise HTTPException(401, "Invalid token")

# ───────── Routes ────────────────────────────────────────────────────────────


@app.get("/health")
def health():
    return "oui"

@app.post("/MindGetToken", response_model=Token)
def mind_get_token(req: TokenRequest = Body(...)):
    if (req.client_id, req.client_secret) != (CLIENT_ID, CLIENT_SECRET):
        raise HTTPException(status_code=401, detail="Bad credentials")
    payload = {
        "sub": req.client_id,
        "exp": datetime.utcnow() + timedelta(hours=12)
    }
    token = jwt.encode(payload, JWT_SECRET, algorithm=ALGO)
    return {"access_token": token}

@app.post("/MindRunAsync", response_model=RunResponse)
async def mind_run_async(
    req: RunRequest,
    background_tasks: BackgroundTasks,
    user: str = Depends(verify_token)
):
    # 1. Vérification du calcul
    if req.calculationName != "doubleMatrix":
        raise HTTPException(400, "Unknown calculationName")

    # 2. Génération du run_id et préparation du dossier
    run_id = str(uuid.uuid4())
    run_dir = os.path.join(LOCAL_TMP_ROOT, run_id)
    os.makedirs(run_dir, exist_ok=True)

    # 3. Emplacement du ZIP téléchargé ou copié
    download_path = os.path.join(run_dir, f"{run_id}_input.zip")

    try:
        blob = BlobClient.from_blob_url(req.inputZipUrl)
        with open(download_path, "wb") as f:
            stream = blob.download_blob()
            stream.readinto(f)

        # 5. Extraction dans run_dir
        with zipfile.ZipFile(download_path, mode="r") as zf:
            extracted = []
            missing = []
            for fname in req.inputFiles:
                csv_name = f"{fname}.csv"
                try:
                    zf.extract(csv_name, path=run_dir)
                    extracted.append(csv_name)
                except KeyError:
                    missing.append(csv_name)

            if missing:
                raise HTTPException(400, f"Missing files in zip: {', '.join(missing)}")

    except HTTPException:
        raise
    except Exception as e:
        shutil.rmtree(run_dir, ignore_errors=True)
        raise HTTPException(400, f"Failed to download or extract input zip: {e}")

    # 6. Planification du calcul en arrière‑plan
    background_tasks.add_task(
        run_calculation,
        run_id=run_id,
        input_csvs=[os.path.join(run_dir, f) for f in extracted],
        output_files=req.outputFiles
    )

    # 7. Retour de l’identifiant de run
    return {"runId": run_id}

@app.get("/MindRunProgress", response_model=Progress)
def mind_run_progress(runId: str, user: str = Depends(verify_token)):
    st = STATUS.get(runId)
    if not st:
        raise HTTPException(400, "runId inconnu")
    return st

@app.post("/MindUploadClose", response_model=UploadCloseRequest)
def mind_upload_close(req: UploadCloseRequest, user: str = Depends(verify_token)):
    run_id = req.runId
    status = STATUS.get(run_id)
    if not status:
        raise HTTPException(status_code=400, detail="runId inconnu")

    output_dir = os.path.join(LOCAL_TMP_ROOT, "output", run_id)
    if not os.path.isdir(output_dir):
        raise HTTPException(status_code=404, detail=f"Aucun dossier de sortie pour le run {run_id}")
    csv_files = [f for f in os.listdir(output_dir) if f.lower().endswith(".csv")]
    if not csv_files:
        raise HTTPException(status_code=404, detail=f"Aucun fichier CSV trouvé pour le run {run_id}")

    zip_filename = f"{run_id}_results.zip"
    zip_path = os.path.join(output_dir, zip_filename)
    try:
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zip_out:
            for csv_file in csv_files:
                zip_out.write(os.path.join(output_dir, csv_file), arcname=csv_file)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Échec de la création de l'archive de résultats : {e}")

    try:
        result_blob = BlobClient.from_blob_url(req.resultSas)
        with open(zip_path, "rb") as data:
            result_blob.upload_blob(data, overwrite=True)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Échec de l'envoi de l'archive de résultats sur Azure : {e}")

    status.done = True
    status.result = req.resultSas

    try:
        os.remove(zip_path)
        for csv_file in csv_files:
            os.remove(os.path.join(output_dir, csv_file))
        shutil.rmtree(output_dir, ignore_errors=True)
        shutil.rmtree(os.path.join(LOCAL_TMP_ROOT, run_id), ignore_errors=True)
    except OSError:
        pass

    return UploadCloseRequest(runId=run_id, resultSas=req.resultSas)
