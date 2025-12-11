# app.py

# TODO: importer FastAPI
from fastapi import FastAPI

# TODO: créer une instance FastAPI
app = FastAPI()

@app.get("/health")
def health():
    return {"status": "ok"}
