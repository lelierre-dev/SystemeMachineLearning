from fastapi import FastAPI
from pydantic import BaseModel
from feast import FeatureStore
import mlflow
import mlflow.pyfunc
import pandas as pd
import os
from typing import Any

app = FastAPI(title="StreamFlow Churn Prediction API")

# --- Config ---
REPO_PATH = "/repo"
MODEL_NAME = "streamflow_churn"  # <-- ton modèle MLflow enregistré
MODEL_URI = f"models:/{MODEL_NAME}/Production"

# Important: depuis un conteneur -> mlflow:5000 (pas localhost:5001)
MLFLOW_TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")
mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

# Init Feast + MLflow model
try:
    store = FeatureStore(repo_path=REPO_PATH)
    model = mlflow.pyfunc.load_model(MODEL_URI)
except Exception as e:
    print(f"Warning: init failed: {e}")
    store = None
    model = None


class UserPayload(BaseModel):
    user_id: str


@app.get("/health")
def health():
    return {"status": "ok"}


# (Optionnel) ton endpoint existant pour debug
@app.get("/features/{user_id}")
def get_features(user_id: str):
    if store is None:
        return {"error": "Feature store not initialized"}

    features = [
        "subs_profile_fv:months_active",
        "subs_profile_fv:monthly_fee",
        "subs_profile_fv:paperless_billing",
    ]

    feature_dict = store.get_online_features(
        features=features,
        entity_rows=[{"user_id": user_id}],
    ).to_dict()

    # Simplifie (clé -> scalaire)
    simple = {name: values[0] for name, values in feature_dict.items()}

    return {"user_id": user_id, "features": simple}


@app.post("/predict")
def predict(payload: UserPayload):
    if store is None or model is None:
        return {"error": "Model or feature store not initialized"}

    features_request = [
        "subs_profile_fv:months_active",
        "subs_profile_fv:monthly_fee",
        "subs_profile_fv:paperless_billing",
        "subs_profile_fv:plan_stream_tv",
        "subs_profile_fv:plan_stream_movies",
        "subs_profile_fv:net_service",
        "usage_agg_30d_fv:watch_hours_30d",
        "usage_agg_30d_fv:avg_session_mins_7d",
        "usage_agg_30d_fv:unique_devices_30d",
        "usage_agg_30d_fv:skips_7d",
        "usage_agg_30d_fv:rebuffer_events_7d",
        "payments_agg_90d_fv:failed_payments_90d",
        "support_agg_90d_fv:support_tickets_90d",
        "support_agg_90d_fv:ticket_avg_resolution_hrs_90d",
    ]

    # TODO 3 : Récupérer les features online
    feature_dict = store.get_online_features(
        features=features_request,
        entity_rows=[{"user_id": payload.user_id}],
    ).to_dict()

    # Feast renvoie { "feat_name": [value], ... } -> DF 1 ligne
    X = pd.DataFrame({k: [v[0]] for k, v in feature_dict.items()})

    # Gestion des features manquantes
    if X.isnull().any().any():
        missing = X.columns[X.isnull().any()].tolist()
        return {
            "error": f"Missing features for user_id={payload.user_id}",
            "missing_features": missing,
        }

    # Nettoyage minimal (évite bugs de types)
    X = X.drop(columns=["user_id"], errors="ignore")

    # TODO 4: appeler le modèle
    y_pred = model.predict(X)

    # Rendre robuste quel que soit le type de sortie MLflow (np array / Series / DataFrame)
    pred_value: Any
    if isinstance(y_pred, pd.DataFrame):
        # cas fréquent: DataFrame avec une colonne prediction
        if "prediction" in y_pred.columns:
            pred_value = y_pred["prediction"].iloc[0]
        else:
            pred_value = y_pred.iloc[0, 0]
    elif isinstance(y_pred, pd.Series):
        pred_value = y_pred.iloc[0]
    else:
        # numpy array / list
        pred_value = y_pred[0]

    return {
        "user_id": payload.user_id,
        "prediction": int(pred_value),
        "features_used": X.to_dict(orient="records")[0],
    }
