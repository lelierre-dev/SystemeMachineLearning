import os
import pandas as pd
from sqlalchemy import create_engine
from feast import FeatureStore

# Date de référence pour la création du dataset d'entraînement
AS_OF = "2024-01-31"
FEAST_REPO = "/repo"

def get_engine():
    uri = (
        f"postgresql+psycopg2://{os.getenv('POSTGRES_USER','streamflow')}:"
        f"{os.getenv('POSTGRES_PASSWORD','streamflow')}@"
        f"{os.getenv('POSTGRES_HOST','postgres')}:5432/"
        f"{os.getenv('POSTGRES_DB','streamflow')}"
    )
    return create_engine(uri)

def build_entity_df(engine, as_of: str) -> pd.DataFrame:
    """
    Construit le DataFrame des entités (utilisateurs) actifs à la date donnée.
    Feast a besoin de 'user_id' et 'event_timestamp' pour savoir "qui" et "quand" regarder.
    """
    q = """
    SELECT user_id, as_of
    FROM subscriptions_profile_snapshots
    WHERE as_of = %(as_of)s
    """
    df = pd.read_sql(q, engine, params={"as_of": as_of})
    if df.empty:
        raise RuntimeError(f"No snapshot rows found at as_of={as_of}")
    
    # Renommage impératif pour Feast : la colonne temporelle doit s'appeler event_timestamp
    df = df.rename(columns={"as_of": "event_timestamp"})
    df["event_timestamp"] = pd.to_datetime(df["event_timestamp"])
    return df[["user_id", "event_timestamp"]]

def fetch_labels(engine, as_of: str) -> pd.DataFrame:
    """
    Récupère les variables cibles (labels).
    On associe ces labels au même timestamp que les entités pour permettre la jointure.
    """
    q = "SELECT user_id, churn_label FROM labels"
    labels = pd.read_sql(q, engine)
    if labels.empty:
        raise RuntimeError("Labels table is empty.")
    
    labels["event_timestamp"] = pd.to_datetime(as_of)
    return labels[["user_id", "event_timestamp", "churn_label"]]

def main():
    engine = get_engine()
    print(f"--- Construction du dataset pour la date {AS_OF} ---")

    # 1. Préparation des entités et des labels
    entity_df = build_entity_df(engine, AS_OF)
    print(f"Entities found: {len(entity_df)}")
    
    labels = fetch_labels(engine, AS_OF)
    print(f"Labels found: {len(labels)}")

    store = FeatureStore(repo_path=FEAST_REPO)

    # 2. Définition des features à récupérer via le Feature Store
    features = [
        "subs_profile_fv:months_active",
        "subs_profile_fv:monthly_fee",
        "subs_profile_fv:paperless_billing",
        "usage_agg_30d_fv:watch_hours_30d",
        "usage_agg_30d_fv:avg_session_mins_7d",
        "payments_agg_90d_fv:failed_payments_90d"
    ]

    # 3. Récupération historique (Point-in-Time Join)
    print("Fetching historical features from Feast...")
    hf = store.get_historical_features(
        entity_df=entity_df,
        features=features,
    ).to_df()
    
    print(f"Features fetched. Shape: {hf.shape}")

    # 4. Fusion des features avec les labels
    # On utilise un inner join pour ne garder que les lignes complètes (Features + Label)
    df = hf.merge(labels, on=["user_id", "event_timestamp"], how="inner")

    if df.empty:
        raise RuntimeError("Training set is empty after merge. Check AS_OF and labels.")

    # 5. Sauvegarde du dataset final
    os.makedirs("/data/processed", exist_ok=True)
    output_path = "/data/processed/training_df.csv"
    df.to_csv(output_path, index=False)
    print(f"[OK] Wrote {output_path} with {len(df)} rows")

if __name__ == "__main__":
    main()