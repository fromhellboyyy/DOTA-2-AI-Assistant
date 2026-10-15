import os
import time
import hashlib
import requests
import pandas as pd
import mlflow

# --- Настройки ---
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
OPENDOTA_API_BASE = "https://api.opendota.com/api"
DATA_DIR = "ml/data"
CSV_PATH = os.path.join(DATA_DIR, "matches.csv")
# OpenDota API: без ключа ~60 запросов/мин, 2000 запросов/день[reference:0]
# Рекомендуемая пауза между запросами — 3 секунды, чтобы не исчерпать дневной лимит[reference:1]
REQUEST_DELAY_SEC = 3
# ------------------

def compute_dataset_digest(file_path: str) -> str:
    """Вычисляет SHA-256 хеш файла датасета."""
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            sha256.update(chunk)
    return sha256.hexdigest()

def fetch_matches(limit: int = 100) -> pd.DataFrame:
    """Скачивает матчи через OpenDota API."""
    os.makedirs(DATA_DIR, exist_ok=True)

    print(f"Запрос к {OPENDOTA_API_BASE}/publicMatches ...")
    response = requests.get(f"{OPENDOTA_API_BASE}/publicMatches", timeout=30)
    response.raise_for_status()
    matches = response.json()
    print(f"Получено {len(matches)} публичных матчей.")

    match_ids = [m["match_id"] for m in matches[:limit]]
    detailed_matches = []

    for i, match_id in enumerate(match_ids, start=1):
        print(f"Скачиваем матч {i}/{len(match_ids)}: {match_id}")
        try:
            detail_url = f"{OPENDOTA_API_BASE}/matches/{match_id}"
            detail = requests.get(detail_url, timeout=30).json()

            if "duration" in detail and "radiant_win" in detail:
                detailed_matches.append({
                    "match_id": match_id,
                    "duration": detail.get("duration"),
                    "radiant_win": 1 if detail.get("radiant_win") else 0,
                    "radiant_score": detail.get("radiant_score", 0),
                    "dire_score": detail.get("dire_score", 0),
                    "game_mode": detail.get("game_mode"),
                    "lobby_type": detail.get("lobby_type"),
                    "first_blood_time": detail.get("first_blood_time", 0),
                })
        except Exception as e:
            print(f"  ⚠️ Ошибка при загрузке {match_id}: {e}")

        time.sleep(REQUEST_DELAY_SEC)

    df = pd.DataFrame(detailed_matches)
    df.to_csv(CSV_PATH, index=False)
    print(f"✅ Сохранено {len(df)} матчей в {CSV_PATH}")
    return df

def log_dataset_to_mlflow():
    """Регистрирует датасет в MLflow с указанием source и digest."""
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment("Dota 2 Dataset Tracking")

    digest = compute_dataset_digest(CSV_PATH)
    df = pd.read_csv(CSV_PATH)

    with mlflow.start_run(run_name="dataset_registration"):
        # Логируем датасет как артефакт
        mlflow.log_artifact(CSV_PATH, artifact_path="dataset")
        # Логируем метаданные датасета (source, digest)
        mlflow.log_param("dataset_source", "https://api.opendota.com/api/publicMatches")
        mlflow.log_param("dataset_digest", digest)
        mlflow.log_param("dataset_rows", len(df))
        mlflow.log_param("dataset_columns", ", ".join(df.columns))
        print(f"✅ Датасет залогирован в MLflow. Digest: {digest}")

if __name__ == "__main__":
    log_dataset_to_mlflow()