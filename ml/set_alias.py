import os

from mlflow import MlflowClient

TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
MODEL_NAME = "Dota2WinPredictor"
ALIAS = "champion"


def main() -> None:
    client = MlflowClient(tracking_uri=TRACKING_URI)
    versions = client.search_model_versions(f"name='{MODEL_NAME}'")
    if not versions:
        raise SystemExit(f"model {MODEL_NAME} not registered")
    version = max(int(v.version) for v in versions)
    client.set_registered_model_alias(MODEL_NAME, ALIAS, version)
    print(f"set alias {ALIAS} -> {MODEL_NAME} v{version}")


if __name__ == "__main__":
    main()