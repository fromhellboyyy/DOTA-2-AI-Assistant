import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import mlflow
import mlflow.sklearn
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    f1_score,
    precision_score,
    recall_score,
    confusion_matrix,
)


os.environ.setdefault("MLFLOW_S3_ENDPOINT_URL", "http://localhost:9000")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "minio")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "minio123")

mlflow.set_tracking_uri("http://localhost:5000")
mlflow.set_experiment("Dota 2 Win Prediction")

df = pd.read_csv("ml/data/matches.csv")
print(f"Загружено {len(df)} строк")

# Препроцессинг 
features = ["duration", "radiant_score", "dire_score", "first_blood_time"]
X = df[features]
y = df["radiant_win"]

mask = X.notna().all(axis=1) & y.notna()
X = X[mask]
y = y[mask]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

def train_and_log(model, model_name, params):
    with mlflow.start_run(run_name=model_name) as run:
        mlflow.log_params(params)
        mlflow.log_param("model_type", model.__class__.__name__)
        mlflow.log_param("train_size", len(X_train))
        mlflow.log_param("test_size", len(X_test))

        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)

        acc = accuracy_score(y_test, y_pred)
        f1 = f1_score(y_test, y_pred)
        prec = precision_score(y_test, y_pred, zero_division=0)
        rec = recall_score(y_test, y_pred, zero_division=0)

        mlflow.log_metric("accuracy", acc)
        mlflow.log_metric("f1", f1)
        mlflow.log_metric("precision", prec)
        mlflow.log_metric("recall", rec)

        cm = confusion_matrix(y_test, y_pred)
        fig, ax = plt.subplots(figsize=(5, 4))
        sns.heatmap(
            cm, annot=True, fmt="d", cmap="Blues",
            xticklabels=["Dire", "Radiant"],
            yticklabels=["Dire", "Radiant"],
            ax=ax,
        )
        ax.set_xlabel("Predicted")
        ax.set_ylabel("Actual")
        ax.set_title(f"Confusion Matrix — {model_name}")
        cm_path = f"confusion_matrix_{model_name}.png"
        fig.savefig(cm_path, bbox_inches="tight")
        mlflow.log_artifact(cm_path)
        plt.close(fig)

        mlflow.sklearn.log_model(
            model,
            "model",
            skops_trusted_types=["sklearn.tree._tree.Tree"]
        )

        print(f"{model_name}: accuracy={acc:.3f}, f1={f1:.3f}")
        return run.info.run_id, acc


results = {}

# 1 Baseline: LogisticRegression
lr_id, lr_acc = train_and_log(
    LogisticRegression(max_iter=1000, random_state=42),
    "LogisticRegression",
    {"max_iter": 1000},
)
results["LogisticRegression"] = (lr_id, lr_acc)

# 2 RandomForest
rf_id, rf_acc = train_and_log(
    RandomForestClassifier(n_estimators=100, random_state=42),
    "RandomForest",
    {"n_estimators": 100},
)
results["RandomForest"] = (rf_id, rf_acc)

# 3 GradientBoosting
gb_id, gb_acc = train_and_log(
    GradientBoostingClassifier(n_estimators=100, learning_rate=0.1, random_state=42),
    "GradientBoosting",
    {"n_estimators": 100, "learning_rate": 0.1},
)
results["GradientBoosting"] = (gb_id, gb_acc)

# Выбираем лучшую модель по accuracy 
best_name = max(results, key=lambda k: results[k][1])
best_run_id, best_acc = results[best_name]
print(f"\n Лучшая модель: {best_name} (accuracy={best_acc:.3f})")

# Регистрируем лучшую модель в Model Registry 
model_uri = f"runs:/{best_run_id}/model"
try:
    registered = mlflow.register_model(model_uri, "Dota2WinPredictor")
    print(f"Модель зарегистрирована. Version: {registered.version}")
    print(f"Не забудь поставить alias 'champion' в MLflow UI!")
except Exception as e:
    print(f"Не удалось зарегистрировать модель: {e}")