import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import mlflow

os.environ.setdefault("MLFLOW_S3_ENDPOINT_URL", "http://localhost:9000")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "minio")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "minio123")

sns.set_theme(style="whitegrid")
plt.rcParams["figure.figsize"] = (10, 6)

mlflow.set_tracking_uri("http://localhost:5000")
mlflow.set_experiment("Dota 2 EDA")

df = pd.read_csv("ml/data/matches.csv")
print(f"Размер датасета: {df.shape}")

df = df[df["duration"] > 0].copy()
print(f"После фильтрации (duration > 0): {len(df)} матчей")

print("\nПервые 5 строк:")
print(df.head())
print("\nОписательная статистика:")
print(df.describe())
print("\nПропуски:")
print(df.isna().sum())

with mlflow.start_run(run_name="EDA Analysis"):
    mlflow.log_param("dataset_size", len(df))
    mlflow.log_param("columns", ", ".join(df.columns))

    fig, ax = plt.subplots()
    sns.histplot(df["duration"] / 60, bins=30, ax=ax, kde=True)
    ax.set_title("Распределение длительности матчей")
    ax.set_xlabel("Длительность (минуты)")
    ax.set_ylabel("Количество матчей")
    fig.savefig("duration_dist.png", bbox_inches="tight")
    mlflow.log_artifact("duration_dist.png")
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(6, 6))
    counts = df["radiant_win"].value_counts()
    ax.pie(
        counts,
        labels=["Dire Win", "Radiant Win"],
        autopct="%1.1f%%",
        colors=["#d9534f", "#5cb85c"],
        startangle=90,
    )
    ax.set_title("Соотношение побед Radiant и Dire")
    fig.savefig("win_rate.png", bbox_inches="tight")
    mlflow.log_artifact("win_rate.png")
    plt.close(fig)

    fig, ax = plt.subplots()
    sns.histplot(df["radiant_score"], bins=20, ax=ax, label="Radiant", color="#5cb85c", alpha=0.6)
    sns.histplot(df["dire_score"], bins=20, ax=ax, label="Dire", color="#d9534f", alpha=0.6)
    ax.set_title("Распределение счёта команд")
    ax.set_xlabel("Убийства (score)")
    ax.legend()
    fig.savefig("score_dist.png", bbox_inches="tight")
    mlflow.log_artifact("score_dist.png")
    plt.close(fig)

    numeric_cols = ["duration", "radiant_win", "radiant_score", "dire_score", "first_blood_time"]
    corr = df[numeric_cols].corr()
    fig, ax = plt.subplots(figsize=(8, 6))
    sns.heatmap(corr, annot=True, cmap="coolwarm", fmt=".2f", ax=ax, square=True)
    ax.set_title("Корреляционная матрица признаков")
    fig.savefig("correlation.png", bbox_inches="tight")
    mlflow.log_artifact("correlation.png")
    plt.close(fig)

    # 5. Метрики
    mlflow.log_metric("avg_duration_min", df["duration"].mean() / 60)
    mlflow.log_metric("median_duration_min", df["duration"].median() / 60)
    mlflow.log_metric("radiant_win_rate", df["radiant_win"].mean())
    mlflow.log_metric("avg_radiant_score", df["radiant_score"].mean())
    mlflow.log_metric("avg_dire_score", df["dire_score"].mean())

    print("\nEDA завершён, артефакты залогированы в MLflow")

print(f"""
Итоговая статистика:
- Всего валидных матчей: {len(df)}
- Средняя длительность: {df['duration'].mean() / 60:.1f} мин
- Процент побед Radiant: {df['radiant_win'].mean() * 100:.1f}%
- Средний счёт Radiant: {df['radiant_score'].mean():.1f}
- Средний счёт Dire: {df['dire_score'].mean():.1f}
""")