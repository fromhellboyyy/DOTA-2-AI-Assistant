import mlflow.pyfunc

def load_champion_model():
    mlflow.set_tracking_uri("http://mlflow:5000") 
    model = mlflow.pyfunc.load_model("models:/Dota2WinPredictor@champion")
    return model