from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.ml.model import is_model_loaded, predict

router = APIRouter(tags=["ml"])


class MatchFeatures(BaseModel):
    duration: float = Field(..., ge=0, description="Длительность матча в секундах")
    radiant_score: float = Field(..., ge=0, description="Счёт Radiant")
    dire_score: float = Field(..., ge=0, description="Счёт Dire")
    first_blood_time: float = Field(..., ge=0, description="Время первой крови (сек)")


class PredictionResponse(BaseModel):
    prediction: int
    meaning: str


@router.post("/process", response_model=PredictionResponse)
async def process(features: MatchFeatures) -> PredictionResponse:
    if not is_model_loaded():
        raise HTTPException(status_code=503, detail="Model is not loaded")

    prediction = predict(features.model_dump())
    meaning = "Radiant Win" if prediction == 1 else "Dire Win"

    return PredictionResponse(prediction=prediction, meaning=meaning)
