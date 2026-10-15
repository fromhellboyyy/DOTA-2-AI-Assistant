from unittest.mock import MagicMock, patch

import pytest

from app.ml import model as model_module


@pytest.fixture(autouse=True)
def reset_model_state():
    model_module._model = None
    yield
    model_module._model = None


def test_load_model_success_first_attempt() -> None:
    mock_model = MagicMock()
    with patch("app.ml.model.mlflow.pyfunc.load_model", return_value=mock_model):
        model_module.load_model()

    assert model_module._model is mock_model
    assert model_module.is_model_loaded() is True


def test_load_model_retries_then_succeeds() -> None:
    mock_model = MagicMock()
    call_count = {"n": 0}

    def side_effect(uri: str) -> MagicMock:
        call_count["n"] += 1
        if call_count["n"] < 3:
            raise ConnectionError("MLflow not ready")
        return mock_model

    with (
        patch("app.ml.model.mlflow.pyfunc.load_model", side_effect=side_effect),
        patch("app.ml.model.time.sleep") as mock_sleep,
    ):
        model_module.load_model()

    assert call_count["n"] == 3
    assert model_module._model is mock_model
    assert mock_sleep.call_count == 2  # две паузы между тремя попытками


def test_load_model_all_retries_fail() -> None:
    with (
        patch(
            "app.ml.model.mlflow.pyfunc.load_model",
            side_effect=ConnectionError("nope"),
        ),
        patch("app.ml.model.time.sleep"),
    ):
        model_module.load_model()

    assert model_module._model is None
    assert model_module.is_model_loaded() is False


def test_predict_returns_int() -> None:
    mock_model = MagicMock()
    mock_model.predict.return_value = [1]
    model_module._model = mock_model

    result = model_module.predict({"duration": 2400, "radiant_score": 30})

    assert result == 1
    mock_model.predict.assert_called_once()


def test_predict_returns_zero() -> None:
    mock_model = MagicMock()
    mock_model.predict.return_value = [0]
    model_module._model = mock_model

    result = model_module.predict({"duration": 1800})

    assert result == 0


def test_predict_raises_when_model_not_loaded() -> None:
    model_module._model = None

    with pytest.raises(RuntimeError, match="Model is not loaded"):
        model_module.predict({"duration": 2400})


def test_is_model_loaded_false_by_default() -> None:
    model_module._model = None
    assert model_module.is_model_loaded() is False
