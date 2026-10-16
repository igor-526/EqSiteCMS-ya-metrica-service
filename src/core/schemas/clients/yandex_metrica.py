from typing import Any


def validate_counters_response(response: Any) -> dict:
    """Проверить только оболочку, сохранив исходные rows и отдельные элементы.

    Числовые строки и прежняя int-конвертация rows допустимы. Некорректные
    счётчики пропускает сервис; здесь они не делают весь ответ непригодным.
    """
    if not isinstance(response, dict):
        raise ValueError("Expected a counters object")
    if not isinstance(response.get("counters"), list):
        raise ValueError("Expected a counters list")
    int(response["rows"])
    return response
