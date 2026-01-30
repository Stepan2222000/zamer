"""
Абстракция AI провайдеров для валидации объявлений.

Базовый класс AIValidationProvider определяет интерфейс для всех провайдеров.
DummyProvider — заглушка, которая пропускает все объявления без проверки.
"""

import logging
import base64
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class ListingForValidation:
    """Объявление для AI валидации."""
    avito_item_id: str
    title: str
    price: Optional[float]
    snippet_text: Optional[str]
    seller_name: Optional[str]
    images_bytes: List[bytes] = field(default_factory=list)

    def to_dict(self) -> Dict:
        """Конвертация в словарь для JSON сериализации (без изображений)."""
        return {
            'id': self.avito_item_id,
            'title': self.title,
            'price': self.price,
            'snippet': self.snippet_text,
            'seller': self.seller_name,
        }

    def get_images_base64(self, max_images: int = 2) -> List[str]:
        """
        Возвращает изображения в формате base64.

        Args:
            max_images: Максимальное количество изображений для возврата.

        Returns:
            Список строк base64-encoded изображений.
        """
        result = []
        for img_bytes in self.images_bytes[:max_images]:
            if img_bytes:
                result.append(base64.b64encode(img_bytes).decode('utf-8'))
        return result


@dataclass
class RejectedListing:
    """Отклонённое объявление с причиной."""
    avito_item_id: str
    reason: str


@dataclass
class ValidationResult:
    """Результат валидации группы объявлений."""
    passed_ids: List[str]
    rejected: List[RejectedListing]

    @property
    def passed_count(self) -> int:
        return len(self.passed_ids)

    @property
    def rejected_count(self) -> int:
        return len(self.rejected)


class AIValidationProvider(ABC):
    """
    Базовый класс для AI провайдеров валидации.

    Все провайдеры должны реализовать:
    - validate() — основной метод валидации
    - close() — освобождение ресурсов
    """

    @abstractmethod
    async def validate(
        self,
        articulum: str,
        listings: List[ListingForValidation],
        use_images: bool = True
    ) -> ValidationResult:
        """
        Валидация списка объявлений для артикула.

        Args:
            articulum: Артикул для проверки.
            listings: Список объявлений для валидации.
            use_images: Использовать ли изображения в валидации.

        Returns:
            ValidationResult с passed_ids и rejected списками.

        Raises:
            AIProviderError: При ошибках API.
        """
        pass

    @abstractmethod
    async def close(self):
        """Освобождение ресурсов (HTTP клиенты, сессии и т.д.)"""
        pass

    def __str__(self) -> str:
        return self.__class__.__name__


class AIProviderError(Exception):
    """Ошибка AI провайдера — артикул нужно вернуть в очередь."""
    pass


class DummyProvider(AIValidationProvider):
    """
    Заглушка провайдера — пропускает все объявления без проверки.

    Используется для:
    - Тестирования пайплайна без настройки AI
    - Режима работы когда AI временно недоступен
    """

    def __init__(self):
        logger.info("DummyProvider инициализирован — все объявления будут пропущены")

    async def validate(
        self,
        articulum: str,
        listings: List[ListingForValidation],
        use_images: bool = True
    ) -> ValidationResult:
        """
        Пропускает все объявления без проверки.

        Логирует информацию о том, что было бы обработано.
        """
        passed_ids = [listing.avito_item_id for listing in listings]

        # Логирование для отладки
        images_info = ""
        if use_images:
            total_images = sum(len(l.images_bytes) for l in listings)
            images_info = f", изображений: {total_images}"

        logger.info(
            f"DummyProvider: пропущено {len(passed_ids)} объявлений "
            f"для артикула '{articulum}'{images_info}"
        )

        return ValidationResult(
            passed_ids=passed_ids,
            rejected=[]
        )

    async def close(self):
        """Ничего не делает — нет ресурсов для освобождения."""
        pass


def create_provider(provider_type: str = "dummy") -> AIValidationProvider:
    """
    Фабричный метод для создания AI провайдера.

    Args:
        provider_type: Тип провайдера ("dummy", "gemini", "openai", "custom").

    Returns:
        Экземпляр AIValidationProvider.

    Raises:
        ValueError: Если тип провайдера не поддерживается.
    """
    providers = {
        "dummy": DummyProvider,
        # Будущие провайдеры:
        # "gemini": GeminiProvider,
        # "openai": OpenAIProvider,
        # "claude": ClaudeProvider,
        # "custom": CustomProvider,
    }

    if provider_type not in providers:
        available = ", ".join(providers.keys())
        raise ValueError(f"Неизвестный тип провайдера: '{provider_type}'. Доступные: {available}")

    return providers[provider_type]()


def convert_listing_dict_to_validation(
    listing: Dict,
    max_images: int = 5
) -> ListingForValidation:
    """
    Конвертирует словарь объявления в ListingForValidation.

    Args:
        listing: Словарь с данными объявления из БД.
        max_images: Максимальное количество изображений.

    Returns:
        ListingForValidation для передачи в провайдер.
    """
    # Получаем изображения
    images_bytes_raw = listing.get('images_bytes') or []
    images_bytes = images_bytes_raw[:max_images] if images_bytes_raw else []

    # Конвертируем price из Decimal в float
    price = listing.get('price')
    if price is not None:
        price = float(price)

    return ListingForValidation(
        avito_item_id=listing['avito_item_id'],
        title=listing.get('title', ''),
        price=price,
        snippet_text=listing.get('snippet_text'),
        seller_name=listing.get('seller_name'),
        images_bytes=images_bytes,
    )
