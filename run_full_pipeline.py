import asyncio
from scheduler import hourly_pipeline
from insightflow_service import InsightFlow

async def run_full_pipeline():
    print("🚀 Запуск полного пайплайна: RSS → фильтрация → релевантность → классификация → суммаризация → Telegram")

    # 1. Получаем новые посты, фильтруем, сохраняем, проверяем релевантность, классифицируем
    await hourly_pipeline()

    # 2. Отправляем дайджест в Telegram (использует релевантные и классифицированные посты)
    service = InsightFlow()
    await service.run_daily_job()

    print("✅ Полный пайплайн завершён")

if __name__ == "__main__":
    asyncio.run(run_full_pipeline())
