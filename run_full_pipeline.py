import asyncio
from insightflow_service import InsightFlow

async def run_full_pipeline():
    print("🚀 Запуск полного пайплайна: RSS → фильтрация → релевантность → классификация → суммаризация → Telegram")

    service = InsightFlow()

    # 1. Ежечасная часть: загрузка и разметка новых постов
    await service.run_hourly_job()

    # 2. Ежедневная часть: дайджест по релевантным постам
    #await service.run_daily_job()

    print("✅ Полный пайплайн завершён")

if __name__ == "__main__":
    asyncio.run(run_full_pipeline())
