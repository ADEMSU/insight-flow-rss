print("Starting test_scheduler.py...")

try:
    print("Importing modules...")
    import sys
    print(f"Python version: {sys.version}")
    
    import os
    print(f"LM_STUDIO_URL: {os.getenv('LM_STUDIO_URL')}")
    
    print("Importing schedule...")
    import schedule
    print("Schedule imported OK")
    
    print("Importing loguru...")
    from loguru import logger
    print("Loguru imported OK")
    
    print("Importing asyncio...")
    import asyncio
    print("Asyncio imported OK")
    
    print("Trying to import rss_manager...")
    from rss_manager import RSSManager
    print("RSSManager imported OK")
    
    print("Trying to import relevance_checker...")
    from relevance_checker import check_relevance_task
    print("relevance_checker imported OK")
    
    print("Trying to import content_classifier...")
    from content_classifier import classify_relevant_posts_task
    print("content_classifier imported OK")
    
    print("Trying to import insightflow_service...")
    from insightflow_service import run_insight_flow
    print("insightflow_service imported OK")
    
    print("All imports successful!")
    
except Exception as e:
    print(f"ERROR during import: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()