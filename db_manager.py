# db_manager.py
import os
from contextlib import contextmanager
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from post import Post

from loguru import logger
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    Integer,
    String,
    Text,
    UniqueConstraint,
    create_engine,
    select,
    update,
    func,
    and_,
)
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

# ------------------------------------------------------------
#  Логирование
# ------------------------------------------------------------
DB_LOGGER = logger.bind(channel="DB")

# добавляем file-sink один раз при первом импорте
if not any(
    getattr(h, "sink", None) == "logs/db_debug.log"
    for h in logger._core.handlers.values()
):
    logger.add(
        "logs/db_debug.log",
        rotation="10 MB",
        level="DEBUG",
        filter=lambda r: r["extra"].get("channel") == "DB",
    )


# ------------------------------------------------------------
#  ORM-база и модель
# ------------------------------------------------------------
class Base(DeclarativeBase):
    pass


class PostModel(Base):
    """Таблица публикаций (из RSS)."""

    __tablename__ = "posts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    post_id = Column(String(256), nullable=False)
    title = Column(String(512), nullable=False)
    content = Column(Text, nullable=False)
    blog_host = Column(String(256), nullable=False)
    blog_host_type = Column(Integer, nullable=False)
    url = Column(String(1024), nullable=False)
    published_on = Column(DateTime(timezone=True), nullable=False)
    simhash = Column(String(64), nullable=False)
    html_content = Column(Text, nullable=True)
    summary = Column(Text, nullable=True)

    relevance = Column(Boolean, nullable=True)
    relevance_score = Column(Float, nullable=True)
    category = Column(String(128), nullable=True)
    subcategory = Column(String(128), nullable=True)
    classification_confidence = Column(Float, nullable=True)

    created_at = Column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    __table_args__ = (
        UniqueConstraint("post_id", name="uq_posts_post_id"),
        UniqueConstraint("url", name="uq_posts_url"),
    )


# ------------------------------------------------------------
#  Менеджер работы с БД
# ------------------------------------------------------------
class DBManager:
    """Инкапсулирует работу с Postgres через SQLAlchemy."""

    def __init__(self, db_url: Optional[str] = None) -> None:
        db_url = db_url or os.getenv(
            "DATABASE_URL",
            "postgresql+psycopg2://postgres:postgres@postgres:5432/insightflow",
        )
        self.engine = create_engine(
            db_url, pool_pre_ping=True, pool_size=5, max_overflow=5
        )
        self.SessionLocal = sessionmaker(
            bind=self.engine, expire_on_commit=False, class_=Session
        )

        Base.metadata.create_all(self.engine)
        DB_LOGGER.info("DBManager инициализирован, metadata создана (если отсутствовала)")
        logger.debug("DB url in DBManager: %s", self.engine.url)

    def get_all_posts_urls(self) -> list:
        with self.SessionLocal() as session:
            posts = session.query(PostModel.url).all()
            return [p.url for p in posts if p.url]
    
    def save_posts_bulk(self, posts: List[Post]) -> None:
        """
        Сохраняет список объектов Post в базу данных (таблица PostModel).
        """
        if not posts:
            return
            
        saved = 0
        with self.SessionLocal() as session:
            for post in posts:
                try:
                    session.add(PostModel(
                        post_id=post.post_id,
                        title=post.title or "",
                        content=post.content or "",
                        blog_host=post.blog_host or "",
                        blog_host_type=post.blog_host_type.value if post.blog_host_type else 0,
                        url=post.url or "",
                        published_on=post.published_on,
                        simhash=post.simhash or "",
                        html_content=post.html_content,
                    ))
                    session.commit()
                    inserted += 1
                except Exception as e:
                    session.rollback()
                    logger.warning(f"❌ Ошибка при добавлении поста post_id={post.post_id}, url={post.url}")
                    logger.warning(f"Причина: {e}")

            
            session.commit()
            DB_LOGGER.info(f"save_posts_bulk: сохранено {saved} из {len(posts)} постов")

    def update_post_summaries(self, summaries: List[Dict[str, str]]) -> int:
        """
        Обновляет поле summary у постов по post_id.
        Пример входа:
        [
            {"post_id": "abc123", "summary": "Краткое содержание..."},
            ...
        ]
        """
        if not summaries:
            return 0

        updated = 0
        with self.session_scope() as session:
            for item in summaries:
                post_id = item.get("post_id")
                summary = item.get("summary", "").strip()
                if post_id and summary:
                    session.execute(
                        update(PostModel)
                        .where(PostModel.post_id == post_id)
                        .values(summary=summary)
                    )
                    updated += 1
        DB_LOGGER.info("Обновлено summary для %d постов", updated)
        return updated

# -----------------------------------------------------------
#  Обёртки для обратной совместимости
# -----------------------------------------------------------

    # 1. create_tables — вызывался в RSSManager / InsightFlow
    def create_tables(self):
        """Оставлено для старого кода — просто проксирует metadata.create_all()."""
        Base.metadata.create_all(self.engine)

    # 2. get_posts_by_date_range — расширенная сигнатура
    def get_posts_by_date_range(
        self,
        date_from: datetime,
        date_to: datetime,
        *,
        limit: int | None = None,
        only_relevant: bool = False,
        only_classified: bool = False,
    ):
        stmt = select(PostModel).where(
            PostModel.published_on.between(date_from, date_to)
        )
        if only_relevant:
            stmt = stmt.where(PostModel.relevance.is_(True))
        if only_classified:
            stmt = stmt.where(PostModel.category.is_not(None))
        stmt = stmt.order_by(PostModel.published_on.desc())
        if limit:
            stmt = stmt.limit(limit)

        with self.session_scope() as s:
            return s.scalars(stmt).all()

    # 3. batch-update для классификатора
    def update_posts_classification(
        self, mapping: dict[str, tuple[str, str, float]]
    ) -> int:
        """
        mapping = {post_id: (category, subcategory, confidence)}
        """
        if not mapping:
            return 0

        tmpl = (
            update(PostModel)
            .where(PostModel.post_id == func.bindparam("pid"))
            .values(
                category=func.bindparam("cat"),
                subcategory=func.bindparam("sub"),
                classification_confidence=func.bindparam("conf"),
            )
        )
        with self.session_scope() as s:
            for pid, (cat, sub, conf) in mapping.items():
                s.execute(tmpl.params(pid=pid, cat=cat, sub=sub, conf=conf))
        return len(mapping)

    
    # ------------------------- helpers -------------------------

    @contextmanager
    def session_scope(self):
        """Контекстный менеджер сессии: commit / rollback / close."""
        session: Session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    # -----------------------------------------------------------
    #  Сохранение постов
    # -----------------------------------------------------------
    def save_posts(self, posts: List["Post"]) -> int:
        """Принимает список DTO-постов (из post.py) и вставляет новые записи."""
        if not posts:
            return 0

        # превращаем DTO в ORM-объекты
        orm_objects = [
            PostModel(
                post_id=p.post_id,
                title=p.title,
                content=p.content,
                blog_host=p.blog_host,
                blog_host_type=p.blog_host_type.value,
                html_content=p.html_content,
                url=p.url,
                published_on=p.published_on,
                simhash=p.simhash,
            )
            for p in posts
        ]

        inserted = 0
        with self.session_scope() as session:
            try:
                # bulk-insert; при конфликте по unique получим IntegrityError
                session.bulk_save_objects(orm_objects, return_defaults=False)
                inserted = len(session.new)
            except IntegrityError as exc:
                DB_LOGGER.warning(
                    "IntegrityError bulk-insert (%s). Переходим на поштучную вставку",
                    exc.orig,
                )
                session.rollback()
                for obj in orm_objects:
                    try:
                        session.add(obj)
                        session.flush()  # проверка на уникальность
                        inserted += 1
                    except IntegrityError:
                        session.rollback()  # дубликат — пропускаем
            DB_LOGGER.info("save_posts: вставлено %s / %s", inserted, len(posts))
        return inserted

    # -----------------------------------------------------------
    #  Чтение
    # -----------------------------------------------------------
    def get_posts_by_date_range(
        self, date_from: datetime, date_to: datetime
    ) -> List[PostModel]:
        """Все посты, опубликованные в указанном интервале (включительно)."""
        stmt = (
            select(PostModel)
            .where(
                and_(
                    PostModel.published_on >= date_from,
                    PostModel.published_on <= date_to,
                )
            )
            .order_by(PostModel.published_on.desc())
        )

        with self.session_scope() as session:
            rows = session.scalars(stmt).all()
            DB_LOGGER.debug(
                "get_posts_by_date_range: %s строк (%s – %s)", len(rows), date_from, date_to
            )
            return rows

    def get_categories_statistics(self, date_from=None, date_to=None, only_relevant=False) -> dict:
        """
        Возвращает словарь {категория: количество постов} за указанный период.
        Если даты не указаны — возвращает по всей базе.
        Если only_relevant=True — фильтрует только релевантные посты.
        """
        with self.SessionLocal() as session:
            query = session.query(PostModel.category)

            if date_from and date_to:
                query = query.filter(PostModel.published_on.between(date_from, date_to))

            if only_relevant:
                query = query.filter(PostModel.relevance.is_(True))

            categories = query.all()
            stats = {}

            for cat, in categories:
                if cat:
                    stats[cat] = stats.get(cat, 0) + 1

            return stats



    def get_unchecked_posts(self, limit: int = None) -> List[PostModel]:
        """Посты, где relevance ещё не определён."""
        stmt = (
            select(PostModel)
            .where(PostModel.relevance.is_(None))
            .order_by(PostModel.published_on.desc())
            .limit(limit)
        )
        with self.session_scope() as session:
            rows = session.scalars(stmt).all()
            DB_LOGGER.debug("get_unchecked_posts: %s строк (limit=%s)", len(rows), limit)
            return rows

    def get_relevant_unclassified_posts(self, limit: Optional[int] = None):
        stmt = select(PostModel).where(
            PostModel.relevance.is_(True),
            PostModel.relevance_score >= 0.7,
            PostModel.category.is_(None)
        ).order_by(PostModel.published_on.desc())

        if limit is not None:
            stmt = stmt.limit(limit)

        with self.session_scope() as session:
            return session.scalars(stmt).all()

    def get_relevant_posts(
        self,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
        limit: int = 100,
    ) -> List[PostModel]:
        """Релевантные и уже классифицированные посты (для дайджеста)."""
        stmt = select(PostModel).where(
            PostModel.relevance.is_(True), PostModel.category.is_not(None)
        )
        if since is not None:
            stmt = stmt.where(PostModel.published_on >= since)
        if until is not None:
            stmt = stmt.where(PostModel.published_on <= until)
        stmt = stmt.order_by(PostModel.published_on.desc()).limit(limit)

        with self.session_scope() as session:
            rows = session.scalars(stmt).all()
            DB_LOGGER.debug(
                "get_relevant_posts: %s строк (range=%s–%s, limit=%s)",
                len(rows),
                since,
                until,
                limit,
            )
            return rows

    def create_post_mapping_from_db(self, posts: List[PostModel]) -> Dict[str, str]:
        """
        Создает словарь {post_id: url} из списка постов.
        
        Args:
            posts: список объектов PostModel из базы данных
            
        Returns:
            dict: словарь маппинга post_id -> url
        """
        mapping = {}
        for post in posts:
            if post.post_id and post.url:
                mapping[post.post_id] = post.url
                DB_LOGGER.debug(f"Добавлен в mapping: {post.post_id[:8]}... -> {post.url[:50]}...")
        
        DB_LOGGER.info(f"Создан post_mapping с {len(mapping)} элементами")
        return mapping

    def create_tables(self):
        Base.metadata.create_all(self.engine)

    def get_posts_by_date_range(
        self, date_from, date_to,
        limit: int | None = None,
        only_relevant: bool = False,
        only_classified: bool = False,
    ):
        stmt = select(PostModel).where(
            PostModel.published_on.between(date_from, date_to)
        )
        if only_relevant:
            stmt = stmt.where(PostModel.relevance.is_(True))
        if only_classified:
            stmt = stmt.where(PostModel.category.is_not(None))
        if limit:
            stmt = stmt.limit(limit)
        stmt = stmt.order_by(PostModel.published_on.desc())
        with self.session_scope() as s:
            return s.scalars(stmt).all()

    # -----------------------------------------------------------
    #  Batch-update
    # -----------------------------------------------------------
    def update_posts_relevance_batch(
        self,
        results: dict[str, tuple[bool, float]],
    ) -> int:
        """
        results = {post_id: (relevance_bool, relevance_score)}
        """
        if not results:
            return 0

        with self.session_scope() as s:
            for pid, (rel, score) in results.items():
                s.execute(
                    update(PostModel)
                    .where(PostModel.post_id == pid)
                    .values(relevance=rel, relevance_score=score)
                )
        return len(results)


    def update_posts_classification(
        self,
        mapping: dict[str, tuple[str, str, float]],
    ) -> int:
        """
        mapping = {post_id: (category, subcategory, confidence)}
        """
        if not mapping:
            return 0

        with self.session_scope() as s:
            for pid, (cat, sub, conf) in mapping.items():
                s.execute(
                    update(PostModel)
                    .where(PostModel.post_id == pid)
                    .values(
                        category=cat,
                        subcategory=sub,
                        classification_confidence=conf,
                    )
                )
        return len(mapping)


    def update_posts_category_batch(self, mapping: Dict[str, str]) -> int:
        """`mapping = {post_id: category}`."""
        if not mapping:
            return 0
        stmt_template = (
            update(PostModel)
            .where(PostModel.post_id == func.bindparam("pid"))
            .values(category=func.bindparam("cat"))
        )
        with self.session_scope() as session:
            for pid, cat in mapping.items():
                session.execute(stmt_template.params(pid=pid, cat=cat))
            DB_LOGGER.debug("update_posts_category_batch: %s строк", len(mapping))
            return len(mapping)
        # batch-update трёх полей за один проход
    

    # -------------------------- stubs --------------------------
    def create_partition_if_not_exists(self):
        """Заглушка: партиционирование по дате публикации."""
        pass

    def create_indexes_if_not_exists(self):
        """Заглушка: дополнительные индексы."""
        pass

    def add_rss_sources(self, sources: List[Dict]):
        """Заглушка под отдельную таблицу `rss_sources`."""
        pass

    def delete_irrelevant_posts(self):
        from sqlalchemy import delete
        from models import PostModel

        stmt = delete(PostModel).where(PostModel.relevance.is_(False))
        with self.session_scope() as session:
            result = session.execute(stmt)
            DB_LOGGER.info(f"Удалено {result.rowcount} нерелевантных постов")
