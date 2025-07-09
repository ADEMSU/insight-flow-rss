import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from collections import defaultdict
from loguru import logger

# Импортируем существующий класс Post
from post import Post
from token_estimator import TokenEstimator

class TextPreprocessor:
    def __init__(self, similarity_threshold=0.85, min_content_length=100, max_tokens=27000):
        """
        Инициализирует предпроцессор текста
        
        Args:
            similarity_threshold (float): Порог сходства для удаления дубликатов (0-1)
            min_content_length (int): Минимальная длина содержимого для обработки
            max_tokens (int): Максимальное количество токенов, которое должны занимать посты
        """
        self.similarity_threshold = similarity_threshold
        self.min_content_length = min_content_length
        self.max_tokens = max_tokens
        self.token_estimator = TokenEstimator()
        
        # Увеличиваем богатство векторизации
        self.vectorizer = TfidfVectorizer(
            min_df=1, 
            stop_words=None,
            lowercase=True,
            analyzer='word',
            ngram_range=(1, 3),  # униграммы, биграммы и триграммы
            max_features=5000,
            token_pattern=r'(?u)\b\w\w+\b'  # более мягкий шаблон для токенизации
        )
    
    def _filter_by_similarity(self, texts, post_ids=None):
        """
        Фильтрует тексты по косинусному сходству на основе TF-IDF
        
        Args:
            texts (list): Список текстов для фильтрации
            post_ids (list, optional): Список идентификаторов постов
            
        Returns:
            list: Список уникальных текстов
            list: Список идентификаторов соответствующих постов
        """
        if not texts:
            return [], []
            
        # Используем post_ids, если они предоставлены, иначе создаем фиктивные id
        if post_ids is None:
            post_ids = [f"post_{i}" for i in range(len(texts))]
        
        # Фильтруем слишком короткие тексты
        filtered_indices = []
        filtered_texts = []
        filtered_post_ids = []
        
        for i, text in enumerate(texts):
            if len(text) >= self.min_content_length:
                filtered_indices.append(i)
                filtered_texts.append(text)
                filtered_post_ids.append(post_ids[i])
            else:
                logger.info(f"Пропускаем пост {post_ids[i]}: слишком короткий текст ({len(text)} символов)")
        
        if not filtered_texts:
            logger.warning("После фильтрации по длине текста не осталось постов")
            return [], []
        
        # Создаем tf-idf матрицу
        try:
            tfidf_matrix = self.vectorizer.fit_transform(filtered_texts)
        except Exception as e:
            logger.error(f"Ошибка при создании TF-IDF матрицы: {e}")
            return filtered_texts, filtered_post_ids
            
        # Вычисляем матрицу сходства
        cosine_sim = cosine_similarity(tfidf_matrix)
        
        # Начинаем с первого документа и выбираем только уникальные
        unique_indices = []
        
        # Сначала сортируем тексты по длине (от большей к меньшей)
        # Это поможет нам предпочесть более длинные и информативные тексты
        length_with_indices = [(len(filtered_texts[i]), i) for i in range(len(filtered_texts))]
        length_with_indices.sort(reverse=True)
        sorted_indices = [x[1] for x in length_with_indices]
        
        for i in sorted_indices:
            # Проверяем, похож ли текст на любой из уже выбранных текстов
            is_unique = True
            for unique_idx in unique_indices:
                if cosine_sim[i, unique_idx] > self.similarity_threshold:
                    is_unique = False
                    logger.info(f"Пост {filtered_post_ids[i]} похож на пост {filtered_post_ids[unique_idx]} (сходство: {cosine_sim[i, unique_idx]:.2f})")
                    break
                    
            if is_unique:
                unique_indices.append(i)
        
        # Получаем уникальные тексты и их идентификаторы
        unique_texts = [filtered_texts[i] for i in unique_indices]
        unique_post_ids = [filtered_post_ids[i] for i in unique_indices]
        
        logger.info(f"Найдено {len(unique_texts)} уникальных текстов из {len(filtered_texts)} общих")
        return unique_texts, unique_post_ids
    
    def filter_by_simhash_and_similarity(self, posts, min_batches=2):
        """
        Фильтр 1: Разделяет посты по simhash и применяет фильтрацию по сходству внутри каждого батча
        
        Args:
            posts (list): Список объектов Post
            min_batches (int): Минимальное количество батчей
            
        Returns:
            list: Список отфильтрованных постов
        """
        if not posts:
            return []
            
        # Предварительная фильтрация постов с очень коротким или пустым контентом
        filtered_by_length = []
        for post in posts:
            content_length = len(post.content or "") + len(post.title or "")
            if content_length >= self.min_content_length:
                filtered_by_length.append(post)
            else:
                logger.info(f"Отфильтрован пост {post.post_id} по длине текста ({content_length} символов)")
        
        if len(filtered_by_length) < len(posts):
            logger.info(f"Отфильтровано {len(posts) - len(filtered_by_length)} постов из {len(posts)} по длине текста")
            posts = filtered_by_length
            
        if not posts:
            logger.warning("После фильтрации по длине текста не осталось постов")
            return []
        
        # Группируем по simhash
        simhash_groups = defaultdict(list)
        for post in posts:
            # Если simhash отсутствует, используем дефолтное значение
            simhash = post.simhash if post.simhash else "default_simhash"
            simhash_groups[simhash].append(post)
        
        # Если количество групп меньше min_batches, создаем дополнительные группы
        if len(simhash_groups) < min_batches:
            logger.info(f"Недостаточно групп simhash ({len(simhash_groups)}), создаем дополнительные группы")
            
            # Сортируем группы по размеру (от большей к меньшей)
            items = sorted(simhash_groups.items(), key=lambda x: len(x[1]), reverse=True)
            
            # Разделяем самую большую группу
            if items and len(items[0][1]) > 1:
                largest_group_key = items[0][0]
                largest_group = simhash_groups[largest_group_key]
                
                # Разделяем группу пополам
                mid = len(largest_group) // 2
                simhash_groups[largest_group_key] = largest_group[:mid]
                simhash_groups[f"{largest_group_key}_split"] = largest_group[mid:]
        
        # Применяем фильтрацию по сходству внутри каждой группы
        filtered_posts = []
        
        for simhash, group_posts in simhash_groups.items():
            logger.info(f"Обработка группы simhash {simhash} с {len(group_posts)} постами")
            
            # Подготавливаем тексты и идентификаторы для этой группы
            group_texts = []
            group_post_ids = []
            
            for post in group_posts:
                # Объединяем заголовок и содержимое для анализа
                post_text = f"{post.title or ''} {post.content or ''}".strip()
                if post_text:  # Исключаем пустые тексты
                    group_texts.append(post_text)
                    group_post_ids.append(post.post_id)
            
            # Фильтруем тексты внутри группы
            _, unique_post_ids = self._filter_by_similarity(group_texts, group_post_ids)
            
            # Добавляем отфильтрованные посты
            for post in group_posts:
                if post.post_id in unique_post_ids:
                    filtered_posts.append(post)
        
        logger.info(f"После фильтра 1: осталось {len(filtered_posts)} постов из {len(posts)} исходных")
        return filtered_posts
    
    def filter_by_global_similarity(self, posts):
        """
        Фильтр 2: Применяет фильтрацию по сходству ко всем оставшимся постам
        с более низким порогом сходства для более агрессивной фильтрации,
        но сохраняет посты с высокой релевантностью (score > 0.7)
        """
        if not posts:
            return []

        # Подготовка текстов
        all_texts = []
        all_post_ids = []
        id_to_post = {}

        for post in posts:
            post_text = f"{post.title or ''} {post.content or ''}".strip()
            if post_text:
                all_texts.append(post_text)
                all_post_ids.append(post.post_id)
                id_to_post[post.post_id] = post

        # Сохраняем ID релевантных постов (score > 0.7)
        protected_ids = set()
        for post in posts:
            if hasattr(post, "relevance_score") and (post.relevance_score or 0.0) > 0.7:
                protected_ids.add(post.post_id)

        # Временное понижение порога
        stricter_threshold = self.similarity_threshold - 0.05
        original_threshold = self.similarity_threshold
        self.similarity_threshold = max(0.6, stricter_threshold)

        logger.info(f"Глобальная фильтрация с порогом сходства: {self.similarity_threshold} (было {original_threshold})")

        _, unique_post_ids = self._filter_by_similarity(all_texts, all_post_ids)

        # Восстановление порога
        self.similarity_threshold = original_threshold

        # Добавляем защищённые ID обратно
        final_ids = set(unique_post_ids).union(protected_ids)

        filtered_posts = [id_to_post[pid] for pid in final_ids if pid in id_to_post]

        logger.info(f"После фильтра 2: осталось {len(filtered_posts)} постов из {len(posts)} после фильтра 1")

        if len(filtered_posts) < 10 and len(posts) > 10:
            logger.warning("Фильтрация слишком агрессивна, добавляем топ-10 самых длинных постов")
            sorted_posts = sorted(posts, key=lambda p: len(p.content or "") + len(p.title or ""), reverse=True)
            return sorted_posts[:10]

        return filtered_posts


    def limit_tokens(self, posts, prompt_example=""):
        """
        Ограничивает количество постов так, чтобы их токены не превышали заданный лимит
        
        Args:
            posts (list): Список объектов Post
            prompt_example (str): Примерный текст промпта для учета его токенов
            
        Returns:
            list: Список постов, вписывающихся в ограничение токенов
        """
        if not posts:
            return []
            
        # Сначала сортируем посты по длине (от большего к меньшему)
        sorted_posts = sorted(posts, key=lambda p: len(p.content or "") + len(p.title or ""), reverse=True)
        
        # Оцениваем токены промпта
        prompt_tokens = self.token_estimator.estimate_tokens(prompt_example)
        logger.info(f"Токены промпта: ~{prompt_tokens}")
        
        # Токены доступные для постов
        available_tokens = self.max_tokens - prompt_tokens - 3000  # Резерв для ответа
        logger.info(f"Доступно токенов для постов: ~{available_tokens} из {self.max_tokens}")
        
        # Оцениваем токены каждого поста и добавляем до достижения лимита
        limited_posts = []
        current_tokens = 0
        
        for post in sorted_posts:
            post_text = f"[POST_ID:{post.post_id}] [{post.blog_host}] {post.title or ''}\n{post.content or ''}"
            post_tokens = self.token_estimator.estimate_tokens(post_text)
            
            if current_tokens + post_tokens <= available_tokens:
                limited_posts.append(post)
                current_tokens += post_tokens
                logger.info(f"Добавлен пост: {post.post_id} (~{post_tokens} токенов, всего: {current_tokens})")
            else:
                logger.info(f"Пост не вошел в лимит: {post.post_id} (~{post_tokens} токенов)")
        
        logger.info(f"Выбрано {len(limited_posts)} постов из {len(posts)} с общим размером ~{current_tokens} токенов")
        return limited_posts

    def process_posts(self, posts):
        """
        Применяет оба фильтра последовательно с улучшенной логикой
        и учетом ограничения по токенам
        
        Args:
            posts (list): Исходный список объектов Post
            
        Returns:
            list: Финальный отфильтрованный список постов
        """
        # Фильтр 1: Группировка по simhash и фильтрация внутри групп
        filtered_by_simhash = self.filter_by_simhash_and_similarity(posts)
        
        # Фильтр 2: Глобальная фильтрация всех оставшихся постов
        filtered_posts = self.filter_by_global_similarity(filtered_by_simhash)
        
        # Примерный шаблон промпта для учета токенов
        prompt_example = """
        Проанализируй следующие упоминания из СМИ на русском языке. Нужно:
        1. Выделить наиболее значимые сюжеты. Значимый сюжет - это тот который может повлиять на мой бизнес в области репутационного консалтинга
        2. Для каждого сюжета сделать краткое содержание (2-3 предложения раскрывающие смысл)
        3. Обязательно указать POST_ID исходной статьи или статей, откуда взят сюжет
        4. Предложить идеи для контент-плана на основе сюжета, план должен быть применим в области репутационного консалтинга. Включай только идеи тем для публикаций в блоге

        Упоминания:
        [ТЕКСТЫ ПОСТОВ БУДУТ ЗДЕСЬ]

        Верни результат строго в следующем формате для каждого сюжета:
        СЮЖЕТ: [Название сюжета]
        СОДЕРЖАНИЕ: [Краткое содержание, 2-3 предложения]
        POST_ID: [ID поста-источника, выбери наиболее релевантный пост для сюжета]
        ПРЕДЛОЖЕНИЯ ДЛЯ КОНТЕНТ-ПЛАНА: [Предложения для контент-плана]
        """
        
        # Ограничиваем количество постов по токенам
        #final_filtered_posts = self.limit_tokens(filtered_posts, prompt_example)
        
        logger.info(f"Итоговое количество постов после обработки: {len(filtered_posts)}")
        return filtered_posts
    
    def remove_duplicates(self, posts: list[dict]) -> list[dict]:
        """
        Удаляет дубликаты по смыслу из списка словарей с ключами 'title' и 'content'.
        Использует TF-IDF + cosine similarity.
        """
        if not posts:
            return []
        texts = [f"{p['title']} {p['content']}".strip() for p in posts]
        vectorizer = self.vectorizer.fit_transform(texts)
        cosine_sim = cosine_similarity(vectorizer)
        np.fill_diagonal(cosine_sim, 0)

        unique_indices = []
        for i in range(len(posts)):
            if all(cosine_sim[i, j] < self.similarity_threshold for j in unique_indices):
                unique_indices.append(i)

        logger.info(f"Удалено дубликатов: {len(posts) - len(unique_indices)} из {len(posts)}")
        return [posts[i] for i in unique_indices]

