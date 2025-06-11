import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import re
from typing import List, Dict, Any, Tuple
from loguru import logger
import binascii

class BatchManager:
    """
    Класс для управления батчами постов с группировкой по simhash
    и двухэтапной дедупликацией
    """
    
    def __init__(self, stopwords=None, min_batches=2, similarity_threshold=0.65):
        """
        Инициализирует менеджер батчей
        
        Args:
            stopwords: список стоп-слов для TF-IDF векторизации
            min_batches: минимальное количество создаваемых батчей
            similarity_threshold: порог сходства для определения дубликатов
        """
        self.stopwords = stopwords or []
        self.min_batches = min_batches
        self.similarity_threshold = similarity_threshold
        self.vectorizer = None
        
    def normalize_text(self, text):
        """
        Нормализует текст для анализа
        """
        if not text:
            return ""
        # Приведение к нижнему регистру
        text = text.lower()
        # Удаление URL
        text = re.sub(r'https?://\S+', '', text)
        # Удаление HTML-тегов
        text = re.sub(r'<.*?>', '', text)
        # Удаление повторяющихся символов
        text = re.sub(r'([!?\.,:;])\1+', r'\1', text)
        # Замена множественных пробелов на один
        text = re.sub(r'\s+', ' ', text)
        # Удаление спецсимволов
        text = re.sub(r'[^\w\s]', '', text)
        return text.strip()
    
    def get_simhash_distance(self, hash1, hash2):
        """
        Вычисляет расстояние Хэмминга между двумя simhash значениями
        
        Args:
            hash1: первый хэш (строка или число)
            hash2: второй хэш (строка или число)
            
        Returns:
            int: расстояние Хэмминга (меньше - более похожие тексты)
        """
        try:
            # Преобразуем строковые представления хэшей в числа, если нужно
            if isinstance(hash1, str):
                try:
                    hash1_int = int(hash1, 16) if '0x' in hash1 else int(hash1)
                except ValueError:
                    # Если строка не может быть преобразована в число напрямую
                    hash1_int = int(binascii.hexlify(hash1.encode()), 16)
            else:
                hash1_int = hash1
                
            if isinstance(hash2, str):
                try:
                    hash2_int = int(hash2, 16) if '0x' in hash2 else int(hash2)
                except ValueError:
                    hash2_int = int(binascii.hexlify(hash2.encode()), 16)
            else:
                hash2_int = hash2
            
            # Считаем расстояние Хэмминга (XOR + подсчет битов)
            xor_result = hash1_int ^ hash2_int
            distance = bin(xor_result).count('1')
            return distance
        except Exception as e:
            logger.error(f"Ошибка при вычислении расстояния simhash: {e}")
            # Возвращаем максимальное расстояние в случае ошибки
            return 64
    
    def group_posts_by_simhash(self, posts, max_distance=16):
        """
        Группирует посты по сходству simhash
        
        Args:
            posts: список объектов Post
            max_distance: максимальное расстояние Хэмминга для группировки
            
        Returns:
            List[List]: список групп постов
        """
        if not posts:
            return []
            
        # Группируем посты с simhash
        posts_with_simhash = [post for post in posts if post.simhash]
        posts_without_simhash = [post for post in posts if not post.simhash]
        
        logger.info(f"Найдено {len(posts_with_simhash)} постов с simhash и {len(posts_without_simhash)} без simhash")
        
        # Если нет постов с simhash, используем альтернативное разделение
        if not posts_with_simhash:
            return self.alternative_grouping(posts)
            
        # Группировка постов с simhash по близости
        groups = []
        assigned = set()
        
        # Первый проход: формируем начальные группы
        for i, post in enumerate(posts_with_simhash):
            if i in assigned:
                continue
                
            group = [post]
            assigned.add(i)
            
            for j, other_post in enumerate(posts_with_simhash):
                if j in assigned or i == j:
                    continue
                    
                distance = self.get_simhash_distance(post.simhash, other_post.simhash)
                if distance <= max_distance:
                    group.append(other_post)
                    assigned.add(j)
            
            groups.append(group)
        
        # Распределяем посты без simhash по существующим группам
        # на основе TF-IDF сходства
        if posts_without_simhash and groups:
            self.distribute_posts_without_simhash(posts_without_simhash, groups)
        elif posts_without_simhash:
            # Если нет групп, но есть посты без simhash, создаем группу из них
            groups.append(posts_without_simhash)
        
        # Проверяем, что получилось достаточное количество групп
        if len(groups) < self.min_batches and len(groups) > 0:
            logger.info(f"Получено только {len(groups)} групп, разделяем их дополнительно")
            new_groups = []
            
            # Разделяем крупные группы, чтобы достичь минимального количества
            for group in sorted(groups, key=len, reverse=True):
                if len(new_groups) >= self.min_batches:
                    # Если уже достигли нужного числа групп, добавляем остаток как есть
                    new_groups.append(group)
                    break
                    
                if len(group) > 1:
                    # Делим группу пополам
                    mid = len(group) // 2
                    new_groups.append(group[:mid])
                    new_groups.append(group[mid:])
                else:
                    new_groups.append(group)
            
            groups = new_groups
        
        logger.info(f"Сформировано {len(groups)} групп постов по simhash")
        for i, group in enumerate(groups):
            logger.info(f"Группа {i+1}: {len(group)} постов")
            
        return groups
    
    def alternative_grouping(self, posts, num_groups=None):
        """
        Альтернативное разделение постов на группы, если simhash недоступен
        
        Args:
            posts: список объектов Post
            num_groups: желаемое количество групп (если None, используется self.min_batches)
            
        Returns:
            List[List]: список групп постов
        """
        if not posts:
            return []
            
        if num_groups is None:
            num_groups = self.min_batches
        
        # Если постов меньше, чем нужно групп, создаем группы по одному посту
        if len(posts) <= num_groups:
            return [[post] for post in posts]
        
        # Распределяем посты по группам на основе длины контента
        posts_sorted = sorted(posts, key=lambda p: len(p.content or ""))
        
        # Распределяем посты по группам так, чтобы длинные и короткие были распределены равномерно
        groups = [[] for _ in range(num_groups)]
        
        # Распределение постов зигзагом
        for i, post in enumerate(posts_sorted):
            group_idx = i % num_groups
            groups[group_idx].append(post)
        
        logger.info(f"Альтернативное разделение: создано {len(groups)} групп")
        for i, group in enumerate(groups):
            logger.info(f"Группа {i+1}: {len(group)} постов")
            
        return groups
    
    def distribute_posts_without_simhash(self, posts_without_simhash, groups):
        """
        Распределяет посты без simhash по существующим группам на основе TF-IDF сходства
        
        Args:
            posts_without_simhash: список постов без simhash
            groups: список групп постов
        """
        if not posts_without_simhash or not groups:
            return
            
        # Инициализируем векторизатор, если еще не сделано
        if self.vectorizer is None:
            self.vectorizer = TfidfVectorizer(
                analyzer='word',
                lowercase=True,
                stop_words=self.stopwords,
                token_pattern=r'\b[а-яА-Яa-zA-Z0-9]{2,}\b',
                max_features=5000
            )
        
        # Подготавливаем тексты для каждой группы
        group_texts = []
        for group in groups:
            group_content = []
            for post in group:
                content = self.normalize_text((post.content or "") + " " + (post.title or ""))
                if content:
                    group_content.append(content)
            
            if group_content:
                group_texts.append(" ".join(group_content))
            else:
                group_texts.append("")
        
        # Векторизуем тексты групп
        try:
            group_vectors = self.vectorizer.fit_transform(group_texts)
            
            # Распределяем каждый пост без simhash
            for post in posts_without_simhash:
                content = self.normalize_text((post.content or "") + " " + (post.title or ""))
                if not content:
                    # Если контент пустой, добавляем к первой группе
                    groups[0].append(post)
                    continue
                
                # Векторизуем текст поста
                post_vector = self.vectorizer.transform([content])
                
                # Вычисляем сходство с каждой группой
                similarities = cosine_similarity(post_vector, group_vectors).flatten()
                
                # Находим группу с наибольшим сходством
                best_group_idx = np.argmax(similarities)
                groups[best_group_idx].append(post)
        except Exception as e:
            logger.error(f"Ошибка при распределении постов без simhash: {e}")
            # В случае ошибки, равномерно распределяем посты
            for i, post in enumerate(posts_without_simhash):
                group_idx = i % len(groups)
                groups[group_idx].append(post)
    
    def deduplicate_batch(self, posts, keep_min_one=True):
        """
        Удаляет дубликаты из батча, используя TF-IDF и косинусное сходство
        
        Args:
            posts: список объектов Post для дедупликации
            keep_min_one: гарантирует, что в результате будет хотя бы один пост
            
        Returns:
            List: список уникальных постов
        """
        if not posts:
            return []
            
        if len(posts) == 1:
            return posts
            
        # Инициализируем векторизатор, если еще не сделано
        if self.vectorizer is None:
            self.vectorizer = TfidfVectorizer(
                analyzer='word',
                lowercase=True,
                stop_words=self.stopwords,
                token_pattern=r'\b[а-яА-Яa-zA-Z0-9]{2,}\b',
                max_features=5000,
                ngram_range=(1, 2)
            )
        
        # Подготовка нормализованных текстов
        normalized_contents = []
        for post in posts:
            content = self.normalize_text((post.content or "") + " " + (post.title or ""))
            normalized_contents.append(content)
        
        # Создаем TF-IDF матрицу
        try:
            tfidf_matrix = self.vectorizer.fit_transform(normalized_contents)
            
            # Вычисляем матрицу попарных сходств
            pairwise_similarity = cosine_similarity(tfidf_matrix)
            
            # Выбираем уникальные посты
            # 1. Начинаем с поста с наибольшим количеством уникальных терминов
            # 2. Исключаем похожие посты
            
            # Подсчитываем количество уникальных терминов в каждом посте
            term_counts = np.asarray(tfidf_matrix.sum(axis=1)).flatten()
            
            # Индексы всех постов
            all_indices = list(range(len(posts)))
            # Индексы выбранных постов
            selected_indices = []
            # Индексы исключенных постов
            excluded_indices = set()
            
            # Выбираем посты, исключая похожие
            while all_indices and (not keep_min_one or len(selected_indices) == 0):
                # Находим доступные индексы
                available_indices = [i for i in all_indices if i not in excluded_indices and i not in selected_indices]
                
                if not available_indices:
                    break
                    
                # Выбираем пост с наибольшим количеством уникальных терминов
                best_idx = max(available_indices, key=lambda i: term_counts[i])
                
                # Добавляем индекс в выбранные
                selected_indices.append(best_idx)
                
                # Исключаем похожие посты
                for idx in available_indices:
                    if idx != best_idx and pairwise_similarity[best_idx, idx] > self.similarity_threshold:
                        excluded_indices.add(idx)
            
            # Формируем результат
            unique_posts = [posts[idx] for idx in selected_indices]
            
            logger.info(f"Дедупликация: из {len(posts)} постов оставлено {len(unique_posts)} уникальных")
            return unique_posts
            
        except Exception as e:
            logger.error(f"Ошибка при дедупликации: {e}")
            
            # В случае ошибки возвращаем первый пост, если keep_min_one=True
            if keep_min_one:
                logger.warning("Возвращаем первый пост из-за ошибки дедупликации")
                return [posts[0]]
            else:
                return []
    
    def process_batches(self, posts, max_tokens_per_batch=30000):
        """
        Обрабатывает посты - группирует, дедуплицирует и объединяет
        
        Args:
            posts: список объектов Post
            max_tokens_per_batch: максимальное количество токенов в батче
            
        Returns:
            List: список уникальных постов для анализа
        """
        if not posts:
            return []
        
        # Шаг 1: Группируем посты по simhash
        batch_groups = self.group_posts_by_simhash(posts)
        
        if not batch_groups:
            logger.warning("Не удалось разделить посты на группы")
            return []
        
        # Шаг 2: Дедупликация в каждом батче
        deduplicated_batches = []
        for i, batch in enumerate(batch_groups):
            logger.info(f"Дедупликация батча {i+1} из {len(batch_groups)}, {len(batch)} постов")
            unique_posts = self.deduplicate_batch(batch, keep_min_one=True)
            deduplicated_batches.append(unique_posts)
        
        # Шаг 3: Объединяем все уникальные посты
        combined_posts = []
        for batch in deduplicated_batches:
            combined_posts.extend(batch)
        
        # Шаг 4: Второй этап дедупликации на объединенном наборе
        logger.info(f"Вторичная дедупликация на объединенном наборе из {len(combined_posts)} постов")
        final_unique_posts = self.deduplicate_batch(combined_posts, keep_min_one=True)
        
        # Проверка, остался ли хотя бы один пост
        if not final_unique_posts and combined_posts:
            logger.warning("После дедупликации не осталось постов, возвращаем первый пост из объединенного набора")
            final_unique_posts = [combined_posts[0]]
        
        logger.info(f"Финальный набор: {len(final_unique_posts)} уникальных постов")
        return final_unique_posts