import re
from typing import List, Dict, Any, Tuple
from loguru import logger

class TokenEstimator:
    """
    Класс для оценки количества токенов в тексте и разбиения на батчи
    """
    
    def __init__(self, model_name="cl100k_base"):
        """
        Инициализирует оценщик токенов
        
        Args:
            model_name: название модели токенизации
                cl100k_base - используется для новых моделей Claude/GPT-4
                p50k_base - для GPT-3, GPT-3.5
        """
        try:
            import tiktoken
            self.tokenizer = tiktoken.get_encoding(model_name)
            self.has_tokenizer = True
            logger.info(f"Токенизатор {model_name} успешно инициализирован")
        except Exception as e:
            logger.warning(f"Не удалось инициализировать токенизатор {model_name}: {e}")
            logger.warning("Будет использована приблизительная оценка токенов")
            self.tokenizer = None
            self.has_tokenizer = False
    
    def estimate_tokens(self, text: str) -> int:
        """
        Оценивает количество токенов в тексте
        
        Args:
            text: текст для оценки
            
        Returns:
            int: количество токенов
        """
        if not text:
            return 0
        
        if self.has_tokenizer:
            # Используем токенизатор tiktoken для точной оценки
            tokens = self.tokenizer.encode(text)
            return len(tokens)
        else:
            # Используем приблизительную оценку, если токенизатор недоступен
            # Для русского текста примерное соотношение: 1 токен ~ 6 символов (консервативная оценка)
            # Для смешанного текста используем 4 символа на токен
            ru_chars = len(re.findall(r'[а-яА-ЯёЁ]', text))
            total_chars = len(text)
            
            if ru_chars / total_chars > 0.5:  # если больше половины - русский текст
                return total_chars // 6 + 1
            else:
                return total_chars // 4 + 1

    def estimate_post_tokens(self, post) -> int:
        """
        Оценивает количество токенов в посте
        
        Args:
            post: объект поста
            
        Returns:
            int: оценка количества токенов
        """
        # Подготавливаем текст поста, как он будет отправлен нейросети
        blog_host = post.blog_host or ""
        title = post.title or ""
        content = post.content or ""
        
        post_text = f"[{blog_host}] {title}\n{content}"
        return self.estimate_tokens(post_text)

    def estimate_prompt_tokens(self, prompt_template: str) -> int:
        """
        Оценивает количество токенов в шаблоне промпта
        
        Args:
            prompt_template: шаблон промпта без данных
            
        Returns:
            int: оценка количества токенов
        """
        return self.estimate_tokens(prompt_template)

    def split_texts_into_batches(self, texts, prompt_template, max_tokens=30000, 
                                 tokens_for_completion=2000, preserve_order=True):
        """
        Разбивает тексты на батчи с учетом ограничения токенов
        
        Args:
            texts: список текстов для анализа
            prompt_template: шаблон промпта, в который будут вставлены тексты
            max_tokens: максимальное количество токенов в одном запросе
            tokens_for_completion: предполагаемое количество токенов для ответа модели
            preserve_order: сохранять ли порядок текстов (если False, сортирует по длине)
            
        Returns:
            List[List]: список батчей с текстами
        """
        if not texts:
            return []
            
        # Оцениваем токены для промпта
        prompt_tokens = self.estimate_prompt_tokens(prompt_template)
        logger.info(f"Оценка токенов для шаблона промпта: {prompt_tokens}")
        
        # Рассчитываем доступное количество токенов для текстов
        available_tokens = max_tokens - prompt_tokens - tokens_for_completion
        
        logger.info(f"Доступно токенов для текстов: {available_tokens} из {max_tokens}")
        
        if available_tokens <= 0:
            logger.warning(f"Недостаточно токенов для текстов, шаблон промпта слишком большой: {prompt_tokens}")
            return []
        
        # Оценка токенов для каждого текста
        text_tokens = [(i, text, self.estimate_tokens(text)) for i, text in enumerate(texts)]
        
        logger.info(f"Оценка токенов завершена для {len(texts)} текстов")
        
        # Если не сохраняем порядок, сортируем тексты по длине (от большего к меньшему)
        if not preserve_order:
            text_tokens.sort(key=lambda x: x[2], reverse=True)
        
        # Формируем батчи с учетом максимально допустимого размера
        batches = []
        current_batch = []
        current_tokens = 0
        
        for idx, text, tokens in text_tokens:
            # Проверяем, поместится ли текст в текущий батч
            if current_tokens + tokens > available_tokens:
                # Если текущий батч не пустой, сохраняем его
                if current_batch:
                    batches.append(current_batch)
                    current_batch = []
                    current_tokens = 0
                
                # Если один текст слишком большой для батча, разделяем его
                if tokens > available_tokens:
                    logger.warning(f"Текст слишком большой ({tokens} токенов), будет усечен до {available_tokens} токенов")
                    
                    # Здесь мы можем реализовать разделение текста, но для простоты просто усекаем
                    truncated_text = text[:int(len(text) * available_tokens / tokens)]
                    current_batch.append((idx, truncated_text))
                    current_tokens = available_tokens
                    
                    # Сохраняем батч с усеченным текстом
                    batches.append(current_batch)
                    current_batch = []
                    current_tokens = 0
                    continue
            
            # Добавляем текст в текущий батч
            current_batch.append((idx, text))
            current_tokens += tokens
        
        # Добавляем последний батч, если он не пустой
        if current_batch:
            batches.append(current_batch)
        
        # Подготавливаем результат: батчи с текстами без индексов
        result_batches = []
        for batch in batches:
            result_batches.append([text for _, text in batch])
        
        logger.info(f"Тексты разделены на {len(result_batches)} батчей")
        for i, batch in enumerate(result_batches):
            batch_tokens = self.estimate_tokens("\n".join(batch))
            logger.info(f"Батч {i+1}: {len(batch)} текстов, ~{batch_tokens} токенов")
        
        return result_batches
    
    def should_split_into_batches(self, texts, prompt_template, max_tokens=30000, tokens_for_completion=2000):
        """
        Проверяет, нужно ли разделять тексты на батчи
        
        Args:
            texts: список текстов для анализа
            prompt_template: шаблон промпта, в который будут вставлены тексты
            max_tokens: максимальное количество токенов в одном запросе
            tokens_for_completion: предполагаемое количество токенов для ответа модели
            
        Returns:
            bool: True если нужно разделить на батчи, иначе False
        """
        if not texts:
            return False
            
        # Оцениваем токены для промпта и текстов
        prompt_tokens = self.estimate_prompt_tokens(prompt_template)
        texts_tokens = sum(self.estimate_tokens(text) for text in texts)
        
        total_tokens = prompt_tokens + texts_tokens + tokens_for_completion
        
        logger.info(f"Общая оценка токенов: {total_tokens} (промпт: {prompt_tokens}, тексты: {texts_tokens}, ответ: {tokens_for_completion})")
        
        return total_tokens > max_tokens