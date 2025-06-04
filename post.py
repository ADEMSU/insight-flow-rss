import re
from datetime import datetime
from enum import Enum
import traceback

from pydantic import BaseModel, Field, ValidationError, model_validator
from loguru import logger

MIN_TITLE_LEN = 10
MAX_TITLE_LEN = 100

class BlogHostType(int, Enum):
    OTHER = 0
    BLOG = 1
    MICROBLOG = 2
    SOCIAL = 3
    FORUM = 4
    MEDIA = 5
    REVIEW = 6
    MESSANGER = 7

class Post(BaseModel):
    post_id: str = Field(default_factory=lambda: '')
    object_ids: list[str] = []
    content: str | None = None
    blog_host: str | None = None
    blog_host_type: BlogHostType = BlogHostType.OTHER
    published_on: datetime | None = None
    simhash: str | None = ''
    url: str = ''
    title: str = ""
    raw: object | None = None

    @model_validator(mode='before')
    @classmethod
    def set_post_id(cls, data):
        # Если post_id не указан, пытаемся извлечь из других источников
        if not data.get('post_id') and data.get('PostId'):
            data['post_id'] = str(data['PostId'])
        return data

    def dict(self):
        """
        Создает словарь для сериализации в JSON, пропуская поля,
        которые не могут быть сериализованы
        """
        result = {
            "post_id": self.post_id,
            "object_ids": self.object_ids,
            "content": self.content,
            "blog_host": self.blog_host,
            "blog_host_type": self.blog_host_type.value if self.blog_host_type else BlogHostType.OTHER.value,
            "published_on": self.published_on,
            "simhash": self.simhash,
            "url": self.url,
            "title": self.title,
        }
        return result

    @classmethod
    def parse_posts(cls, cubus_posts):
        posts = []
        for cubus_post in cubus_posts:
            try:
                # Преобразование объекта Zeep в словарь
                if hasattr(cubus_post, '__values__'):
                    cubus_dict = dict(cubus_post.__values__)
                else:
                    cubus_dict = cubus_post

                # Обработка неполных или некорректных данных
                processed_post = cls.parse_post(cubus_dict)
                if processed_post:
                    posts.append(processed_post)
            except Exception as e:
                logger.error(f"Ошибка при парсинге поста: {e}")
                logger.error(traceback.format_exc())
        
        logger.info(f"Успешно обработано постов: {len(posts)}")
        return posts

    @classmethod
    def get_title(cls, cubus_post, content):
        cubus_title = cubus_post.get("Title", "") if isinstance(cubus_post, dict) else ""
        if cubus_title and len(re.findall(r"\w+?", cubus_title)) >= MIN_TITLE_LEN:
            return cubus_title
        elif content:
            title_array = content[:MAX_TITLE_LEN].split()
            if title_array:
                title_array.pop(-1)
                return " ".join(title_array)
        return ""

    @classmethod
    def parse_post(cls, cubus_post):
        try:
            # Если передан объект Zeep, преобразуем в словарь
            if hasattr(cubus_post, '__values__'):
                cubus_post = dict(cubus_post.__values__)

            # Безопасное извлечение данных
            content = cls.get_content(cubus_post)
            object_ids = cls.get_object_ids(cubus_post)
            title = cls.get_title(cubus_post, content)
            
            # Дополнительные способы извлечения post_id
            post_id = (
                str(cubus_post.get('PostId', '')) or 
                str(cubus_post.get('post_id', '')) or 
                str(cubus_post.get('id', ''))
            )
            
            data = {
                "post_id": post_id,
                "object_ids": object_ids,
                "content": content,
                "blog_host": cubus_post.get("BlogHost", ""),
                "blog_host_type": cubus_post.get("BlogHostType", BlogHostType.OTHER),
                "published_on": cubus_post.get("PublishDate"),
                "simhash": str(cubus_post.get("Simhash", "")),
                "url": cubus_post.get("Url", ""),
                "title": title,
                "raw": cubus_post,
            }
            
            return cls.model_validate(data)
        except ValidationError as e:
            logger.error(f"Ошибка валидации поста: {e}")
            logger.error(f"Данные поста: {cubus_post}")
            logger.error(traceback.format_exc())
            return None
        except Exception as e:
            logger.error(f"Ошибка парсинга поста: {e}")
            logger.error(f"Данные поста: {cubus_post}")
            logger.error(traceback.format_exc())
            return None

    @staticmethod
    def get_object_ids(cubus_post):
        object_ids = []
        # Обработка разных форматов входных данных
        if isinstance(cubus_post, dict):
            objects = cubus_post.get('Objects', {})
        else:
            objects = getattr(cubus_post, 'Objects', {})
        
        # Безопасное извлечение object_ids
        if hasattr(objects, 'CubusObject'):
            for obj in objects.CubusObject:
                obj_id = getattr(obj, 'ObjectId', None) if hasattr(obj, 'ObjectId') else obj.get('ObjectId')
                if obj_id and (not hasattr(obj, 'ClassId') or getattr(obj, 'ClassId', 0) == 0):
                    object_ids.append(str(obj_id))
        elif isinstance(objects, dict) and 'CubusObject' in objects:
            for obj in objects['CubusObject']:
                if obj.get('ClassId', 0) == 0:
                    object_ids.append(str(obj.get('ObjectId', '')))
        
        return object_ids

    @staticmethod
    def get_content(cubus_post):
        # Обработка разных форматов входных данных
        contents = []
        
        # Извлечение контента
        content = (cubus_post.get('Content') if isinstance(cubus_post, dict) 
                   else getattr(cubus_post, 'Content', ''))
        if content:
            contents.append(content)
        
        # Проверка текста на картинках
        images = (cubus_post.get('Images') if isinstance(cubus_post, dict) 
                  else getattr(cubus_post, 'Images', None))
        
        if images:
            # Обработка разных форматов Images
            image_list = (images.get('CubusImage') if isinstance(images, dict) 
                          else getattr(images, 'CubusImage', []))
            
            for image in image_list:
                body = (image.get('Body') if isinstance(image, dict) 
                        else getattr(image, 'Body', ''))
                if body:
                    contents.append(body)

        return "\n".join(contents) if contents else ""