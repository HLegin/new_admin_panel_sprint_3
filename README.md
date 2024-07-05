1) Клонируем проект в какую-либо директорию.

2) Открываем терминал. 
    cd docker_compose_files

    В папке требуется создать файл data.env

    Содержимое data.env будет использоваться для сборки образов.

    ВНИМАНИЕ:

        POSTGRES_USER, DB_USER --- значение должны быть одинаковыми
        POSTGRES_PASSWORD, DB_PASSWORD --- значение должны быть одинаковыми
        POSTGRES_DB, DB_NAME --- значение должны быть одинаковыми

    data.env:
        
        POSTGRES_USER=<Имя пользователя в БД>
        POSTGRES_PASSWORD=<пароль к БД>
        POSTGRES_DB=movies_database
        POSTGRES_HOST=postgres
        POSTGRES_PORT=5432
        LVL_LOGS=DEBUG
        SCHEMA_NAME=content
        TABLE_NAMES_SQLITE_TO_POSTGRES=film_work,person,genre,person_film_work,genre_film_work

    Далее находясь по адресу в папке docker_compose_files запустить команду в терминал:
    
        docker compose up
    
    Наслаждаемся. Тесты пройдены. Персоны с жанрами отслеживаются.