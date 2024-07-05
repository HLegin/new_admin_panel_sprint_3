CREATE SCHEMA IF NOT EXISTS content;

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS content.film_work (
    id UUID PRIMARY KEY,
    title CHARACTER VARYING(255) NOT NULL,
    description TEXT,
    creation_date DATE,
    rating FLOAT,
    type CHARACTER VARYING(255) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE,
    certificate CHARACTER VARYING(512),
    file_path CHARACTER VARYING(100)
);

CREATE TABLE IF NOT EXISTS content.person (
    id UUID PRIMARY KEY,
    full_name CHARACTER VARYING(255) NOT NULL,
    gender CHARACTER VARYING(255),
    created_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS content.person_film_work (
    id UUID PRIMARY KEY,
    film_work_id UUID NOT NULL,
    person_id UUID NOT NULL,
    FOREIGN KEY (film_work_id) REFERENCES content.film_work (id) DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (person_id) REFERENCES content.person (id) DEFERRABLE INITIALLY DEFERRED,
    role TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS content.genre (
    id UUID PRIMARY KEY,
    name CHARACTER VARYING(255) NOT NULL,
    description TEXT,
    created_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS content.genre_film_work (
    id UUID PRIMARY KEY,
    film_work_id UUID NOT NULL,
    genre_id UUID NOT NULL,
    FOREIGN KEY (genre_id) REFERENCES content.genre (id) DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (film_work_id) REFERENCES content.film_work (id) DEFERRABLE INITIALLY DEFERRED,
    created_at TIMESTAMP WITH TIME ZONE
);

-- CREATE INDEX

CREATE INDEX IF NOT EXISTS film_work_update_date_idx ON content.film_work(updated_at);
CREATE INDEX IF NOT EXISTS person_update_date_idx ON content.person(updated_at);
CREATE INDEX IF NOT EXISTS genre_update_date_idx ON content.genre(updated_at);

CREATE UNIQUE INDEX IF NOT EXISTS film_work_person_role_idx ON content.person_film_work (film_work_id, person_id, role);
CREATE UNIQUE INDEX IF NOT EXISTS film_work_genre_idx ON content.genre_film_work (film_work_id, genre_id);