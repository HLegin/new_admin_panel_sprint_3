from dataclasses import dataclass, field
from datetime import datetime
from uuid import UUID


@dataclass(frozen=True)
class film_work:
    id: UUID
    title: str
    type: str
    certificate: str = field(default=None)
    file_path: str = field(default=None)
    description: str = field(default=None)
    creation_date: datetime = field(default=None)
    rating: float = field(default=None)
    created_at: datetime = field(default=None)
    updated_at: datetime = field(default=None)


@dataclass(frozen=True)
class person:
    id: UUID
    full_name: str
    gender: str = field(default=None)
    created_at: datetime = field(default=None)
    updated_at: datetime = field(default=None)


@dataclass(frozen=True)
class person_film_work:
    id: UUID
    film_work_id: UUID
    person_id: UUID
    role: str
    created_at: datetime = field(default=None)


@dataclass(frozen=True)
class genre:
    id: UUID
    name: str
    description: str = field(default=None)
    created_at: datetime = field(default=None)
    updated_at: datetime = field(default=None)


@dataclass(frozen=True)
class genre_film_work:
    id: UUID
    film_work_id: UUID
    genre_id: UUID
    created_at: datetime = field(default=None)


TABLE_DATACLASSES = {
    film_work.__name__: film_work,
    person.__name__: person,
    person_film_work.__name__: person_film_work,
    genre.__name__: genre,
    genre_film_work.__name__: genre_film_work,
}
