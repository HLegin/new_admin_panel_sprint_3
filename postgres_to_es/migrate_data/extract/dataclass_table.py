from dataclasses import dataclass, field
from datetime import datetime
from uuid import UUID


@dataclass(frozen=True)
class FullFilm:
    id: UUID
    title: str
    type: str
    genres: tuple
    persons: tuple
    certificate: str = field(default=None)
    file_path: str = field(default=None)
    description: str = field(default=None)
    creation_date: datetime = field(default=None)
    rating: float = field(default=None)
    created_at: datetime = field(default=None)
    updated_at: datetime = field(default=None)


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


@dataclass(frozen=False)
class person:
    id: UUID
    full_name: str
    gender: str = field(default=None)
    created_at: datetime = field(default=None)
    updated_at: datetime = field(default=None)

    def __post_init__(self):
        if isinstance(self.created_at, str):
            if self.created_at.lower() == "none":
                self.created_at = None
            else:
                self.created_at = datetime.fromisoformat(self.created_at)

        if isinstance(self.updated_at, str):
            if self.updated_at.lower() == "none":
                self.updated_at = None
            else:
                self.updated_at = datetime.fromisoformat(self.updated_at)

        if isinstance(self.id, str):
            self.id = UUID(self.id)

        if isinstance(self.gender, str):
            if self.gender.lower() == "none":
                self.gender = None


@dataclass(frozen=True)
class person_film_work:
    film_work_id: UUID
    person_id: UUID
    role: str
    id: UUID = field(default=None)
    created_at: datetime = field(default=None)


@dataclass(frozen=False)
class genre:
    id: UUID
    name: str
    description: str = field(default=None)
    created_at: datetime = field(default=None)
    updated_at: datetime = field(default=None)

    def __post_init__(self):
        if isinstance(self.created_at, str):
            if self.created_at.lower() == "none":
                self.created_at = None
            else:
                self.created_at = datetime.fromisoformat(self.created_at)

        if isinstance(self.updated_at, str):
            if self.updated_at.lower() == "none":
                self.updated_at = None
            else:
                self.updated_at = datetime.fromisoformat(self.updated_at)

        if isinstance(self.id, str):
            self.id = UUID(self.id)

        if isinstance(self.description, str):
            if self.description.lower() == "none":
                self.description = None


@dataclass(frozen=True)
class genre_film_work:
    film_work_id: UUID
    genre_id: UUID
    id: UUID = field(default=None)
    created_at: datetime = field(default=None)


TABLE_DATACLASSES = {
    film_work.__name__: film_work,
    person.__name__: person,
    person_film_work.__name__: person_film_work,
    genre.__name__: genre,
    genre_film_work.__name__: genre_film_work,
}
