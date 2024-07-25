from dataclasses import asdict

def transform_data(data_from_postrgre: tuple, data_person_or_genre: dict) -> tuple:

    data_to_elastic = []

    if len(data_from_postrgre) != 0:

        for film in data_from_postrgre:

            film = asdict(film)

            id_str = str(film.pop("id", None))
            persons = tuple(film.pop("persons", None))
            genres_data = tuple(film.pop("genres", None))

            film.pop("created_at")
            film.pop("updated_at")
            film.pop("type")
            film.pop("certificate")
            film.pop("file_path")
            film.pop("creation_date")

            film.update({"imdb_rating": film.pop("rating")})

            directors = [{"id": str(item[2]), "name": item[0]} for item in persons if item[1] == "director"]
            directors_names = [item[0] for item in persons if item[1] == "director"]

            actors = [{"id": str(item[2]), "name": item[0]} for item in persons if item[1] == "actor"]
            actors_names = [item[0] for item in persons if item[1] == "actor"]

            writers = [{"id": str(item[2]), "name": item[0]} for item in persons if item[1] == "writer"]
            writers_name = [item[0] for item in persons if item[1] == "writer"]

            genres = [{"id": str(item[1]), "name": item[0]} for item in genres_data]
            genres_names = [item[0] for item in genres_data]

            film.update(
                {
                    "id": id_str,
                    "genres": genres,
                    "genres_names": genres_names,
                    "directors": directors,
                    "directors_names": directors_names,
                    "actors": actors,
                    "actors_names": actors_names,
                    "writers": writers,
                    "writers_names": writers_name,
                }
            )

            data_to_elastic.append(film)

    elif len(data_person_or_genre) != 0:

        for dataclass, data in data_person_or_genre.items():
            for person_or_genre in data:
                person_or_genre = asdict(person_or_genre)

                person_or_genre.pop("created_at")
                person_or_genre.pop("id")

                if {dataclass: person_or_genre} not in data_to_elastic:
                    data_to_elastic.append({dataclass: person_or_genre})
    else:
        data_to_elastic = tuple()

    return tuple(data_to_elastic)
