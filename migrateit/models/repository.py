from migrateit.models.changelog import Changelog
from migrateit.models.config import SupportedDatabase
from migrateit.models.migration import Migration
from migrateit.reporters.logs import logger
from migrateit.reporters.output import write_line


class ChangelogRepository:
    @staticmethod
    def load() -> Changelog:
        changelog = Changelog.from_json()
        if not changelog.migrations:
            return changelog

        # Validate the migrations
        if sum(1 for m in changelog.migrations if m.initial) != 1:
            raise ValueError("Changelog must have exactly one initial migration")

        seen_names: set[str] = set()
        seen_indexes: set[str] = set()
        for m in changelog.migrations:
            seen_names.add(m.name)
            if m.initial and len(m.parents) > 0:
                raise ValueError(f"Initial migration {m.name} cannot have parents")
            if not m.initial and len(m.parents) == 0:
                raise ValueError(f"Migration {m.name} must have parents")
            Migration.assert_valid_name(m.name)

            if m.index in seen_indexes:
                raise ValueError(f"Migration name {m.name} has a duplicated index")
            seen_indexes.add(m.index)

            # NOTE: Works because migrations should be ordered topologically
            if not all(p in seen_names for p in m.parents):
                raise ValueError(f"Migration {m.name} references a non-existent parent")

        return changelog

    @staticmethod
    def save(changelog: Changelog) -> None:
        path = changelog.path.resolve()
        if not path.exists():
            raise FileNotFoundError(f"File {path.name} does not exist")
        path.write_text(changelog.to_json())
        logger.debug("Saved changelog: %s (%d migration(s))", path, len(changelog.migrations))
        write_line(f"Changelog file updated: {path}")

    @staticmethod
    def create(database: SupportedDatabase) -> Changelog:
        changelog = Changelog(version=1, database=database)
        changelog.path.parent.mkdir(parents=True, exist_ok=True)
        if changelog.path.exists():
            raise FileExistsError(f"File {changelog.path.name} already exists")
        changelog.path.write_text(changelog.to_json())
        return ChangelogRepository.load()
