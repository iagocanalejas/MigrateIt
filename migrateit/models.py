import json
from dataclasses import dataclass, field


@dataclass
class MigrateItConfig:
    table_name: str
    migrations_dir: str
    migrations_file: str


@dataclass
class Migration:
    name: str

    def to_dict(self) -> dict:
        return {"name": self.name}


@dataclass
class MigrationsFile:
    version: int
    migrations: list[Migration] = field(default_factory=list)

    @staticmethod
    def from_json(json_str: str) -> "MigrationsFile":
        data = json.loads(json_str)
        try:
            migrations = [Migration(**m) for m in data.get("migrations", [])]
            return MigrationsFile(
                version=data["version"],
                migrations=migrations,
            )
        except (KeyError, TypeError, ValueError) as e:
            raise ValueError(f"Invalid JSON for MigrationsFile: {e}")

    def to_dict(self) -> dict:
        return {
            "version": self.version,
            "migrations": [migration.to_dict() for migration in self.migrations],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=4)
