import os


def create_migrations_file(file_name: str):
    assert not os.path.exists(file_name), f"File {file_name} already exists"
    assert file_name.endswith(".json"), f"File {file_name} must be a JSON file"

    with open(file_name, "w") as f:
        f.write(
            """
            {
                "version": 1,
                "migrations": []
            }
            """
        )


def create_migrations_dir(folder_name: str):
    assert not os.path.exists(folder_name), f"Folder {folder_name} already exists"
    os.makedirs(folder_name, exist_ok=True)
