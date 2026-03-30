from pathlib import Path


class InputResolver:
    SUPPORTED_FILES = {".zip", ".csv", ".xlsx", ".xls", ".json", ".parquet"}

    def discover(
        self,
        root_dir: str,
        ignore_dirs: list[str] | None = None,
        prefer_zip: bool = True,
        allowed_suffixes: set[str] | None = None
    ) -> Path:
        root = Path(root_dir)

        if not root.exists():
            raise FileNotFoundError(f"Pasta de entrada não encontrada: {root}")

        ignored = {Path(path).resolve() for path in (ignore_dirs or [])}
        valid_suffixes = {suffix.lower() for suffix in (allowed_suffixes or self.SUPPORTED_FILES)}
        candidates = []

        for path in root.rglob("*"):
            if not path.is_file():
                continue

            if path.suffix.lower() not in valid_suffixes:
                continue

            if any(ignored_dir in path.resolve().parents for ignored_dir in ignored):
                continue

            candidates.append(path)

        if not candidates:
            raise FileNotFoundError(f"Nenhum arquivo suportado foi encontrado em: {root}")

        if prefer_zip:
            zip_files = [file for file in candidates if file.suffix.lower() == ".zip"]
            if zip_files:
                return max(zip_files, key=lambda file: file.stat().st_mtime)

        non_zip_files = [file for file in candidates if file.suffix.lower() != ".zip"]
        if non_zip_files:
            return max(non_zip_files, key=lambda file: file.stat().st_mtime)

        return max(candidates, key=lambda file: file.stat().st_mtime)