from pathlib import Path


def search_for_files(start_path: Path, file_names: tuple, levels: int) -> dict:
    """
    Searches from the current path and up the path hierarchy

    :param levels: Determines how far up the hierarchy the search should go
    :param file_names: Tuple of file names to search for
    :return: dict containing found files key: file name, value: Absolute path to file
    """

    file_names = _set_file_names(file_names)
    _validate_path_levels(start_path, levels)
    _validate_search_path(start_path)

    current_path = start_path.absolute()
    return _search_paths_in_range(current_path, file_names, levels)


def search_current_path(path: Path, file_names):
    """

    :param path: path to be searched for files
    :param file_names: file names to be searched for
    :return: dict with key: filename, value: Path to file
    """
    found_files = {}
    for file in path.iterdir():
        file_str = str(file.parts[-1])
        if file_str in file_names:
            found_files[file_str] = Path(file).absolute()
    return found_files


def _search_paths_in_range(current_path: Path, file_names: set, levels: int) -> dict:
    found_files = {}
    for times in range(levels):
        # merges newly found files with already found files. Keeping the first found in case of multiple matches
        found_files = {**search_current_path(current_path, file_names), **found_files}
        if not Path(current_path.parent).exists():
            break
        current_path = current_path.parent
    return found_files


def _validate_path_levels(path: Path, levels):
    if not levels <= len(path.absolute().parts):
        raise ValueError("levels to search is higher than path depth")


def _set_file_names(files: tuple):
    for file in files:
        if not isinstance(file, str):
            raise ValueError("values in file_names must be str")
    try:
        return set(files)
    except ValueError as err:
        raise ValueError("files_to_find has to contain only unique values")


def _validate_search_path(path: Path):
    if not path.is_dir():
        raise ValueError("path is not a directory")