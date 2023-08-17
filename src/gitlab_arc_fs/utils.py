from pathlib import Path
import io
from hashlib import sha256


def clean_file_ext(path: str):
    """
    Removes double file extensions from a given path.

    Args:
        path (str): path to remove double file extensions from

    Returns:
        path (str): path wothoud double file extension.

    Raises:
        No internal exception handling.
    """
    path = Path(path)
    suffixes = list(dict.fromkeys(path.suffixes))
    while path.suffix:
        path = path.with_suffix("")
    path = path.with_suffix("".join(suffixes))

    return str(path)


def compute_size_sha(file: io.IOBase) -> dict:
    """
    Computes the size (in bytes) as well as the shasum of a given file-like
    object.

    Args:
        file (io.IOBase): File like object.

    Returns:
        info (dict): dictionary with the keys "size" and "shasum" and
                     corresponding values.

    Raises:
        No internal exception handling.
    """
    file.seek(0)

    # compute shasum
    shasum = sha256()
    for chunk in iter(lambda: file.read(1024*1024*10), b''):
        shasum.update(chunk)

    # determine file size
    file.seek(0, 2)
    filesize = file.tell()

    info = {"size": filesize,
            "shasum": shasum}

    file.seek(0)
    return info
