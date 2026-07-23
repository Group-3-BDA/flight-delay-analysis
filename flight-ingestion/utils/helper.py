import os
import shutil
import zipfile


def create_directory(path):
    """
    Create directory if it doesn't exist.
    """
    os.makedirs(path, exist_ok=True)


def delete_file(filepath):
    """
    Delete a file.
    """
    if os.path.exists(filepath):
        os.remove(filepath)


def delete_directory(directory):
    """
    Delete directory recursively.
    """
    if os.path.exists(directory):
        shutil.rmtree(directory)


def get_all_csv_files(root_folder):
    """
    Returns list of all CSV files recursively.
    """
    csv_files = []

    for root, dirs, files in os.walk(root_folder):

        for file in files:

            if file.lower().endswith(".csv"):
                csv_files.append(os.path.join(root, file))

    return sorted(csv_files)


def get_zip_files(folder):

    zip_files = []

    for file in os.listdir(folder):

        if file.endswith(".zip"):
            zip_files.append(os.path.join(folder, file))

    return zip_files


def verify_file_exists(path):

    return os.path.exists(path)


def verify_zip(zip_path):

    return zipfile.is_zipfile(zip_path)


def delete_empty_folders(root):

    for current, dirs, files in os.walk(root, topdown=False):

        if not dirs and not files:

            os.rmdir(current)


def get_file_size(filepath):

    return round(os.path.getsize(filepath) / (1024 * 1024), 2)


def print_banner(title):

    print("=" * 70)
    print(title)
    print("=" * 70)
