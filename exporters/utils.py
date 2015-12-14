import os


def remove_if_exists(file_name):
    try: os.remove(file_name)
    except: pass
