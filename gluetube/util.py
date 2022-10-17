# Craig Tomkow
# 2022-10-17

def append_name_to_dir_list(name: str, conf_dir: list) -> list:

    return [s + name for s in conf_dir]


# all the possible directories for the cfg files,
#   depending on how things are packaged and deployed
#   starts locally, then branches out eventually system-wide
def conf_dir() -> list:

    return [
        './',
        'cfg/',
        '~/.gluetube/cfg/',
        '/usr/local/etc/gluetube/',
        '/etc/opt/gluetube/',
        '/etc/gluetube/'
    ]
