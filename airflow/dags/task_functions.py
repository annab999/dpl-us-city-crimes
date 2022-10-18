# functions for project_full_dag.py tasks

# test if ti needs to be arg

def printer(msg):
    import logging

    logging.info(msg)

def parse_py(name, ext, gs):
    """
    parse download links from csv file for city
    """
    import csv, os

    printer(f'---------WE ARE IN {os.getcwd()}-------')

    prefix = 'include/'
    with open(prefix + name + ext, 'r') as read_file:
        content = csv.DictReader(read_file)
        details = [ (row['dataset'].replace(' ', '_').replace(':', ''), row['download_url']) for row in content ]

    return {'name': name, 'fnames': details[:][0], 'urls': details[:][1], 'gs': gs, 'ext': ext}

def parse_bash(url_dict):
    """
    parse download-upload commands from list of links for city
    """
    gs_path = url_dict['gs'] + '/raw/' + urls_dict['name']
    up_command = f'gcloud storage cp - {gs_path}'

    delay = 10
    curls = []
    for i in range(len(url_dict['urls'])):
        curl = f"curl {url_dict['urls'][i]} | {up_command}/{url_dict['fnames'][i]}{url_dict['ext']}"
        curl += f' && sleep {delay}'
        curls.append(curl)

    return curls
