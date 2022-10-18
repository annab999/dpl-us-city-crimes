# functions for project_full_dag.py tasks

# test if ti needs to be arg

def printer(msg):
    import logging

    logging.info(msg)

def parse_py(name, ext, gs, ti):
    """
    parse download links from csv file for city
    """
    import csv, os

    printer(f'---------WE ARE IN {os.getcwd()}-------')

    prefix = 'include/'
    with open(prefix + name + ext, 'r') as read_file:
        content = csv.DictReader(read_file)
        details = [ (row['dataset'].replace(' ', '_').replace(':', ''), row['download_url']) for row in content ]
    fnames, urls = zip(*details)

    ti.xcom_push(key=f'dict_{name}', value={'fnames': fnames, 'urls': urls, 'gs': gs, 'ext': ext})

def parse_bash(name, ti):
    """
    parse download-upload commands from list of links for city
    """
    url_dict = ti.xcom_pull(key=f'dict_{name}', task_ids=f'files_tg.parse_link_{name}')
    gs_path = url_dict['gs'] + '/raw/' + name
    up_command = f'gcloud storage cp - {gs_path}'

    delay = 10
    curls = []
    urls = url_dict['urls']
    fnames = url_dict['fnames']
    for i in range(len(urls)):
        curl = f"curl {urls[i]} | {up_command}/{fnames[i]}{url_dict['ext']}"
        curl += f' && sleep {delay}'
        curls.append(curl)
        printer(f'-----{curl}----------')

    return curls
