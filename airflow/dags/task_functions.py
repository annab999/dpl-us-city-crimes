# functions for project_full_dag.py tasks

# test if ti needs to be arg

def printer(msg):
    import logging

    logging.info(msg)

def parse_py(name, gs, ext):
    """
    parse download links from csv file for city
    """
    import csv

    urls = []
    prefix = 'include/'
    with open(prefix + name + ext, 'r') as read_file:
        content = csv.DictReader(read_file)
        for row in content:
            fname = row['dataset'].replace(' ', '_').replace(':', '')
            urls.append({'name': name, 'gs': gs, 'ext': ext, 'fname': fname, 'url': row['download_url']})

    return urls

def parse_bash(item):
    """
    parse download-upload commands from list of links for city
    """
    gs_path = item['gs'] + '/raw/' + item['name']
    up_command = 'gcloud storage cp - ' + gs_path
    curl = f"curl {item['url']} | {up_command}/{item['fname']}{item['ext']}"
    printer(f'-----{curl}----------')

    return curl
