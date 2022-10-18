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
    fnames, urls = zip(*details)

    return {'name': name, 'fnames': fnames, 'urls': urls, 'gs': gs, 'ext': ext}

def parse_bash(url_dict):
    """
    parse download-upload commands from list of links for city
    """
    gs_path = url_dict['gs'] + '/raw/' + url_dict['name']
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

def print_py(outputs):
    printer(f'---------type of curls {type(outputs)} of length {len(outputs)}-------')
#    printer(f'---------index is {outputs.index()}-------')
#    printer(f'---------callables is {outputs.callables()}-------')
#    printer(f'---------value is {outputs.value()}-------')
    for item in ['name', 'fnames', 'gs', 'ext', '0', '1', '2', 'urls']:
        printer(f'---------count is {outputs.value[item]}-------')
#    for k, v in outputs.items():
#        printer(f'---------item is {k},{v} of type {type(v)} and length {len(v)}-------')
    return outputs
