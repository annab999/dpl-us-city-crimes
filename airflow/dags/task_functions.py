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
    printer(f'---------stuff in /opt/airflow/ are: {os.listdir("/opt/airflow")}-------')
    printer(f'---------stuff in /opt/airflow/include/ are: {os.listdir("/opt/airflow/include")}-------')

    prefix = 'include/'
    with open(prefix + name + ext, 'r') as read_file:
        content = csv.DictReader(read_file)
        urls = [row['download_url'] for row in content] 

    return {'name': name, 'urls': urls, 'gs': gs}

def parse_bash(urls_dict):
    """
    parse download-upload commands from list of links for city
    """
    gs_path = urls_dict['gs'] + '/raw/' + urls_dict["name"] + '/'
    up_command = f'gcloud storage cp - {gs_path}'
    return [ f'curl {url} | {up_command} && sleep 4' for url in urls_dict['urls'] ]
