#!/usr/bin/python

from settings import LOGGING
import requests, requests_cache, json, logging, logging.config, string
from bs4 import BeautifulSoup

requests_cache.install_cache('data_cache')

logging.config.dictConfig(LOGGING)
logger = logging.getLogger('parse')

def memoize(filename):

    def decorator(original_func):

        def new_func():
            try:
                cache = json.load(open(filename, 'r'))
            except (IOError, ValueError):
                cache = original_func()
                json.dump(cache, open(filename, 'w'))
            return cache
            
        return new_func
        
    return decorator

@memoize('players.json')
def get_players():
    logger.info('Obtaining player list...')
    players = {}
    for letter in string.lowercase:
        logger.info('Retrieving players with last name starting with {l}'.format(l=letter))
        r = requests.get('http://www.basketball-reference.com/players/{l}'.format(l=letter))
        logger.info('Parsing raw page...')
        soup = BeautifulSoup(r.text)
        table = soup.find('table', {'id': 'players'})
        try:
            headers = [str(h.string).replace(' ', '_').lower() for h in table.thead.find_all('th')]
        except AttributeError:
            logger.exception('Failed to obtain table headers')
            continue
        for row in table.tbody.find_all('tr'):
            columns = row.find_all('td')
            pid = columns[0].a['href'].split('/')[-1].split('.')[0]
            players[pid] = dict(zip(headers[1:], [str(col.string) for col in columns[1:]]))
            players[pid]['name'] = str(columns[0].a.string)
    return players

def get_player_stats(player_id):
    logger.info('Obtaining profile of player {id}'.format(id=player_id))
    stats = {}
    r = requests.get('http://www.basketball-reference.com/players/{initial}/{id}.html'.format(initial=player_id[0], id=player_id))
    soup = BeautifulSoup(r.text)
    
    for statistic_type in ['totals', 'per_game', 'advanced']:
        stats[statistic_type] = {}
        stat_section = soup.find('div', {'id': 'all_{type}'.format(type=statistic_type), 'class': 'stw'})
        stat_table = stat_section.find('div', {'class': 'table_container', 'id': 'div_{type}'.format(type=statistic_type)})
        raw_headers = stat_table.table.thead.find_all('th')
        raw_data = stat_table.table.tfoot.tr.find_all('td')
        for col_header, col_data in zip(raw_headers, raw_data):
            stat_name = col_header['data-stat']
            stats[statistic_type][stat_name] = dict(
                value = str(col_data.string),
                complete = not 'incomplete' in col_data.get('class', []),                
            )
    return stats
#players = get_players()
#for p in players:    
jabbar = get_player_stats('abdulka01')
print json.dumps(jabbar, sort_keys=True, indent=4, separators=(',', ': '))    
