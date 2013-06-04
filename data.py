#!/usr/bin/python

from settings import LOGGING
import requests, requests_cache, json, logging, logging.config, string, pprint, re
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

def get_player_profile(player_id):
    logger.info('Obtaining profile of player {id}'.format(id=player_id))
    r = requests.get('http://www.basketball-reference.com/players/{initial}/{id}.html'.format(initial=player_id[0], id=player_id))
    soup = BeautifulSoup(r.text)
    # Get basic information
    profile = {}
    basic_info_section = soup.find('div', {'id': 'info_box'})
    
    # Full name
    profile['name'] = str(basic_info_section.h1.string)
    
    # Activity
    profile['active'] = basic_info_section.find('span', {'class': 'bold_text'}, text='Experience:') is not None
    
    # Hall of Fame
    try:
        profile['hall_of_fame'] = basic_info_section.find(
            'span', {'class': 'bold_text'}, text='Hall of Fame:'
        ).find_next_sibling(
            text=re.compile('Inducted as Player')
        ) is not None
    except AttributeError:
        profile['hall_of_fame'] = False
    
    # Get player stats
    stats = {}
    for statistic_type in ('totals', 'per_game', 'advanced', 'playoffs_totals', 'playoffs_per_game', 'playoffs_advanced', 'all_star'):
        stat_section = soup.find('div', {'id': 'all_{type}'.format(type=statistic_type), 'class': 'stw'})
        if stat_section:
            stats[statistic_type] = {}
            stat_table = stat_section.find('div', {'class': 'table_container', 'id': 'div_{type}'.format(type=statistic_type)})
            raw_headers = stat_table.table.thead.find_all('th')
            raw_data = stat_table.table.tfoot.tr.find_all('td')
            for col_header, col_data in zip(raw_headers, raw_data):
                stat_name = col_header['data-stat']
                try:
                    stat_value = int(col_data.string)
                except ValueError:
                    try:
                        stat_value = float(col_data.string)
                    except ValueError:
                        continue
                except TypeError:
                    continue
                stats[statistic_type][stat_name] = dict(
                    value = stat_value,
                    complete = not 'incomplete' in col_data.get('class', []),                
                )
    
    # Get leaderboard information
    other_stats = {}
    leaderboard_section = soup.find('div', {'id': 'all_leaderboards_other', 'class': 'stw'})
    if leaderboard_section:
        # Championships
        championship_section = leaderboard_section.table.find('span', text='Championships')
        if championship_section:
            other_stats['championships'] = []
            championship_section = championship_section.parent.parent
            for br in championship_section.find_all('br'):
                second_link = br.find_previous_sibling('a')
                if second_link:
                    other_stats['championships'].append(str(second_link.find_previous_sibling('a').string))
        # All-star appearances
        allstar_section = leaderboard_section.table.find('span', text='All-Star Games')
        if allstar_section:
            other_stats['allstar_appearances'] = []
            allstar_section = allstar_section.parent.parent
            other_stats['allstar_appearances'] = [str(br.find_previous_sibling('a').string) for br in allstar_section.find_all('br')]
        # MVP Shares
        mvpshares_section = leaderboard_section.table.find('span', text='MVP Award Shares')
        if mvpshares_section:
            mvpshares_section = mvpshares_section.parent.parent
            career_mvpshare = str(mvpshares_section.find('a', text='Career').next_sibling.string)
            other_stats['mvpshares'] = float(career_mvpshare.split()[0])
            
    return profile
    
debug = 1
    
if debug:
    players = get_players()
    for p in players:    
        pprint.pprint(get_player_profile(p))
else:
    #print json.dumps(get_player_stats('abdulka01'), sort_keys=True, indent=4, separators=(',', ': '))    
    pprint.pprint(get_player_profile('kamanch01'))
    pprint.pprint(get_player_profile('abdulka01'))