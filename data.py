#!/usr/bin/python

from settings import LOGGING
import requests, requests_cache, json, logging, logging.config, string, pprint, re, datetime, pymongo, arff
from bs4 import BeautifulSoup
from pymongo import MongoClient

client = MongoClient()

db = client.hof_database
collection = db.players_collection

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
    
    # Get honours and awards information
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
            
    profile['stats'] = stats
    profile['honors'] = other_stats
    
    return profile
    
def feet_to_cm(feet_inches_str):
    feet, inches = [int(s) for s in feet_inches_str.split('-')]
    inches += 12 * feet
    return int(inches * 2.54 + 0.5)

db_players = db.players

# players = get_players()
# full_players_info = {}
# for p in players:
#     player = players[p]
#     full_players_info[p] = get_player_profile(p)
#     # List of positions in case the player plays multiple
#     full_players_info[p]['pos'] = player.get('pos', []).split('-')
#     try:
#         full_players_info[p]['wt'] = int(player.get('wt'))
#         full_players_info[p]['ht'] = feet_to_cm(player.get('ht'))
#     except ValueError:
#         logger.exception('Could not parse player {p}\'s weight'.format(p=p))
#         
#     full_players_info[p]['from'] = datetime.datetime.strptime(player.get('from'), '%Y')
#     full_players_info[p]['to'] = datetime.datetime.strptime(player.get('to'), '%Y')
# 
#     try:
#         full_players_info[p]['dob'] = datetime.datetime.strptime(player.get('birth_date'), '%B %d, %Y')
#     except TypeError:
#         logger.exception('Player {p} does not have date of birth listed'.format(p=p))
#     except ValueError:
#         logger.exception('Could not parse player {p}\'s date of birth'.format(p=p))
# 
#     full_players_info[p]['_id'] = p
# 
#     db_players.save(full_players_info[p])

query = {
    #'name': 'Michael Jordan',
    'stats.totals.g.value': {'$gte': 100},
    'active': False,
    'from': {'$gte': datetime.datetime(1951, 1, 1)},
    'to': {'$lte': datetime.datetime.now() - datetime.timedelta(days=5*365)},
    'stats.advanced.per': {'$exists': True},
    'stats.advanced.per.complete': True,
    'stats.advanced.ts_pct': {'$exists': True},
    'stats.advanced.ts_pct.complete': True,
    'stats.advanced.ws': {'$exists': True},
    'stats.advanced.ws.complete': True,
    'stats.advanced.dws': {'$exists': True},
    'stats.advanced.dws.complete': True,
    'stats.advanced.ows': {'$exists': True},
    'stats.advanced.ows.complete': True,
    'stats.advanced.efg_pct': {'$exists': True},
    'stats.advanced.efg_pct.complete': True,
    'stats.totals.pts': {'$exists': True},
    'stats.totals.pts.complete': True,
    'stats.totals.ast': {'$exists': True},
    'stats.totals.ast.complete': True,
    'stats.totals.trb': {'$exists': True},
    'stats.totals.trb.complete': True,
    'stats.per_game.trb_per_g': {'$exists': True},
    'stats.per_game.trb_per_g.complete': True,
    'stats.per_game.pts_per_g': {'$exists': True},
    'stats.per_game.pts_per_g.complete': True,
    'stats.per_game.ast_per_g': {'$exists': True},
    'stats.per_game.ast_per_g.complete': True,
    #'hall_of_fame': True,
}

def get_nested(player_info, field_name, default=None):
    for n in field_name.split('.'):
        try:
            a = a.get(n, default)
        except NameError:
            a = player_info.get(n, default)
        except AttributeError:
            break
    return a

fields = [
    'stats.totals.pts.value', 
    'stats.totals.ast.value', 
    'stats.totals.trb.value',
    'stats.per_game.pts_per_g.value',
    'stats.per_game.ast_per_g.value',
    'stats.per_game.trb_per_g.value',
    'stats.advanced.per.value', 
    'stats.advanced.ts_pct.value', 
    'stats.advanced.ws.value', 
    'stats.advanced.dws.value', 
    'stats.advanced.ows.value', 
    'stats.advanced.efg_pct.value', 
    'stats.totals.g.value', 
    #'hall_of_fame'
]

weka_players = []
for p in db_players.find(query):
    player = [get_nested(p, f) for f in fields]
    player.append(len(get_nested(p, 'honors.allstar_appearances', [])))
    player.append(len(get_nested(p, 'honors.championships', [])))
    player.append(get_nested(p, 'honors.mvpshares', 0))
    player.append(get_nested(p, 'hall_of_fame'))
    weka_players.append(player)
    
#pprint.pprint(weka_players)
#print db_players.find(query).count()
fields.extend(['honors.allstar_appearances', 'honors.championships', 'honors.mvpshares', 'hall_of_fame'])
#fields.extend(['honors.allstar_appearances', 'honors.mvpshares', 'hall_of_fame'])
arff.dump('new_advanced_nba_stats.arff', weka_players, relation="nba", names=fields)