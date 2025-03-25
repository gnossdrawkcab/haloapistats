import json
import csv
import asyncio
from datetime import datetime
from aiohttp import ClientSession
from spnkr.client import HaloInfiniteClient

# Define the players to track
PLAYERS = [
    {"gamertag": "l 0cty l", "xuid": "2533274818160056"},
    {"gamertag": "Zaidster7", "xuid": "2533274965035069"},
    {"gamertag": "l P1N1 l", "xuid": "2533274804338345"},
    {"gamertag": "l Viper18 l", "xuid": "2535430400255009"},
    {"gamertag": "l Jordo l", "xuid": "2533274797008163"}
]

# Caches for metadata
medal_cache = {}
map_name_cache = {}
playlist_name_cache = {}
game_type_cache = {}

def clean_xuid(xuid):
    if isinstance(xuid, str) and "xuid(" in xuid:
        return xuid.replace("xuid(", "").replace(")", "")
    return str(xuid)

def outcome_to_string(outcome_value):
    outcomes = {0: "Left", 1: "Loss", 2: "Win", 3: "Tie"}
    if isinstance(outcome_value, (int, str)) and str(outcome_value).isdigit():
        return outcomes.get(int(outcome_value), f"Unknown ({outcome_value})")
    return str(outcome_value)

def load_tokens():
    with open('tokens.json', 'r') as f:
        return json.load(f)

def safe_get(obj, *attrs, default=None):
    for attr in attrs:
        if not hasattr(obj, attr):
            return default
        obj = getattr(obj, attr)
        if obj is None:
            return default
    return obj

def process_csr_data(playlist_csr_value, match_row):
    if not playlist_csr_value:
        return
    try:
        playlist_csr_result = playlist_csr_value.result if hasattr(playlist_csr_value, 'result') else playlist_csr_value
        if hasattr(playlist_csr_result, 'current'):
            current_csr = playlist_csr_result.current
            if hasattr(current_csr, 'value'):
                match_row['current_csr_value'] = current_csr.value
            if hasattr(current_csr, 'measurement_matches_remaining'):
                match_row['current_csr_measurement_matches_remaining'] = current_csr.measurement_matches_remaining
            if hasattr(current_csr, 'initial_measurement_matches'):
                match_row['current_csr_initial_measurement_matches'] = current_csr.initial_measurement_matches
            if hasattr(current_csr, 'tier'):
                match_row['current_csr_tier_name'] = str(current_csr.tier)
            if hasattr(current_csr, 'tier_start'):
                match_row['current_csr_tier_start'] = current_csr.tier_start
            if hasattr(current_csr, 'sub_tier'):
                match_row['current_csr_sub_tier_name'] = str(current_csr.sub_tier)
        if hasattr(playlist_csr_result, 'season_max'):
            season_max_csr = playlist_csr_result.season_max
            if hasattr(season_max_csr, 'value'):
                match_row['season_max_csr_value'] = season_max_csr.value
            if hasattr(season_max_csr, 'tier'):
                match_row['season_max_csr_tier_name'] = str(season_max_csr.tier)
            if hasattr(season_max_csr, 'sub_tier'):
                match_row['season_max_csr_sub_tier_name'] = str(season_max_csr.sub_tier)
        if hasattr(playlist_csr_result, 'all_time_max'):
            all_time_max_csr = playlist_csr_result.all_time_max
            if hasattr(all_time_max_csr, 'value'):
                match_row['all_time_max_csr_value'] = all_time_max_csr.value
            if hasattr(all_time_max_csr, 'tier'):
                match_row['all_time_max_csr_tier_name'] = str(all_time_max_csr.tier)
            if hasattr(all_time_max_csr, 'sub_tier'):
                match_row['all_time_max_csr_sub_tier_name'] = str(all_time_max_csr.sub_tier)
    except Exception:
        pass

def process_medals(medals_data, match_row, medal_names=None):
    if not medals_data:
        return
    try:
        for medal in medals_data:
            if hasattr(medal, 'name_id') and hasattr(medal, 'count'):
                medal_id = medal.name_id
                medal_count = medal.count
                if medal_names and medal_id in medal_names:
                    medal_name = medal_names[medal_id]
                    clean_name = ''.join(c if c.isalnum() else '_' for c in medal_name)
                    column_name = f"medal_{clean_name}"
                else:
                    column_name = f"medal_id_{medal_id}"
                match_row[column_name] = medal_count
    except Exception:
        pass

async def get_medal_metadata(client):
    global medal_cache
    if medal_cache:
        return medal_cache
    try:
        metadata_response = await client.gamecms_hacs.get_medal_metadata()
        metadata = await metadata_response.parse()
        if hasattr(metadata, 'medals'):
            for medal in metadata.medals:
                if hasattr(medal, 'name_id') and hasattr(medal, 'name'):
                    medal_id = medal.name_id
                    medal_name = medal.name.value if hasattr(medal.name, 'value') else str(medal.name)
                    medal_cache[medal_id] = medal_name
                    medal_cache[str(medal_id)] = medal_name
    except Exception:
        pass
    return medal_cache

async def get_map_name(client, asset_id, version_id=None):
    key = f"{asset_id}:{version_id}"
    if key in map_name_cache:
        return map_name_cache[key]
    if version_id is None:
        return f"Map ID: {asset_id}"
    try:
        map_response = await client.discovery_ugc.get_map(asset_id, version_id)
        map_data = await map_response.parse()
        for attr in ['name', 'asset_name', 'internal_name', 'display_name', 'public_name', 'title']:
            if hasattr(map_data, attr):
                name = getattr(map_data, attr)
                map_name_cache[key] = name
                return name
        try:
            pair_response = await client.discovery_ugc.get_map_mode_pair(asset_id, version_id)
            pair_data = await pair_response.parse()
            for attr in ['name', 'asset_name', 'internal_name', 'display_name', 'public_name', 'title']:
                if hasattr(pair_data, attr):
                    name = getattr(pair_data, attr)
                    map_name_cache[key] = name
                    return name
        except Exception:
            pass
        return f"Map ID: {asset_id}"
    except Exception:
        return f"Map ID: {asset_id}"

async def get_playlist_name(client, asset_id, version_id=None):
    key = f"{asset_id}:{version_id}"
    if key in playlist_name_cache:
        return playlist_name_cache[key]
    if version_id is None:
        return f"Playlist ID: {asset_id}"
    try:
        playlist_response = await client.discovery_ugc.get_playlist(asset_id, version_id)
        playlist_data = await playlist_response.parse()
        for attr in ['name', 'asset_name', 'internal_name', 'display_name', 'public_name', 'title']:
            if hasattr(playlist_data, attr):
                name = getattr(playlist_data, attr)
                playlist_name_cache[key] = name
                return name
        return f"Playlist ID: {asset_id}"
    except Exception:
        return f"Playlist ID: {asset_id}"

async def get_game_variant_name(client, asset_id, version_id=None):
    key = f"{asset_id}:{version_id}"
    if key in game_type_cache:
        return game_type_cache[key]
    if version_id is None:
        return f"Game Type ID: {asset_id}"
    try:
        variant_response = await client.discovery_ugc.get_ugc_game_variant(asset_id, version_id)
        variant_data = await variant_response.parse()
        for attr in ['name', 'asset_name', 'internal_name', 'display_name', 'public_name', 'title', 'game_mode']:
            if hasattr(variant_data, attr):
                name = getattr(variant_data, attr)
                game_type_cache[key] = name
                return name
        properties = safe_get(variant_data, 'properties')
        if properties:
            for attr in ['name', 'display_name', 'game_mode', 'variant_name']:
                if hasattr(properties, attr):
                    name = getattr(properties, attr)
                    game_type_cache[key] = name
                    return name
        return f"Game Type ID: {asset_id}"
    except Exception:
        return f"Game Type ID: {asset_id}"

async def process_match(client, player_info, match_id, match_number, csv_data, csv_headers, medal_names):
    player_gamertag = player_info["gamertag"]
    player_xuid = clean_xuid(player_info["xuid"])
    # Add a print statement to show each match ID as it's being processed
    print(f"Processing match ID: {match_id} for player {player_gamertag}")
    
    try:
        match_stats_response = await client.stats.get_match_stats(match_id)
        match_stats = await match_stats_response.parse()
    except Exception:
        return
    match_date = match_stats.match_info.start_time
    match_duration = match_stats.match_info.duration
    game_type = "Unknown"
    if hasattr(match_stats.match_info, 'game_variant_category'):
        raw_game_type = match_stats.match_info.game_variant_category
        game_type = raw_game_type if not isinstance(raw_game_type, int) and not (isinstance(raw_game_type, str) and raw_game_type.isdigit()) else f"Game Type: {raw_game_type}"
    game_variant = safe_get(match_stats.match_info, 'ugc_game_variant')
    if game_variant:
        for name_attr in ['name', 'asset_name', 'internal_name', 'display_name', 'public_name', 'title']:
            if hasattr(game_variant, name_attr):
                game_type = getattr(game_variant, name_attr)
                break
        if game_type.startswith("Game Type:") or game_type == "Unknown":
            asset_id = safe_get(game_variant, 'asset_id')
            if asset_id:
                version_id = None
                for version_attr in ['version_id', 'version', 'asset_version_id']:
                    version_id = safe_get(game_variant, version_attr)
                    if version_id:
                        break
                if version_id:
                    game_type = await get_game_variant_name(client, asset_id, version_id)
                else:
                    game_type = f"Game Type ID: {asset_id}"
    map_name = "Unknown"
    map_variant = safe_get(match_stats.match_info, 'map_variant')
    if map_variant:
        for name_attr in ['name', 'asset_name', 'internal_name', 'display_name', 'public_name', 'title']:
            if hasattr(map_variant, name_attr):
                map_name = getattr(map_variant, name_attr)
                break
        if map_name == "Unknown":
            asset_id = safe_get(map_variant, 'asset_id')
            if asset_id:
                version_id = None
                for version_attr in ['version_id', 'version', 'asset_version_id']:
                    version_id = safe_get(map_variant, version_attr)
                    if version_id:
                        break
                if version_id:
                    map_name = await get_map_name(client, asset_id, version_id)
                else:
                    map_name = f"Map ID: {asset_id}"
    playlist = "Unknown"
    playlist_id = None
    playlist_obj = safe_get(match_stats.match_info, 'playlist')
    if playlist_obj:
        for name_attr in ['name', 'asset_name', 'internal_name', 'display_name', 'public_name', 'title']:
            if hasattr(playlist_obj, name_attr):
                playlist = getattr(playlist_obj, name_attr)
                break
        playlist_id = safe_get(playlist_obj, 'asset_id')
        if playlist == "Unknown" and playlist_id:
            version_id = None
            for version_attr in ['version_id', 'version', 'asset_version_id']:
                version_id = safe_get(playlist_obj, version_attr)
                if version_id:
                    break
            if version_id:
                playlist = await get_playlist_name(client, playlist_id, version_id)
            else:
                playlist = f"Playlist ID: {playlist_id}"
    for player in match_stats.players:
        player_id = safe_get(player, 'player_id')
        current_xuid = clean_xuid(player_id)
        if current_xuid != player_xuid:
            continue
        player_team_id = safe_get(player, 'last_team_id', default=0)
        readable_outcome = outcome_to_string(safe_get(player, 'outcome', default="Unknown"))
        team_rank = 0
        if hasattr(match_stats, 'teams'):
            for team in match_stats.teams:
                if safe_get(team, 'team_id') == player_team_id:
                    team_rank = safe_get(team, 'rank', default=0)
                    break
        match_row = {
            'player_gamertag': player_gamertag,
            'player_xuid': player_xuid,
            'match_number': match_number,
            'match_id': match_id,
            'date': match_date.strftime('%Y-%m-%d %H:%M:%S') if isinstance(match_date, datetime) else str(match_date),
            'duration': str(match_duration),
            'game_type': game_type,
            'map': map_name,
            'playlist': playlist,
            'playlist_id': playlist_id if playlist_id else 'Unknown',
            'outcome': readable_outcome,
            'team_id': player_team_id,
            'team_rank': team_rank,
            'kills': 0,
            'deaths': 0,
            'assists': 0,
            'kd': 0,
            'kda': 0,
            'accuracy': 0,
            'score': 0,
            'medal_count': 0,
            'current_csr_value': 0,
            'current_csr_tier_name': '',
            'current_csr_sub_tier_name': '',
            'current_csr_measurement_matches_remaining': 0,
            'current_csr_initial_measurement_matches': 0,
            'current_csr_tier_start': 0,
            'season_max_csr_value': 0,
            'season_max_csr_tier_name': '',
            'season_max_csr_sub_tier_name': '',
            'all_time_max_csr_value': 0,
            'all_time_max_csr_tier_name': '',
            'all_time_max_csr_sub_tier_name': '',
            'match_csr_value': 0,
            'match_csr_tier_name': '',
            'match_csr_sub_tier_name': '',
            'match_mmr_value': 0
        }
        if hasattr(player, 'csr'):
            player_csr_result = {'current': player.csr}
            process_csr_data(player_csr_result, match_row)
        try:
            match_skill_response = await client.skill.get_match_skill(
                match_id=match_id,
                xuids=[player_xuid]
            )
            if match_skill_response:
                match_skill_data = await match_skill_response.parse()
                if match_skill_data:
                    if hasattr(match_skill_data, 'players') and match_skill_data.players:
                        for player_skill in match_skill_data.players:
                            player_id = safe_get(player_skill, 'id')
                            if player_id and clean_xuid(player_id) == player_xuid:
                                if hasattr(player_skill, 'csr'):
                                    match_csr = player_skill.csr
                                    if hasattr(match_csr, 'value'):
                                        match_row['match_csr_value'] = match_csr.value
                                    if hasattr(match_csr, 'tier'):
                                        match_row['match_csr_tier_name'] = str(match_csr.tier)
                                    if hasattr(match_csr, 'sub_tier'):
                                        match_row['match_csr_sub_tier_name'] = str(match_csr.sub_tier)
                                if hasattr(player_skill, 'mmr'):
                                    match_mmr = player_skill.mmr
                                    if hasattr(match_mmr, 'value'):
                                        match_row['match_mmr_value'] = match_mmr.value
                    elif hasattr(match_skill_data, 'value'):
                        value_data = match_skill_data.value
                        if hasattr(value_data, '__iter__') and not isinstance(value_data, str):
                            for skill_value in value_data:
                                player_id = safe_get(skill_value, 'id')
                                if player_id and clean_xuid(player_id) == player_xuid:
                                    skill_result = safe_get(skill_value, 'result')
                                    if skill_result:
                                        rank_recap = safe_get(skill_result, 'rank_recap')
                                        if rank_recap:
                                            pre_match_csr = safe_get(rank_recap, 'pre_match_csr')
                                            if pre_match_csr:
                                                if hasattr(pre_match_csr, 'value'):
                                                    match_row['match_csr_value'] = pre_match_csr.value
                                                if hasattr(pre_match_csr, 'tier'):
                                                    match_row['match_csr_tier_name'] = str(pre_match_csr.tier)
                                                if hasattr(pre_match_csr, 'sub_tier'):
                                                    match_row['match_csr_sub_tier_name'] = str(pre_match_csr.sub_tier)
                                            post_match_csr = safe_get(rank_recap, 'post_match_csr')
                                            if post_match_csr:
                                                if hasattr(post_match_csr, 'value'):
                                                    match_row['post_match_csr_value'] = post_match_csr.value
                                                if hasattr(post_match_csr, 'tier'):
                                                    match_row['post_match_csr_tier_name'] = str(post_match_csr.tier)
                                                if hasattr(post_match_csr, 'sub_tier'):
                                                    match_row['post_match_csr_sub_tier_name'] = str(post_match_csr.sub_tier)
                                        if hasattr(skill_result, 'team_mmr'):
                                            match_row['match_mmr_value'] = skill_result.team_mmr
                        elif hasattr(value_data, 'id') or hasattr(value_data, 'result'):
                            player_id = safe_get(value_data, 'id')
                            if player_id and clean_xuid(player_id) == player_xuid:
                                if hasattr(value_data, 'csr'):
                                    match_csr = value_data.csr
                                    if hasattr(match_csr, 'value'):
                                        match_row['match_csr_value'] = match_csr.value
                                    if hasattr(match_csr, 'tier'):
                                        match_row['match_csr_tier_name'] = str(match_csr.tier)
                                    if hasattr(match_csr, 'sub_tier'):
                                        match_row['match_csr_sub_tier_name'] = str(match_csr.sub_tier)
                                if hasattr(value_data, 'mmr'):
                                    match_mmr = value_data.mmr
                                    if hasattr(match_mmr, 'value'):
                                        match_row['match_mmr_value'] = match_mmr.value
        except Exception:
            pass
        try:
            default_ranked_playlist = "edfef3ac-9cbe-4fa2-b949-8f29deafd483"
            playlist_to_check = playlist_id if playlist_id else default_ranked_playlist
            try:
                playlist_csr_response = await client.skill.get_playlist_csr(
                    playlist_id=playlist_to_check,
                    xuids=[player_xuid]
                )
                if playlist_csr_response:
                    playlist_csr_data = await playlist_csr_response.parse()
                    if playlist_csr_data:
                        if hasattr(playlist_csr_data, 'value'):
                            results = playlist_csr_data.value
                            for player_csr in results:
                                player_id = safe_get(player_csr, 'id')
                                if player_id and clean_xuid(player_id) == player_xuid:
                                    process_csr_data(player_csr, match_row)
                        else:
                            process_csr_data(playlist_csr_data, match_row)
            except Exception:
                pass
        except Exception:
            pass
        player_team_stats = safe_get(player, 'player_team_stats', default=[])
        player_team_stats = player_team_stats[0] if player_team_stats else None
        if player_team_stats:
            stats = safe_get(player_team_stats, 'stats')
            if stats:
                core = safe_get(stats, 'core_stats')
                if core:
                    for stat_name, stat_value in vars(core).items():
                        if stat_name.startswith('_'):
                            continue
                        if stat_name == 'medals':
                            match_row['medal_count'] = len(stat_value)
                            process_medals(stat_value, match_row, medal_names)
                        elif stat_name == 'personal_scores':
                            process_medals(stat_value, match_row, medal_names)
                        else:
                            if stat_name == 'accuracy' and isinstance(stat_value, float):
                                match_row['accuracy'] = stat_value * 100
                            elif stat_name not in ['medals', 'personal_scores']:
                                match_row[stat_name] = stat_value
                                if stat_name not in csv_headers:
                                    csv_headers.append(stat_name)
                    if hasattr(core, 'kd'):
                        match_row['kd'] = getattr(core, 'kd')
                    elif hasattr(core, 'kdr'):
                        match_row['kd'] = getattr(core, 'kdr')
                    elif hasattr(core, 'kills') and hasattr(core, 'deaths'):
                        kills = getattr(core, 'kills')
                        deaths = getattr(core, 'deaths')
                        match_row['kd'] = round(kills / deaths, 2) if deaths > 0 else kills
                stat_categories = [attr for attr in vars(stats) if not attr.startswith('_') and attr != 'core_stats']
                for category in stat_categories:
                    category_stats = getattr(stats, category, None)
                    if category_stats:
                        attr_dict = vars(category_stats)
                        for stat_name, stat_value in attr_dict.items():
                            if not stat_name.startswith('_'):
                                column_name = f"{category}_{stat_name}"
                                if stat_value is None:
                                    if column_name.endswith(('_count', '_kills', '_score', '_ticks', '_captures', '_defusals',
                                                               '_plants', '_returns', '_steals', '_grabs', '_secures', '_denied',
                                                               '_survived', '_remaining', '_assists', '_executions', '_pick_ups',
                                                               '_detonations')) or column_name.startswith(('time_', 'damage_')):
                                        match_row[column_name] = 0
                                    else:
                                        match_row[column_name] = ''
                                else:
                                    match_row[column_name] = stat_value
                                if column_name not in csv_headers:
                                    csv_headers.append(column_name)
                game_mode_defaults = {
                    "bomb_stats": ["bomb_carriers_killed", "bomb_defusals", "bomb_defusers_killed", 
                                  "bomb_detonations", "bomb_pick_ups", "bomb_plants", "bomb_returns", 
                                  "kills_as_bomb_carrier", "time_as_bomb_carrier"],
                    "capture_the_flag_stats": ["flag_capture_assists", "flag_captures", "flag_carriers_killed", 
                                              "flag_grabs", "flag_returners_killed", "flag_returns", 
                                              "flag_secures", "flag_steals", "kills_as_flag_carrier",
                                              "kills_as_flag_returner", "time_as_flag_carrier"],
                    "elimination_stats": ["allies_revived", "elimination_assists", "eliminations", 
                                        "enemy_revives_denied", "executions", "kills_as_last_player_standing", 
                                        "last_players_standing_killed", "rounds_survived", 
                                        "times_revived_by_ally", "lives_remaining", "elimination_order"],
                    "oddball_stats": ["kills_as_skull_carrier", "longest_time_as_skull_carrier", 
                                    "skull_carriers_killed", "skull_grabs", "time_as_skull_carrier", 
                                    "skull_scoring_ticks"],
                    "zones_stats": ["zone_captures", "zone_defensive_kills", "zone_offensive_kills", 
                                  "zone_secures", "total_zone_occupation_time", "zone_scoring_ticks",
                                  "stronghold_captures", "stronghold_defensive_kills", 
                                  "stronghold_offensive_kills", "stronghold_secures",
                                  "stronghold_occupation_time", "stronghold_scoring_ticks"]
                }
                relevant_categories = []
                game_type_lower = game_type.lower()
                if any(term in game_type_lower for term in ["ctf", "flag", "capture the flag"]):
                    relevant_categories.append("capture_the_flag_stats")
                if any(term in game_type_lower for term in ["bomb", "assault"]):
                    relevant_categories.append("bomb_stats")
                if any(term in game_type_lower for term in ["elim", "elimination", "attrition"]):
                    relevant_categories.append("elimination_stats")
                if any(term in game_type_lower for term in ["oddball", "ball"]):
                    relevant_categories.append("oddball_stats")
                if any(term in game_type_lower for term in ["zone", "stronghold", "koth", "king", "control"]):
                    relevant_categories.append("zones_stats")
                for category in stat_categories:
                    if category in game_mode_defaults and category not in relevant_categories:
                        relevant_categories.append(category)
                if not relevant_categories:
                    relevant_categories = list(game_mode_defaults.keys())
                for category in relevant_categories:
                    if category in game_mode_defaults:
                        for stat_name in game_mode_defaults[category]:
                            column_name = f"{category}_{stat_name}"
                            if column_name not in match_row:
                                match_row[column_name] = 0
                                if column_name not in csv_headers:
                                    csv_headers.append(column_name)
        csv_data.append(match_row)
        return
    csv_data.append(match_row)
    return

async def process_player_matches(client, player_info, match_count, match_type, csv_data, csv_headers, medal_names):
    player_gamertag = player_info["gamertag"]
    player_xuid = clean_xuid(player_info["xuid"])
    try:
        history_response = await client.stats.get_match_history(
            player=player_xuid, 
            start=0, 
            count=match_count,
            match_type=match_type
        )
        match_history = await history_response.parse()
        if not match_history.results:
            return
        for i, match_result in enumerate(match_history.results):
            match_id = match_result.match_id
            await process_match(client, player_info, match_id, i+1, csv_data, csv_headers, medal_names)
    except Exception:
        pass

async def run_multi_player_stats(match_count=5, match_type='all', save_to_csv=True, csv_filename='halo_multi_player_stats.csv'):
    tokens = load_tokens()
    spartan_token = tokens["spartan_token"]
    clearance_token = tokens["clearance_token"]
    csv_data = []
    csv_headers = [
        'player_gamertag', 'player_xuid',
        'match_number', 'match_id', 'date', 'duration', 
        'game_type', 'map', 'playlist', 'playlist_id', 
        'outcome', 'team_id', 'team_rank',
        'kills', 'deaths', 'assists', 'kd', 'kda', 
        'accuracy', 'score', 'medal_count',
        'match_csr_value', 'match_csr_tier_name', 'match_csr_sub_tier_name',
        'match_mmr_value',
        'current_csr_value', 'current_csr_tier_name', 'current_csr_sub_tier_name',
        'current_csr_measurement_matches_remaining', 'current_csr_initial_measurement_matches',
        'current_csr_tier_start',
        'season_max_csr_value', 'season_max_csr_tier_name', 'season_max_csr_sub_tier_name',
        'all_time_max_csr_value', 'all_time_max_csr_tier_name', 'all_time_max_csr_sub_tier_name'
    ]
    async with ClientSession() as session:
        client = HaloInfiniteClient(
            session=session,
            spartan_token=spartan_token, 
            clearance_token=clearance_token
        )
        medal_names = await get_medal_metadata(client)
        for player in PLAYERS:
            await process_player_matches(
                client, 
                player, 
                match_count, 
                match_type, 
                csv_data, 
                csv_headers, 
                medal_names
            )
        if save_to_csv and csv_data:
            try:
                additional_headers = []
                for row in csv_data:
                    for key in row.keys():
                        if key not in csv_headers and key not in additional_headers:
                            additional_headers.append(key)
                csv_headers.extend(additional_headers)
                with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=csv_headers)
                    writer.writeheader()
                    for row in csv_data:
                        for header in csv_headers:
                            if header not in row:
                                if (header.startswith(('current_csr_', 'season_max_csr_', 'all_time_max_csr_')) and 
                                    header.endswith(('_id', '_value', '_start', '_remaining', '_matches'))) or \
                                   (header in ['kills', 'deaths', 'assists', 'kd', 'kda', 'score', 'medal_count', 'accuracy']) or \
                                   header.endswith(('_count', '_kills', '_score', '_ticks', '_captures', '_defusals', '_plants', 
                                                  '_returns', '_steals', '_grabs', '_secures', '_denied', '_survived', '_remaining', 
                                                  '_assists', '_executions', '_pick_ups', '_detonations')) or \
                                   header.startswith(('time_', 'damage_', 'medal_')) or \
                                   'time_as_' in header:
                                    row[header] = 0
                                elif header.endswith('_name') and header.startswith(('current_csr_', 'season_max_csr_', 'all_time_max_csr_')):
                                    row[header] = ''
                                else:
                                    row[header] = ''
                        writer.writerow(row)
            except Exception:
                pass

if __name__ == "__main__":
    asyncio.run(run_multi_player_stats(
        match_count=5,
        match_type='all',
        save_to_csv=True,
        csv_filename='halo_multi_player_stats.csv'
    ))
