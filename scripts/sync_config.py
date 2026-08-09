import os
import yaml
import uuid
from datetime import datetime, timezone, timedelta
from ingestion_snowflake import get_snowflake_connection

def sync_aliases(conn, config_path):
    with open(config_path, 'r') as f:
        data = yaml.safe_load(f)
    
    cursor = conn.cursor()
    now = datetime.now(timezone.utc).isoformat()
    
    for target in data.get('targets', []):
        entity_name = target['entity_name']
        entity_type = target['entity_type']
        wikidata_id = target.get('wikidata_id')
        transfermarkt_id = target.get('transfermarkt_id')
        
        cursor.execute("SELECT target_entity_key FROM CORE.DIM_TARGET_ENTITY WHERE entity_name = %s", (entity_name,))
        row = cursor.fetchone()
        if row:
            entity_key = row[0]
            cursor.execute('''UPDATE CORE.DIM_TARGET_ENTITY SET entity_type=%s, wikidata_id=%s, transfermarkt_id=%s 
                              WHERE target_entity_key=%s''', (entity_type, wikidata_id, transfermarkt_id, entity_key))
        else:
            entity_key = str(uuid.uuid4())
            cursor.execute('''INSERT INTO CORE.DIM_TARGET_ENTITY (target_entity_key, entity_name, entity_type, wikidata_id, transfermarkt_id)
                              VALUES (%s, %s, %s, %s, %s)''', (entity_key, entity_name, entity_type, wikidata_id, transfermarkt_id))
            
        for alias in target.get('aliases', []):
            text = alias['text']
            cursor.execute("SELECT alias_key FROM CORE.DIM_TARGET_ENTITY_ALIAS WHERE target_entity_key = %s AND alias_text = %s", (entity_key, text))
            if not cursor.fetchone():
                cursor.execute('''INSERT INTO CORE.DIM_TARGET_ENTITY_ALIAS (alias_key, target_entity_key, alias_text, valid_from, is_active, source)
                                  VALUES (%s, %s, %s, %s, TRUE, 'CONFIG')''', (str(uuid.uuid4()), entity_key, text, now))

def sync_events(conn, config_path):
    with open(config_path, 'r') as f:
        data = yaml.safe_load(f)
        
    cursor = conn.cursor()
    now = datetime.now(timezone.utc).isoformat()
    
    for event in data.get('events', []):
        ext_id = event['external_event_id']
        event_type = event['event_type']
        event_name = event['event_name']
        occurred_at = event['occurred_at']
        rationale = event.get('inclusion_rationale')
        event_terms = event.get('event_terms', [])
        
        cursor.execute("SELECT event_key FROM CORE.DIM_EVENT WHERE external_event_id = %s", (ext_id,))
        row = cursor.fetchone()
        if row:
            event_key = row[0]
        else:
            event_key = str(uuid.uuid4())
            cursor.execute("INSERT INTO CORE.DIM_EVENT (event_key, external_event_id, event_type) VALUES (%s, %s, %s)",
                           (event_key, ext_id, event_type))
            
        # Get or create version
        cursor.execute("SELECT event_version_key FROM CORE.DIM_EVENT_VERSION WHERE event_key = %s AND is_current = TRUE", (event_key,))
        v_row = cursor.fetchone()
        terms_json = json.dumps(event_terms)
        if v_row:
            version_key = v_row[0]
            # Simple update for now instead of full SCD2
            cursor.execute('''UPDATE CORE.DIM_EVENT_VERSION SET event_name=%s, occurred_at=%s, inclusion_rationale=%s, event_terms=PARSE_JSON(%s)
                              WHERE event_version_key=%s''', (event_name, occurred_at, rationale, terms_json, version_key))
        else:
            version_key = str(uuid.uuid4())
            cursor.execute('''INSERT INTO CORE.DIM_EVENT_VERSION (event_version_key, event_key, event_name, occurred_at, inclusion_rationale, event_terms, valid_from, is_current)
                              VALUES (%s, %s, %s, %s, %s, PARSE_JSON(%s), %s, TRUE)''', (version_key, event_key, event_name, occurred_at, rationale, terms_json, now))
                              
        # Sync windows
        occurred_dt = datetime.fromisoformat(occurred_at.replace("Z", "+00:00"))
        
        for w in event.get('windows', []):
            w_type = w['window_type']
            start_hours = w['relative_offset_start_hours']
            end_hours = w['relative_offset_end_hours']
            
            abs_start = (occurred_dt + timedelta(hours=start_hours)).isoformat()
            abs_end = (occurred_dt + timedelta(hours=end_hours)).isoformat()
            
            cursor.execute("SELECT window_key FROM CORE.DIM_EVENT_WINDOW WHERE event_version_key = %s AND window_type = %s", (version_key, w_type))
            w_row = cursor.fetchone()
            if w_row:
                cursor.execute('''UPDATE CORE.DIM_EVENT_WINDOW SET relative_offset_start=%s, relative_offset_end=%s, absolute_start_at=%s, absolute_end_at=%s
                                  WHERE window_key=%s''', (start_hours, end_hours, abs_start, abs_end, w_row[0]))
            else:
                cursor.execute('''INSERT INTO CORE.DIM_EVENT_WINDOW (window_key, event_version_key, window_type, relative_offset_start, relative_offset_end, absolute_start_at, absolute_end_at)
                                  VALUES (%s, %s, %s, %s, %s, %s, %s)''', 
                                  (str(uuid.uuid4()), version_key, w_type, start_hours, end_hours, abs_start, abs_end))

import hashlib
import json

def sync_channels(conn, config_path):
    with open(config_path, 'r') as f:
        data = yaml.safe_load(f)
        
    cursor = conn.cursor()
    now = datetime.now(timezone.utc).isoformat()
    
    frame_name = data.get('frame_name', 'Unknown Frame')
    frame_purpose = data.get('frame_purpose', 'UNKNOWN')
    methodology_version = data.get('methodology_version', '1.2')
    
    config_hash = hashlib.sha256(json.dumps(data, sort_keys=True).encode('utf-8')).hexdigest()
    
    # Sync frame version
    cursor.execute("SELECT frame_version_key FROM CORE.DIM_CHANNEL_FRAME_VERSION WHERE configuration_hash=%s", (config_hash,))
    row = cursor.fetchone()
    if row:
        frame_version_key = row[0]
    else:
        frame_version_key = str(uuid.uuid4())
        cursor.execute('''INSERT INTO CORE.DIM_CHANNEL_FRAME_VERSION 
                          (frame_version_key, frame_name, frame_purpose, methodology_version, configuration_version, configuration_hash, git_commit_sha, constructed_at, configuration_provenance)
                          VALUES (%s, %s, %s, %s, %s, %s, %s, %s, PARSE_JSON(%s))''',
                       (frame_version_key, frame_name, frame_purpose, methodology_version, 'v1', config_hash, 'dev', now, json.dumps(data)))
    
    for ch in data.get('channels', []):
        ch_id = ch['channel_id']
        ch_name = ch['channel_name']
        disc_source = ch.get('discovery_source', 'manual_bootstrap')
        disc_version = ch.get('discovery_source_version', 'pilot_v1')
        
        # Sync Channel Identity
        cursor.execute("SELECT channel_key FROM CORE.DIM_CHANNEL WHERE source_system='YOUTUBE' AND source_id=%s", (ch_id,))
        row = cursor.fetchone()
        if row:
            channel_key = row[0]
            cursor.execute("UPDATE CORE.DIM_CHANNEL SET channel_name=%s WHERE channel_key=%s", (ch_name, channel_key))
        else:
            channel_key = str(uuid.uuid4())
            cursor.execute("INSERT INTO CORE.DIM_CHANNEL (channel_key, source_system, source_id, channel_name) VALUES (%s, 'YOUTUBE', %s, %s)",
                           (channel_key, ch_id, ch_name))
                           
        # Sync Bridge Frame Channel
        cursor.execute("SELECT frame_channel_key FROM CORE.BRIDGE_FRAME_CHANNEL WHERE frame_version_key=%s AND channel_key=%s", (frame_version_key, channel_key))
        b_row = cursor.fetchone()
        if b_row:
            frame_channel_key = b_row[0]
        else:
            frame_channel_key = str(uuid.uuid4())
            cursor.execute('''INSERT INTO CORE.BRIDGE_FRAME_CHANNEL 
                              (frame_channel_key, frame_version_key, channel_key, evaluated_at, eligibility_rule_version, frame_inclusion_status)
                              VALUES (%s, %s, %s, %s, %s, %s)''',
                           (frame_channel_key, frame_version_key, channel_key, now, methodology_version, 'ELIGIBLE'))
                           
        # Sync Source Observation
        cursor.execute('''SELECT frame_channel_source_observation_key FROM CORE.FRAME_CHANNEL_SOURCE_OBSERVATION 
                          WHERE frame_channel_key=%s AND discovery_source=%s AND discovery_source_version=%s''', 
                       (frame_channel_key, disc_source, disc_version))
        o_row = cursor.fetchone()
        if not o_row:
            cursor.execute('''INSERT INTO CORE.FRAME_CHANNEL_SOURCE_OBSERVATION 
                              (frame_channel_source_observation_key, frame_channel_key, discovery_source, discovery_source_version, source_retrieved_at)
                              VALUES (%s, %s, %s, %s, %s)''',
                           (str(uuid.uuid4()), frame_channel_key, disc_source, disc_version, now))

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    conn = get_snowflake_connection(target_db=os.getenv("TARGET_DATABASE", "FOOTBALL_NARRATIVE_DEV"))
    sync_aliases(conn, "config/player_aliases.yml")
    sync_events(conn, "config/event_windows.yml")
    sync_channels(conn, "config/tracked_channels.yml")
    conn.commit()
    print("Successfully synchronized configurations to Snowflake.")
