import sys
import subprocess
import threading
import time
import os
import json
import sqlite3
import re 
from datetime import datetime, timedelta
import urllib.parse
from pathlib import Path

# --- AUTO INSTALL REQUIRED PACKAGES ---
def install_package(package):
    try:
        __import__(package)
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])

try:
    import streamlit as st
    import psutil
    import requests
    import gdown  # <--- LIBRARY BARU KHUSUS GOOGLE DRIVE
except ImportError:
    install_package("streamlit")
    install_package("psutil")
    install_package("requests")
    install_package("gdown") # Install gdown otomatis
    import streamlit as st
    import psutil
    import requests
    import gdown

try:
    import google.auth
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build
    from google_auth_oauthlib.flow import Flow
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "google-auth", "google-auth-oauthlib", "google-api-python-client"])
    import google.auth
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build
    from google_auth_oauthlib.flow import Flow

# Predefined OAuth configuration
PREDEFINED_OAUTH_CONFIG = {
    "web": {
        "client_id": "1086578184958-hin4d45sit9ma5psovppiq543eho41sl.apps.googleusercontent.com",
        "project_id": "anjelikakozme",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_secret": "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",
        "redirect_uris": ["https://livenews1x.streamlit.app"]
    }
}

# --- DATABASE FUNCTIONS ---
def init_database():
    try:
        db_path = Path("streaming_logs.db")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS streaming_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                session_id TEXT NOT NULL,
                log_type TEXT NOT NULL,
                message TEXT NOT NULL,
                video_file TEXT,
                stream_key TEXT,
                channel_name TEXT
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS streaming_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT UNIQUE NOT NULL,
                start_time TEXT NOT NULL,
                end_time TEXT,
                video_file TEXT,
                stream_title TEXT,
                stream_description TEXT,
                tags TEXT,
                category TEXT,
                privacy_status TEXT,
                made_for_kids BOOLEAN,
                channel_name TEXT,
                status TEXT DEFAULT 'active'
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS saved_channels (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_name TEXT UNIQUE NOT NULL,
                channel_id TEXT NOT NULL,
                auth_data TEXT NOT NULL,
                created_at TEXT NOT NULL,
                last_used TEXT NOT NULL
            )
        ''')
        
        conn.commit()
        conn.close()
    except Exception as e:
        st.error(f"Database initialization error: {e}")

def save_channel_auth(channel_name, channel_id, auth_data):
    try:
        conn = sqlite3.connect("streaming_logs.db")
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO saved_channels 
            (channel_name, channel_id, auth_data, created_at, last_used)
            VALUES (?, ?, ?, ?, ?)
        ''', (channel_name, channel_id, json.dumps(auth_data), datetime.now().isoformat(), datetime.now().isoformat()))
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        st.error(f"Error saving channel auth: {e}")
        return False

def load_saved_channels():
    try:
        conn = sqlite3.connect("streaming_logs.db")
        cursor = conn.cursor()
        cursor.execute('SELECT channel_name, channel_id, auth_data, last_used FROM saved_channels ORDER BY last_used DESC')
        channels = []
        for row in cursor.fetchall():
            channels.append({'name': row[0], 'id': row[1], 'auth': json.loads(row[2]), 'last_used': row[3]})
        conn.close()
        return channels
    except Exception as e:
        st.error(f"Error loading saved channels: {e}")
        return []

def update_channel_last_used(channel_name):
    try:
        conn = sqlite3.connect("streaming_logs.db")
        cursor = conn.cursor()
        cursor.execute('UPDATE saved_channels SET last_used = ? WHERE channel_name = ?', (datetime.now().isoformat(), channel_name))
        conn.commit()
        conn.close()
    except Exception as e:
        st.error(f"Error updating channel: {e}")

def log_to_database(session_id, log_type, message, video_file=None, stream_key=None, channel_name=None):
    try:
        conn = sqlite3.connect("streaming_logs.db")
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO streaming_logs (timestamp, session_id, log_type, message, video_file, stream_key, channel_name)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (datetime.now().isoformat(), session_id, log_type, message, video_file, stream_key, channel_name))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Logging error: {e}")

def get_logs_from_database(session_id=None, limit=100):
    try:
        conn = sqlite3.connect("streaming_logs.db")
        cursor = conn.cursor()
        if session_id:
            cursor.execute('SELECT timestamp, log_type, message, video_file, channel_name FROM streaming_logs WHERE session_id = ? ORDER BY timestamp DESC LIMIT ?', (session_id, limit))
        else:
            cursor.execute('SELECT timestamp, log_type, message, video_file, channel_name FROM streaming_logs ORDER BY timestamp DESC LIMIT ?', (limit,))
        logs = cursor.fetchall()
        conn.close()
        return logs
    except Exception as e:
        return []

def save_streaming_session(session_id, video_file, stream_title, stream_description, tags, category, privacy_status, made_for_kids, channel_name):
    try:
        conn = sqlite3.connect("streaming_logs.db")
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO streaming_sessions (session_id, start_time, video_file, stream_title, stream_description, tags, category, privacy_status, made_for_kids, channel_name)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (session_id, datetime.now().isoformat(), video_file, stream_title, stream_description, tags, category, privacy_status, made_for_kids, channel_name))
        conn.commit()
        conn.close()
    except Exception as e:
        st.error(f"Error saving session: {e}")

# --- GOOGLE AUTH FUNCTIONS ---
def load_google_oauth_config(json_file):
    try:
        config = json.load(json_file)
        return config.get('web') or config.get('installed')
    except Exception as e:
        st.error(f"Error loading JSON: {e}")
        return None

def generate_auth_url(client_config):
    scopes = ['https://www.googleapis.com/auth/youtube.force-ssl']
    return (f"{client_config['auth_uri']}?client_id={client_config['client_id']}&"
            f"redirect_uri={urllib.parse.quote(client_config['redirect_uris'][0])}&"
            f"scope={urllib.parse.quote(' '.join(scopes))}&response_type=code&access_type=offline&prompt=consent")

def exchange_code_for_tokens(client_config, auth_code):
    try:
        token_data = {
            'client_id': client_config['client_id'],
            'client_secret': client_config['client_secret'],
            'code': auth_code,
            'grant_type': 'authorization_code',
            'redirect_uri': client_config['redirect_uris'][0]
        }
        response = requests.post(client_config['token_uri'], data=token_data)
        if response.status_code == 200:
            return response.json()
        st.error(f"Token exchange failed: {response.text}")
        return None
    except Exception as e:
        st.error(f"Error exchanging token: {e}")
        return None

def create_youtube_service(credentials_dict):
    try:
        if 'token' in credentials_dict:
            credentials = Credentials.from_authorized_user_info(credentials_dict)
        else:
            credentials = Credentials(
                token=credentials_dict.get('access_token'),
                refresh_token=credentials_dict.get('refresh_token'),
                token_uri=credentials_dict.get('token_uri', 'https://oauth2.googleapis.com/token'),
                client_id=credentials_dict.get('client_id'),
                client_secret=credentials_dict.get('client_secret'),
                scopes=['https://www.googleapis.com/auth/youtube.force-ssl']
            )
        return build('youtube', 'v3', credentials=credentials)
    except Exception as e:
        st.error(f"Error creating service: {e}")
        return None

def get_channel_info(service):
    try:
        request = service.channels().list(part="snippet,statistics", mine=True)
        response = request.execute()
        return response.get('items', [])
    except Exception as e:
        st.error(f"Error fetching channel: {e}")
        return []

# --- YOUTUBE LIVE FUNCTIONS ---
def get_stream_key_only(service):
    try:
        stream_request = service.liveStreams().insert(
            part="snippet,cdn",
            body={
                "snippet": {"title": f"KeyGen-{datetime.now().strftime('%H%M%S')}"},
                "cdn": {"resolution": "1080p", "frameRate": "30fps", "ingestionType": "rtmp"}
            }
        )
        resp = stream_request.execute()
        return {
            "stream_key": resp['cdn']['ingestionInfo']['streamName'],
            "stream_url": resp['cdn']['ingestionInfo']['ingestionAddress']
        }
    except Exception as e:
        st.error(f"Error getting key: {e}")
        return None

def create_live_stream(service, title, description, scheduled_time, tags, category_id, privacy, made_for_kids):
    try:
        # 1. Create Stream
        stream_body = {
            "snippet": {"title": f"{title} - Stream"},
            "cdn": {"resolution": "1080p", "frameRate": "30fps", "ingestionType": "rtmp"}
        }
        stream_resp = service.liveStreams().insert(part="snippet,cdn", body=stream_body).execute()
        
        # 2. Create Broadcast
        broadcast_body = {
            "snippet": {
                "title": title,
                "description": description,
                "scheduledStartTime": scheduled_time.isoformat(),
                "tags": tags,
                "categoryId": category_id
            },
            "status": {
                "privacyStatus": privacy,
                "selfDeclaredMadeForKids": made_for_kids
            },
            "contentDetails": {
                "enableAutoStart": True,
                "enableAutoStop": True,
                "enableDvr": True
            }
        }
        broadcast_resp = service.liveBroadcasts().insert(part="snippet,status,contentDetails", body=broadcast_body).execute()
        
        # 3. Bind
        service.liveBroadcasts().bind(
            part="id,contentDetails",
            id=broadcast_resp['id'],
            streamId=stream_resp['id']
        ).execute()
        
        return {
            "stream_key": stream_resp['cdn']['ingestionInfo']['streamName'],
            "stream_url": stream_resp['cdn']['ingestionInfo']['ingestionAddress'],
            "watch_url": f"https://www.youtube.com/watch?v={broadcast_resp['id']}",
            "studio_url": f"https://studio.youtube.com/video/{broadcast_resp['id']}/livestreaming"
        }
    except Exception as e:
        st.error(f"Create Live Error: {e}")
        return None

def get_existing_broadcasts(service):
    try:
        req = service.liveBroadcasts().list(part="snippet,status", mine=True, maxResults=5, broadcastStatus="all")
        return req.execute().get('items', [])
    except Exception as e:
        st.error(f"Error fetching broadcasts: {e}")
        return []

def get_broadcast_stream_key(service, broadcast_id):
    try:
        b_resp = service.liveBroadcasts().list(part="contentDetails", id=broadcast_id).execute()
        if not b_resp['items']: return None
        stream_id = b_resp['items'][0]['contentDetails'].get('boundStreamId')
        if not stream_id: return None
        
        s_resp = service.liveStreams().list(part="cdn", id=stream_id).execute()
        if s_resp['items']:
            info = s_resp['items'][0]['cdn']['ingestionInfo']
            return {"stream_key": info['streamName'], "stream_url": info['ingestionAddress']}
        return None
    except Exception as e:
        st.error(f"Error fetching stream key: {e}")
        return None

# --- FFMPEG STREAMING ---
def run_ffmpeg(video_path, stream_key, is_shorts, log_callback, rtmp_url=None, session_id=None):
    output_url = rtmp_url or f"rtmp://a.rtmp.youtube.com/live2/{stream_key}"
    scale = "-vf scale=720:1280" if is_shorts else ""
    cmd = [
        "ffmpeg", "-re", "-stream_loop", "-1", "-i", video_path,
        "-c:v", "libx264", "-preset", "veryfast", "-b:v", "2500k", "-maxrate", "2500k", "-bufsize", "5000k",
        "-g", "60", "-keyint_min", "60",
        "-c:a", "aac", "-b:a", "128k", "-f", "flv"
    ]
    if scale: cmd += scale.split()
    cmd.append(output_url)
    
    log_callback(f"🚀 Starting FFmpeg: {' '.join(cmd[:8])}...")
    
    try:
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        for line in process.stdout:
            log_callback(line.strip())
        process.wait()
        log_callback("✅ Streaming completed")
    except Exception as e:
        log_callback(f"❌ FFmpeg Error: {e}")
        if session_id: log_to_database(session_id, "ERROR", str(e))
    finally:
        log_callback("⏹️ Session ended")

def auto_process_auth_code():
    query_params = st.query_params
    if 'code' in query_params:
        auth_code = query_params['code']
        if 'processed_codes' not in st.session_state: st.session_state['processed_codes'] = set()
        
        if auth_code not in st.session_state['processed_codes'] and 'oauth_config' in st.session_state:
            with st.spinner("Authenticating..."):
                tokens = exchange_code_for_tokens(st.session_state['oauth_config'], auth_code)
                if tokens:
                    st.session_state['processed_codes'].add(auth_code)
                    oauth_config = st.session_state['oauth_config']
                    creds_dict = {
                        'access_token': tokens['access_token'],
                        'refresh_token': tokens.get('refresh_token'),
                        'token_uri': oauth_config['token_uri'],
                        'client_id': oauth_config['client_id'],
                        'client_secret': oauth_config['client_secret']
                    }
                    service = create_youtube_service(creds_dict)
                    if service:
                        channels = get_channel_info(service)
                        if channels:
                            channel = channels[0]
                            st.session_state['youtube_service'] = service
                            st.session_state['channel_info'] = channel
                            save_channel_auth(channel['snippet']['title'], channel['id'], creds_dict)
                            st.success(f"✅ Connected: {channel['snippet']['title']}")
                            st.query_params.clear()
                            time.sleep(1)
                            st.rerun()

def get_youtube_categories():
    return {
        "1": "Film & Animation", "2": "Autos & Vehicles", "10": "Music", "15": "Pets & Animals",
        "17": "Sports", "20": "Gaming", "22": "People & Blogs", "23": "Comedy",
        "24": "Entertainment", "25": "News & Politics", "27": "Education", "28": "Science & Tech"
    }

# --- MAIN APP ---
def main():
    st.set_page_config(page_title="Advanced YouTube Live", page_icon="📺", layout="wide")
    init_database()
    
    if 'session_id' not in st.session_state:
        st.session_state['session_id'] = f"session_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    if 'live_logs' not in st.session_state:
        st.session_state['live_logs'] = []

    st.title("🎥 Advanced YouTube Live Streaming Platform")
    auto_process_auth_code()

    # --- SIDEBAR ---
    with st.sidebar:
        st.header("📋 Configuration")
        
        # RAM MONITOR
        try:
            st.subheader("🖥️ Server Health")
            ram = psutil.virtual_memory()
            st.progress(ram.percent / 100)
            st.caption(f"RAM: {ram.percent}% ({ram.used/(1024**3):.1f} GB / {ram.total/(1024**3):.1f} GB)")
            if ram.percent > 90: st.error("⚠️ RAM CRITICAL!")
        except: pass
        
        st.info(f"🆔 {st.session_state['session_id']}")
        
        # SAVED CHANNELS
        st.subheader("💾 Saved Channels")
        saved = load_saved_channels()
        if saved:
            for ch in saved:
                if st.button(f"🔑 Use {ch['name']}", key=ch['name']):
                    service = create_youtube_service(ch['auth'])
                    if service and get_channel_info(service):
                        st.session_state['youtube_service'] = service
                        st.session_state['channel_info'] = get_channel_info(service)[0]
                        update_channel_last_used(ch['name'])
                        st.success("Loaded!")
                        st.rerun()
                    else:
                        st.error("Auth Expired")

        # AUTH SETUP
        st.subheader("🔐 Auth Setup")
        if st.button("🚀 Use Predefined Config"):
            st.session_state['oauth_config'] = PREDEFINED_OAUTH_CONFIG['web']
            st.rerun()
            
        oauth_file = st.file_uploader("Upload OAuth JSON", type=['json'])
        if oauth_file:
            st.session_state['oauth_config'] = load_google_oauth_config(oauth_file)

        if 'oauth_config' in st.session_state:
            auth_url = generate_auth_url(st.session_state['oauth_config'])
            st.markdown(f"[**👉 Click to Authorize**]({auth_url})")
            auth_code = st.text_input("Paste Auth Code Here", type="password")
            if st.button("Verify Code"):
                st.query_params["code"] = auth_code
                st.rerun()

    # --- MAIN CONTENT ---
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.header("1. Video Source")
        
        # FILE SELECTOR
        video_files = [f for f in os.listdir('.') if f.endswith(('.mp4', '.mkv', '.avi', '.mov'))]
        selected_video = st.selectbox("Select Local File", ["-- Select --"] + video_files)
        
        # SMART DOWNLOADER (GOOGLE DRIVE GDOWN SUPPORT)
        st.markdown("---")
        st.write("🔗 **Smart Downloader (Support Google Drive 1GB+):**")
        st.caption("Menggunakan library 'gdown' untuk menembus limitasi Google Drive.")
        url_input = st.text_input("Paste URL (Direct Link / GDrive)", key="dl_url")
        
        if st.button("⬇️ Download ke Server"):
            if url_input:
                try:
                    with st.spinner("⏳ Menggunakan gdown untuk download file besar..."):
                        save_path = "downloaded_video.mp4"
                        
                        # Cek Google Drive
                        gdrive_match = re.search(r'drive\.google\.com\/file\/d\/([a-zA-Z0-9_-]+)', url_input)
                        
                        if gdrive_match:
                            file_id = gdrive_match.group(1)
                            st.info(f"🔍 Google Drive ID Detected: {file_id}")
                            
                            # MENGGUNAKAN GDOWN (SOLUSI FIX)
                            # Remove file lama jika ada
                            if os.path.exists(save_path):
                                os.remove(save_path)
                                
                            url = f'https://drive.google.com/uc?id={file_id}'
                            output = save_path
                            gdown.download(url, output, quiet=False, fuzzy=True)
                            
                        else:
                            # Direct link biasa
                            import requests
                            response = requests.get(url_input, stream=True)
                            with open(save_path, 'wb') as f:
                                for chunk in response.iter_content(chunk_size=1024*1024):
                                    if chunk: f.write(chunk)
                        
                        # Validasi File
                        if os.path.exists(save_path):
                            file_size_mb = os.path.getsize(save_path) / (1024 * 1024)
                            st.success(f"✅ Download Sukses! Ukuran: {file_size_mb:.2f} MB")
                            st.rerun()
                        else:
                            st.error("❌ Gagal download, file tidak ditemukan.")
                            
                except Exception as e:
                    st.error(f"Error: {e}")

        # FILE UPLOAD (CHUNKED)
        st.markdown("---")
        uploaded_file = st.file_uploader("Upload File (Max 200MB - Manual Upload)", type=['mp4', 'mkv'])
        if uploaded_file:
            with st.spinner("Saving..."):
                with open(uploaded_file.name, "wb") as f:
                    while True:
                        chunk = uploaded_file.read(5*1024*1024)
                        if not chunk: break
                        f.write(chunk)
                st.success("Uploaded!")
                st.rerun()

        # DETERMINE ACTIVE VIDEO
        active_video = None
        if selected_video != "-- Select --": active_video = selected_video
        elif os.path.exists("downloaded_video.mp4"): active_video = "downloaded_video.mp4"
        elif uploaded_file: active_video = uploaded_file.name
        
        if active_video:
            # Cek ukuran file
            if os.path.exists(active_video):
                file_size_mb = os.path.getsize(active_video) / (1024 * 1024)
                st.success(f"🎥 Active Video: **{active_video}** (Ukuran: {file_size_mb:.2f} MB)")
                
                if file_size_mb < 1:
                    st.warning("⚠️ File terlalu kecil (<1MB). Pastikan link Google Drive diset ke 'Anyone with the link' (Publik).")
            else:
                st.success(f"🎥 Active Video: **{active_video}**")

        # --- STREAM SETTINGS ---
        st.header("2. Stream Settings")
        if 'youtube_service' in st.session_state:
            ch = st.session_state['channel_info']
            st.info(f"Connected: {ch['snippet']['title']}")
            
            s_title = st.text_input("Title", f"Live Stream {datetime.now().strftime('%H:%M')}")
            s_desc = st.text_area("Description", "Powered by Streamlit")
            col_s1, col_s2 = st.columns(2)
            with col_s1:
                s_privacy = st.selectbox("Privacy", ["public", "unlisted", "private"])
            with col_s2:
                cat_name = st.selectbox("Category", list(get_youtube_categories().values()), index=5)
            
            if st.button("🚀 Create Live Stream", type="primary"):
                if active_video:
                    cat_id = [k for k, v in get_youtube_categories().items() if v == cat_name][0]
                    live_info = create_live_stream(
                        st.session_state['youtube_service'], s_title, s_desc, 
                        datetime.now()+timedelta(seconds=30), [], cat_id, s_privacy, False
                    )
                    if live_info:
                        st.session_state['live_info'] = live_info
                        st.success("Broadcast Created!")
                        st.rerun()
                else:
                    st.error("No Video Source Selected!")
        else:
            st.warning("Please Authenticate First")
            manual_key = st.text_input("Or Manual Stream Key", type="password")
            if manual_key: st.session_state['manual_key'] = manual_key

    # --- CONTROLS ---
    with col2:
        st.header("3. Controls")
        
        stream_key = None
        rtmp_url = None
        
        if 'live_info' in st.session_state:
            info = st.session_state['live_info']
            st.success("YouTube Live Ready")
            st.write(f"[Watch Link]({info['watch_url']})")
            st.write(f"[Studio Link]({info['studio_url']})")
            stream_key = info['stream_key']
            rtmp_url = info['stream_url']
        elif 'manual_key' in st.session_state:
            stream_key = st.session_state['manual_key']
        
        is_streaming = st.session_state.get('is_streaming', False)
        
        if is_streaming:
            st.error("🔴 STREAMING ACTIVE")
            if st.button("⏹️ STOP STREAM"):
                st.session_state['is_streaming'] = False
                os.system("pkill ffmpeg")
                st.rerun()
        else:
            st.success("⚫ IDLE")
            if st.button("▶️ START STREAM"):
                if active_video and stream_key:
                    st.session_state['is_streaming'] = True
                    
                    def log_cb(msg):
                        if 'live_logs' not in st.session_state: st.session_state['live_logs'] = []
                        st.session_state['live_logs'].append(f"{datetime.now().strftime('%H:%M:%S')} {msg}")
                        if len(st.session_state['live_logs']) > 50: st.session_state['live_logs'].pop(0)

                    t = threading.Thread(target=run_ffmpeg, args=(active_video, stream_key, False, log_cb, rtmp_url, st.session_state['session_id']))
                    t.start()
                    st.rerun()
                else:
                    st.error("Check Video or Stream Key")

        # LOGS
        st.markdown("---")
        st.subheader("Logs")
        if 'live_logs' in st.session_state:
            st.text_area("FFmpeg Output", "\n".join(st.session_state['live_logs']), height=300)
            if st.checkbox("Auto-refresh Logs", value=is_streaming):
                time.sleep(2)
                st.rerun()

if __name__ == '__main__':
    main()
