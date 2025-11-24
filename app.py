import streamlit as st
import gspread
from google.oauth2.service_account import Credentials
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from collections import defaultdict
from datetime import datetime
import re
import streamlit.components.v1 as components
import threading
import time
import logging
from PIL import Image
import base64
import pandas as pd
import json

# [修正] グローバルなロックオブジェクトは st.session_state に保存して、
# st.rerun() をまたいで永続化させる
if 'save_lock' not in st.session_state:
    st.session_state.save_lock = threading.Lock()

# ==============================================================================
# 1. 初期設定とグローバル定数
# ==============================================================================

# --- Streamlit, Logging, PILの基本設定 ---
Image.MAX_IMAGE_PIXELS = None
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s',
)
logger = logging.getLogger(__name__)
st.set_page_config(layout="centered")

# --- アプリ全体に適用するカスタムCSS ---
st.markdown(
    """
    <style>
        /* Google Fonts */
        @import url('https://fonts.googleapis.com/css2?family=Noto+Sans+JP:wght@400;700&display=swap');

        html, body, [class*="css"] {
            font-family: 'Noto Sans JP', sans-serif;
        }

        /* Fade in animation */
        @keyframes fadeIn {
            from { opacity: 0; transform: translateY(10px); }
            to { opacity: 1; transform: translateY(0); }
        }
        div[data-testid="stAppViewContainer"] > .main {
            animation: fadeIn 0.4s ease-out;
        }

        /* Image Centering & Styling */
        div[data-testid="stImage"] {
            display: flex;
            justify_content: center;
            align_items: center;
            width: 100%;
            margin-bottom: 1rem;
        }
        div[data-testid="stImage"] img {
            max-width: 100%;
            height: auto;
            border-radius: 8px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            transition: transform 0.2s ease;
        }
        div[data-testid="stImage"] img:hover {
            transform: scale(1.02);
        }

        /* Card Styling for st.container(border=True) */
        div[data-testid="stVerticalBlockBorderWrapper"] {
            background-color: #262730; /* Dark theme card background */
            border: 1px solid #464b5d;
            border-radius: 12px;
            padding: 20px;
            box-shadow: 0 4px 12px rgba(0, 0, 0, 0.2);
            margin-bottom: 24px;
        }

        /* Button Styling */
        .stButton > button {
            border-radius: 8px;
            font-weight: bold;
            transition: all 0.2s;
            border: none;
        }
        .stButton > button:hover {
            transform: translateY(-2px);
            box-shadow: 0 4px 8px rgba(0,0,0,0.2);
        }
        /* Primary Button Emphasis */
        .stButton > button[kind="primary"] {
            background: linear-gradient(90deg, #ff4b4b 0%, #ff6b6b 100%);
            box-shadow: 0 4px 10px rgba(255, 75, 75, 0.3);
        }

        /* Headers */
        h1, h2, h3, h4, h5, h6 {
            font-weight: 700;
            letter-spacing: 0.05em;
        }

        /* Custom Scrollbar */
        ::-webkit-scrollbar {
            width: 8px;
            height: 8px;
        }
        ::-webkit-scrollbar-track {
            background: #0e1117;
        }
        ::-webkit-scrollbar-thumb {
            background: #555;
            border-radius: 4px;
        }
        ::-webkit-scrollbar-thumb:hover {
            background: #888;
        }
        
        /* Modal/Dialog adjustments */
        [data-testid="stDialog"] > div > div {
            width: 95vw; max-width: 95vw; height: 95vh; overflow: auto;
            border-radius: 16px;
        }
    </style>
    """,
    unsafe_allow_html=True,
)

# --- secrets.tomlから読み込む設定値 ---
TARGET_FOLDER_ID = st.secrets["target_folder_id"]
SPREADSHEET_NAME = st.secrets["spreadsheet_name"]
# [追加] 新しい UserData シートの定数を追加します。
USER_DATA_SHEET_NAME = st.secrets.get("user_data_sheet_name", "UserData")
RESULTS_SHEET_NAME = st.secrets.get("results_sheet_name", "集計結果") # 存在しない場合も考慮

# --- アプリケーション全体で利用する定数 ---
THUMBNAIL_SIZE_PX = 700
RESULT_THUMBNAIL_SIZE_PX = 1400


# ファイル名から「出品者」「タイトル」を抽出するための正規表現パターン
# アプリ起動時に一度だけコンパイルしておくことで、パフォーマンスを向上させる
FILENAME_PATTERN = re.compile(r"^(.+?)(\d{2})(.+?)\..+$")


# ==============================================================================
# 2. 認証とデータ取得 (Google API関連)
# ==============================================================================

@st.cache_resource
def authorize_services():
    """
    Streamlitの初回起動時に一度だけ実行される、Googleサービスへの認証処理。
    gspread (Sheets) と PyDrive2 (Drive) の両方のクライアントを生成し、キャッシュする。
    """
    try:
        logger.info("Googleサービスの認証を開始。")
        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

        # SCCのSecrets(辞書)から認証情報を読み込む
        creds_dict = st.secrets["gcp_service_account"] 
        
        # 1. gspread の認証 (辞書をそのまま渡す)
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        gc = gspread.authorize(creds)
        
        # 2. PyDrive2 の認証 (辞書をJSON文字列に変換して渡す)
        
        # ▼▼▼【重要】辞書(AttrDict)を標準のdictに変換し、JSON文字列(str)に変換する ▼▼▼
        creds_json_str = json.dumps(dict(creds_dict))
        # ▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲

        settings = {
            "client_config_backend": "service",
            "service_config": {
                "client_json": creds_json_str  # JSON文字列を渡す
            }
        }
        
        gauth = GoogleAuth(settings=settings)
        gauth.ServiceAuth() # ここでエラーが起きていた [cite: 1, 1435, 1437]
        drive = GoogleDrive(gauth)
        
        logger.info("Googleサービスの認証に成功。")
        return gc, drive
    except Exception:
        logger.exception("Googleサービスの認証中に致命的なエラーが発生。")
        st.error("Googleサービスへの接続に失敗しました。認証情報ファイルを確認してください。")
        st.stop()

# [確認] (app.py 140行目あたり)
def authorize_services_for_thread():
    """
    バックグラウンドスレッド (データ保存用) で使用するための、gspread認証関数。
    Streamlitのキャッシュ機能を使わない、スレッドセーフな認証を行う。
    """
    try:
        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        
        # ▼▼▼ SCCのSecrets(辞書)から直接認証情報を読み込むように変更 ▼▼▼
        creds_dict = st.secrets["gcp_service_account"]
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        return gspread.authorize(creds)
    except Exception:
        logger.exception("バックグラウンドスレッドでのGoogleサービス認証中にエラーが発生。")
        return None

@st.cache_resource
def load_photo_metadata(_drive):
    """
    Google Driveから写真のメタデータ（ID, タイトル, 出品者, サムネイルURL）を全て取得し、
    アプリで扱いやすい2つの辞書形式に整形してキャッシュする。
    """
    logger.info("写真メタデータの読み込みを開始。")
    photos_by_submitter = defaultdict(list)
    photo_id_map = {}
    
    query = f"'{TARGET_FOLDER_ID}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
    subfolders = _drive.ListFile({'q': query}).GetList()

    for folder in subfolders:
        # thumbnailLinkを含めるようにfieldsで指定し、APIレスポンスを最小限に抑える
        file_list = _drive.ListFile({'q': f"'{folder['id']}' in parents and trashed=false", 'fields': 'items(id, title, mimeType, thumbnailLink)'}).GetList()
        for file_obj in file_list:
            if 'image' in file_obj['mimeType']:
                match = FILENAME_PATTERN.match(file_obj['title'])
                if match:
                    submitter, title, photo_id = match.group(1).strip(), match.group(3).strip(), file_obj['id']
                    photo_info = {
                        'id': photo_id, 
                        'title': title, 
                        'submitter': submitter,
                        'thumbnail': file_obj.get('thumbnailLink')
                    }
                    photos_by_submitter[submitter].append(photo_info)
                    photo_id_map[photo_id] = photo_info
                    
    logger.info(f"{len(photo_id_map)}件の写真メタデータを読み込み完了。")
    return dict(photos_by_submitter), photo_id_map


@st.cache_data
def get_high_res_photo(_drive, photo_id):
    """
    指定された写真IDのオリジナル高画質画像をダウンロードし、そのバイトデータをキャッシュする。
    ファイルオブジェクトではなくバイトデータを返すことで、2回目以降の表示でも画像が空になるバグを防ぐ。
    """
    try:
        photo_file = _drive.CreateFile({'id': photo_id})
        photo_file.FetchContent()
        return photo_file.content.read()
    except Exception:
        logger.exception(f"高画質写真の読み込みに失敗。Photo ID: {photo_id}")
        return None

@st.cache_data
def get_thumbnail_photo(_drive, thumbnail_link):
    """
    指定されたサムネイルURLから画像データをダウンロードし、そのバイトデータをキャッシュする。
    """
    if not thumbnail_link: return None
    try:
        http = _drive.auth.http
        resp, content = http.request(thumbnail_link)
        return content if resp.status == 200 else None
    except Exception:
        logger.exception(f"サムネイルの読み込みに失敗。Link: {thumbnail_link}")
        return None

@st.cache_data(ttl=300) # 5分間キャッシュ
def fetch_processed_results(_gc):
    """
    管理者が作成した「集計結果」シートからデータを取得する。
    結果は5分間キャッシュされ、その間の再アクセスではAPIを叩かない。
    """
    try:
        logger.info("集計結果シートの読み込み（キャッシュ）を開始。")
        spreadsheet = _gc.open(SPREADSHEET_NAME)
        sheet_results = spreadsheet.worksheet(RESULTS_SHEET_NAME) 
        return sheet_results.get_all_records()
    except gspread.exceptions.WorksheetNotFound:
        logger.warning(f"シート '{RESULTS_SHEET_NAME}' が見つかりませんでした。")
        return None
    except Exception:
        logger.exception("集計結果シートの読み込み中にエラーが発生。")
        return None


# ==============================================================================
# 3. ヘルパー関数とUIコンポーネント
# ==============================================================================

def get_sized_thumbnail_link(original_link, size=THUMBNAIL_SIZE_PX):
    """
    Google DriveのサムネイルURLの末尾に'=sXXX'を追加し、指定したサイズのサムネイルを生成する。
    """
    if not original_link: return None
    return f"{original_link.split('=')[0]}=s{size}"

def scroll_to_top():
    """
    ページ遷移時に画面のトップまでスムーズにスクロールさせるJavaScriptを実行する。
    """
    components.html(
        """<script>window.parent.document.querySelector(".main").scrollTo({top: 0, behavior: 'smooth'});</script>""",
        height=0
    )

def render_photo_component(photo_id, context, key_prefix=""):
    """
    写真の情報を表示するための、再利用可能なUIコンポーネント。
    context引数に応じて、表示するボタンの種類（Phase1用、Phase2用、ボタンなし）を切り替える。
    """
    photo_info = st.session_state.photo_id_map.get(photo_id)
    if not photo_info: return

    # [修正] st.container(border=True) で全体を囲む
    with st.container(border=True): 
        # --- 1. 変数の準備 ---
        submitter = photo_info['submitter']
        is_rep_vote = st.session_state.voted_for.get(submitter) == photo_id
        is_free_vote = photo_id in st.session_state.free_votes
        is_favorite = photo_id in st.session_state.favorites

        # --- 2. ヘッダーとアイコン表示 ---
        icons = []
        if is_rep_vote: icons.append("✅")
        if is_free_vote: icons.append("🗳️")
        if context == 'vote' and is_favorite: icons.append("⭐")
        icon_text = " ".join(icons)
        
        # [修正] st.subheader から st.markdown(h4) に変更し、少しコンパクトに
        st.markdown(f"#### {icon_text} 【{submitter}】 {photo_info['title']}".strip())

        # --- 3. サムネイル画像表示 ---
        original_thumbnail_link = photo_info.get('thumbnail')
        sized_thumbnail_link = get_sized_thumbnail_link(original_thumbnail_link)
        thumbnail_content = get_thumbnail_photo(st.session_state.drive, sized_thumbnail_link)
        if thumbnail_content: st.image(thumbnail_content)
        else: st.error("画像読み込みエラー")

        # --- 4. ボタン表示 (contextに応じて分岐) ---
        if context == 'vote':
            col1, col2, col3 = st.columns([0.4, 0.4, 0.2])
            with col1: # 代表票
                btn_text = "この写真に投票しています" if is_rep_vote else "この作品に投票する"
                # [修正] use_container_width=True を追加してボタン幅を統一
                if st.button(btn_text, key=f"{key_prefix}vote_{photo_id}", use_container_width=True):
                    st.session_state.voted_for[submitter] = photo_id; st.session_state.dirty = True; st.rerun()
            with col2: # お気に入り
                fav_btn_text = "⭐ お気に入りから削除" if is_favorite else "⭐ お気に入りに追加"
                # [修正] use_container_width=True を追加
                if st.button(fav_btn_text, key=f"{key_prefix}fav_{photo_id}", use_container_width=True):
                    if is_favorite: st.session_state.favorites.remove(photo_id)
                    else: st.session_state.favorites.append(photo_id)
                    st.session_state.dirty = True; st.rerun()
            with col3: # フルサイズ
                # [修正] use_container_width=True を追加
                if st.button("🖼️ フル", key=f"{key_prefix}full_{photo_id}", use_container_width=True): # "フルサイズ"だと溢れる可能性があるので "フル" に
                    show_fullscreen_dialog(photo_id)

        elif context == 'free_vote':
            # --- [修正ここから] ---
            # 変数を取得
            votes_left = st.session_state.get("num_free_votes", 5) - len(st.session_state.free_votes)
            
            # カラムを3つ用意
            col1, col2, col3 = st.columns([0.4, 0.4, 0.2])

            # --- 1. col1 (自由票ボタン) ---
            with col1:
                if is_free_vote:
                    # 既に自由票を投票済みの場合
                    if st.button("🗳️ 自由票を取り消す", key=f"{key_prefix}_free_remove_{photo_id}", use_container_width=True):
                        st.session_state.free_votes.remove(photo_id)
                        st.session_state.dirty = True
                        st.rerun()
                elif votes_left > 0:
                    # まだ投票しておらず、票が残っている場合
                    if st.button(f"🗳️ 自由票を投票する (残り{votes_left})", key=f"{key_prefix}_free_add_{photo_id}", use_container_width=True):
                        st.session_state.free_votes.append(photo_id)
                        st.session_state.dirty = True
                        st.rerun()
                else:
                    # まだ投票しておらず、票が残っていない場合
                    st.button("🗳️ 自由票の枠がありません", key=f"{key_prefix}_free_disabled_{photo_id}", use_container_width=True, disabled=True)

            # --- 2. col2 (代表票ボタン) ---
            with col2:
                btn_text = "✅ 代表票" if is_rep_vote else "代表票にする"
                if st.button(btn_text, key=f"{key_prefix}_rep_vote_{photo_id}", use_container_width=True):
                    st.session_state.voted_for[submitter] = photo_id
                    st.session_state.dirty = True
                    st.rerun()

            # --- 3. col3 (フルサイズボタン) ---
            with col3: # フルサイズ
                if st.button("🖼️ フル", key=f"{key_prefix}_full_{photo_id}", use_container_width=True): # [修正] キー名も他のセクションと重複しないように変更
                    show_fullscreen_dialog(photo_id)
        
    # st.write("---") # [修正] この行を削除

@st.dialog("フルサイズ表示")
def show_fullscreen_dialog(photo_id):
    """
    フルサイズの高画質画像と情報をモーダルダイアログで表示する。
    """
    photo_info = st.session_state.photo_id_map.get(photo_id, {})
    st.subheader(f"【{photo_info.get('submitter')}】 {photo_info.get('title')}")
    placeholder = st.empty()
    with placeholder:
        st.spinner("画像を読み込んでいます...")
    
    dialog_photo_bytes = get_high_res_photo(st.session_state.drive, photo_id)
    if dialog_photo_bytes:
        b64_image = base64.b64encode(dialog_photo_bytes).decode()
        placeholder.markdown(f'<img src="data:image/jpeg;base64,{b64_image}" style="width: 100%;">', unsafe_allow_html=True)
    else:
        placeholder.error("画像の読み込みに失敗しました。")


# ==============================================================================
# 4. データ保存とページ遷移
# ==============================================================================


# [変更後] 新しいKVSモデル（UserData）に対応した保存関数
def save_all_progress(user_name, user_row_index, json_voted, json_free, json_fav, timestamp_str, lock):
    """
    [KVSモデル版] ユーザーの全投票データをスプレッドシートの特定行にピンポイントで保存する。
    シグネチャ（引数）が変更されており、st.session_stateに依存しない値を受け取る。
    """
    
    # --- 0. ロックの試行 ---
    logger.info(f"ユーザー '{user_name}': 保存スレッド開始 (対象行: {user_row_index})。ロック取得試行。")
    
    if not lock.acquire(blocking=False):
        logger.warning(f"ユーザー '{user_name}': ロック取得失敗。別スレッド実行中のため終了。")
        st.session_state.save_status = "skipped: saving in progress" 
        return

    logger.info(f"ユーザー '{user_name}': ロック取得成功。保存処理 (対象行: {user_row_index}) を開始。")
    
    try:
        # --- 1. スレッド用認証 ---
        logger.info(f"ユーザー '{user_name}': GSpread認証 (スレッド用) を開始。")
        gc_thread = authorize_services_for_thread()
        if not gc_thread: 
            logger.error(f"ユーザー '{user_name}': GSpread認証 (スレッド用) に失敗。保存中断。")
            st.session_state.save_status = "error: GSpread認証失敗"; return
        
        logger.info(f"ユーザー '{user_name}': GSpread認証成功。スプレッドシート '{SPREADSHEET_NAME}' を開きます。")
        spreadsheet = gc_thread.open(SPREADSHEET_NAME)
        
        # --- 2. UserDataシートを開き、更新範囲とペイロードを定義 ---
        sheet_userdata = spreadsheet.worksheet(USER_DATA_SHEET_NAME)
        
        # B列 (代表票_json) から E列 (最終更新日時) までを更新
        range_to_update = f'B{user_row_index}:E{user_row_index}'
        
        # gspread.update() は「リストのリスト（二次元配列）」を要求する
        values_to_write = [[json_voted, json_free, json_fav, timestamp_str]]
        
        logger.info(f"ユーザー '{user_name}': sheet.update(range='{range_to_update}') をAPIコールします。")

        # --- 3. データのピンポイント更新 (APIコール 1回) ---
        sheet_userdata.update(
            range_to_update,
            values_to_write,
            value_input_option='USER_ENTERED'
        )
        
        # --- 4. 完了処理 ---
        logger.info(f"ユーザー '{user_name}': データ保存処理 (対象行: {user_row_index}) が【正常に完了】しました。")
        st.session_state.save_status = "success"
        st.session_state.dirty = False # 正常に完了した場合のみ dirty フラグを False にする
        
    except Exception as e:
        logger.exception(f"ユーザー '{user_name}' のデータ保存中 (対象行: {user_row_index}) に【重大なエラー】が発生しました。")
        st.session_state.save_status = f"error: {e}"
        # エラー時は dirty = True のままにして、再試行の機会を残す
    
    finally:
        # --- 5. ロックの解放 ---
        lock.release() 
        logger.info(f"ユーザー '{user_name}': ロックを解放しました。保存スレッドを終了します。")



def transition_and_save_in_background(view=None, index_change=0):
    """
    [デバッグ強化版] ページ遷移やナビゲーションを行う際の共通関数。
    もしデータに変更があれば（dirty=True）、バックグラウンドスレッドで保存処理を実行する。
    """
    logger.info(f"ページ遷移/ナビゲーション発生: view={view}, index_change={index_change}")
    
    if st.session_state.dirty:
        logger.info(f"データ変更 (dirty=True) を検出。バックグラウンド保存スレッドを開始します。")
        st.toast("変更を保存しています...", icon="⏳")
        st.session_state.save_status = "pending"
        
        # [変更後] メインスレッドで全てのデータ変換を完了させる
        try:
            user_name = st.session_state.user_name
            user_row_index = st.session_state.user_row_index # Phase 4 で保存される
            json_voted = json.dumps(st.session_state.voted_for.copy(), ensure_ascii=False)
            json_free = json.dumps(st.session_state.free_votes.copy(), ensure_ascii=False)
            json_fav = json.dumps(st.session_state.favorites.copy(), ensure_ascii=False)
            timestamp_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            args = (
                user_name,
                user_row_index,
                json_voted,
                json_free,
                json_fav,
                timestamp_str,
                st.session_state.save_lock
            )
            
            logger.info(f"スレッド引数 (KVS): User='{user_name}', Row={user_row_index}")
            save_thread = threading.Thread(target=save_all_progress, args=args)
            save_thread.start()
            logger.info(f"スレッド (target=save_all_progress) を .start() しました。")

        except Exception as e:
            logger.exception(f"バックグラウンド保存スレッドの起動準備中にエラー: {e}")
            st.toast("エラー: 保存の準備に失敗しました。", icon="❌")
            # この場合、dirtyフラグはTrueのまま残り、次の遷移時に再試行される
        
    else:
        logger.info(f"データ変更 (dirty=False) はありません。保存スレッドは起動しません。")
    
    if view or index_change != 0: 
        st.session_state.needs_scroll = True
    if view: 
        st.session_state.view = view
    st.session_state.current_index += index_change

    
    #logger.info(f"st.rerun() を呼び出してUIを更新します。")
    st.rerun()


# ==============================================================================
# 5. 各ページの描画関数
# ==============================================================================

def render_login_page():
    """ログインページを描画する。"""
    st.header("ようこそ！")
    name = st.text_input("あなたの学年とクラス、名前を入力してください。例:2H森口蓮音")

    if st.button("決定"):
        if not name:
            st.warning("名前を入力してください。")
            st.stop()

        st.session_state.user_name = name
        
        with st.spinner("投票履歴を読み込んでいます..."):
            total_loaded = 0 # 読み込んだ履歴の件数をカウントする変数
            try:
                # 1. [変更後] UserDataシートを開き、A列(投票者名)を全て取得
                spreadsheet = st.session_state.gc.open(SPREADSHEET_NAME)
                sheet_userdata = spreadsheet.worksheet(USER_DATA_SHEET_NAME)
                
                logger.info(f"ユーザー '{name}': [KVS読込] UserDataシート A列(投票者名)の取得を開始。")
                all_users_list = sheet_userdata.col_values(1) # ヘッダー(A1)から全ユーザー名を取得
                logger.info(f"ユーザー '{name}': [KVS読込] A列の取得完了 (全 {len(all_users_list)} 行)。")

                user_row_index = -1
                if name in all_users_list:
                    # --- (A) 既存ユーザーの場合 ---
                    user_row_index = all_users_list.index(name) + 1 # +1 してgspreadの行番号(1-indexed)にする
                    logger.info(f"ユーザー '{name}': [KVS読込] 既存ユーザーを発見。対象行: {user_row_index}")
                    
                    # 該当行のデータ (B列〜E列) のみを取得 (APIコール 1回)
                    row_data = sheet_userdata.row_values(user_row_index)
                    
                    # row_data[0] はA列(名前)なので、B列(インデックス1)からパースする
                    voted_map = json.loads(row_data[1] or "{}")      # B列: 代表票_json
                    free_votes_list = json.loads(row_data[2] or "[]") # C列: 自由票_json
                    fav_list = json.loads(row_data[3] or "[]")        # D列: お気に入り_json

                    # 読み込んだデータがマスターに存在するかチェック (削除された写真IDを除外)
                    voted_map = {k: v for k, v in voted_map.items() if v in st.session_state.photo_id_map}
                    free_votes_list = [pid for pid in free_votes_list if pid in st.session_state.photo_id_map]
                    fav_list = [pid for pid in fav_list if pid in st.session_state.photo_id_map]
                    
                    st.session_state.user_row_index = user_row_index
                    st.session_state.voted_for = voted_map
                    st.session_state.free_votes = free_votes_list
                    st.session_state.favorites = fav_list
                    
                    logger.info(f"ユーザー '{name}': 履歴読み込み完了。代表票{len(voted_map)}, 自由票{len(free_votes_list)}, お気に入り{len(fav_list)}")
                    total_loaded = len(voted_map) + len(free_votes_list) + len(fav_list)

                else:
                    # --- (B) 新規ユーザーの場合 (競合対策v2適用) ---
                    logger.warning(f"ユーザー '{name}': [KVS読込] 新規ユーザーです。行の追加処理を開始。")
                    
                    # 1. 新しい行データを追加 (APIコール 1回)
                    timestamp_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    new_row_data = [name, "{}", "[]", "[]", timestamp_str] # A列〜E列
                    sheet_userdata.append_row(new_row_data, value_input_option='USER_ENTERED')
                    logger.info(f"ユーザー '{name}': [KVS読込] append_row() が完了。")

                    # 2.【競合対策】再度A列を全件取得し、自分が書き込んだ「最初」の行を探す
                    logger.info(f"ユーザー '{name}': [KVS読込] 競合対策のため、再度A列の全件取得を開始。")
                    latest_users_list = sheet_userdata.col_values(1)
                    logger.info(f"ユーザー '{name}': [KVS読込] 最新A列 (全 {len(latest_users_list)} 行) を取得完了。")
                    
                    # 自分の名前と一致する全てのインデックス(0-indexed)を取得
                    indices = [i for i, user in enumerate(latest_users_list) if user == name]
                    
                    # 最小のインデックス(＝最も早く書き込まれた行)を「正」とする
                    canonical_index = min(indices)
                    canonical_row_index = canonical_index + 1 # gspreadの行番号(1-indexed)に変換
                    
                    logger.info(f"ユーザー '{name}': [KVS読込] 自身の行インデックスを {indices} と認識。正準行を {canonical_row_index} に決定。")

                    # 3. セッションに空のデータを登録
                    st.session_state.user_row_index = canonical_row_index
                    st.session_state.voted_for = {}
                    st.session_state.free_votes = []
                    st.session_state.favorites = []
                    total_loaded = 0 # 新規ユーザーなので0

            except Exception as e:
                logger.exception(f"ユーザー '{name}' の履歴読み込み中にエラーが発生。")
                st.error("履歴の読み込みに失敗しました。投票はリセットされた状態で開始されます。")
                st.session_state.voted_for, st.session_state.free_votes, st.session_state.favorites = {}, [], []
                st.session_state.user_row_index = None # [追加] エラー時は行不明
                time.sleep(2.5) # エラーメッセージをユーザーが読むための時間

        # `with st.spinner` の外 (スピナーが消えた後) でメッセージを表示
        
        if total_loaded > 0:
            st.success(f"前回の投票データ ({total_loaded}件) を読み込みました。続きから開始します。")
            time.sleep(1.5) # ユーザーがメッセージを読むための時間
        else:
            # エラー時以外は、初回訪問時のメッセージを出す
            if 'save_status' not in st.session_state or 'error' not in st.session_state.save_status:
                 st.success("ようこそ！投票を開始します。")
                 time.sleep(1) 

        # 履歴読み込みが成功しても失敗しても、次のページへ遷移する
        st.session_state.view = 'instructions'
        st.rerun()

def render_instructions_page():
    """説明ページと、最初の写真のプリロードを行う。"""
    st.header("投票へようこそ！")
    st.markdown(
        """
        ### 投票の流れ
        このアプリは2つのフェーズに分かれています。
        **Phase 1：代表票**
        - 各出品者の写真の中から、最も良いと思う**1枚**を選んで投票します。
        - 全ての出品者に対して、1枚ずつ投票してください。
        **Phase 2：自由票**
        - 全員の代表票を決め終えると、**自由票**が与えられます。
        - 好きな写真に自由に追加で投票できます（代表票の変更も可能です）。
        ---
        **このアプリについて**
        - 864行の感動するほどクリーンなPythonコードと、streamlitを使って構築されています。
        - UIはちょっとゴミかもだけど、UXはめっちゃ考慮されてるので、感謝して投票してください。
        - 画面遷移時に画面がガクガクするのは仕様です。改善策を知ってるやつは俺に教えてくれマジで

        """
    )

    with st.spinner("最初の写真を準備しています..."):
        # ユーザーが説明を読んでいる間に、最初の出品者のサムネイルを先読みしてキャッシュする
        first_submitter = st.session_state.submitter_list[0]
        photos = st.session_state.photos_by_submitter.get(first_submitter, [])
        for photo in photos:
            link = get_sized_thumbnail_link(photo.get('thumbnail'))
            get_thumbnail_photo(st.session_state.drive, link)

    st.success("準備ができました！")
    if st.button("投票を開始する", type="primary", use_container_width=True):
        st.session_state.view = 'vote'
        st.session_state.needs_scroll = True
        st.rerun()

def render_vote_page():
    """Phase 1: 代表票を投票するページを描画する。"""
    if st.session_state.get('needs_scroll', False):
        scroll_to_top(); st.session_state.needs_scroll = False
    
    current_index = st.session_state.current_index
    submitter_list = st.session_state.submitter_list
    current_submitter = submitter_list[current_index]
    next_submitter = submitter_list[current_index + 1] if (current_index + 1) < len(submitter_list) else None

    # [修正] st.header と st.progress を使用
    st.header(f"「{current_submitter}」さんの作品")
    st.progress(
        (current_index + 1) / len(submitter_list), 
        text=f"進捗: ({current_index + 1}/{len(submitter_list)})"
    )
    
    if st.button(f"⭐ お気に入り一覧を見る ({len(st.session_state.favorites)}件)"):
        transition_and_save_in_background(view='favorites')

    photos = st.session_state.photos_by_submitter.get(current_submitter, [])
    for photo in photos:
        render_photo_component(photo['id'], context='vote')

    # --- ナビゲーションボタン ---
    col1, col2 = st.columns(2)
    with col1:
        if current_index > 0:
            if st.button("◀️ 前の人に戻る"): transition_and_save_in_background(index_change=-1)
    with col2:
        if next_submitter:
            if st.button(f"次の人: {next_submitter} へ ▶️"): transition_and_save_in_background(index_change=+1)
        else:
            if st.button("🎉 全員の投票が完了！自由投票に進む"): transition_and_save_in_background(view='free_vote')
                
    # --- 次の出品者の写真を先読み ---
    if next_submitter:
        photos_to_preload = st.session_state.photos_by_submitter.get(next_submitter, [])
        for photo in photos_to_preload:
            link = get_sized_thumbnail_link(photo.get('thumbnail'))
            get_thumbnail_photo(st.session_state.drive, link)

def render_favorites_page():
    """お気に入りに追加した写真の一覧ページを描画する。"""
    if st.session_state.get('needs_scroll', False):
        scroll_to_top(); st.session_state.needs_scroll = False

    st.header("⭐ お気に入り一覧")
    if st.button("◀️ 投票に戻る"): transition_and_save_in_background(view='vote')
    st.write("---")
    
    if not st.session_state.favorites:
        st.info("お気に入りに登録された写真はありません。")
    else:
        for photo_id in reversed(st.session_state.favorites):
            render_photo_component(photo_id, context='favorites', key_prefix="fav_page")

def render_free_vote_page():
    """Phase 2: 自由票を投票し、代表票も編集できるページを描画する。"""
    st.header("Phase 2: 自由投票")
    st.success("代表票の投票、お疲れ様でした！このページで代表票の変更もできます。")
    
    num_votes = st.session_state.get("num_free_votes", 5)
    votes_left = num_votes - len(st.session_state.free_votes)
    st.info(f"残り自由票: **{votes_left}** / {num_votes}")
    st.write("---")

    with st.expander("⭐ お気に入りから選ぶ", expanded=False):
        if not st.session_state.favorites:
            st.write("お気に入りに登録された写真はありません。")
        else:
            for pid in st.session_state.favorites:
                render_photo_component(pid, context='free_vote', key_prefix="fav")

    for submitter in st.session_state.submitter_list:
        with st.expander(f"「{submitter}」さんの作品一覧", expanded=False):
            photos = st.session_state.photos_by_submitter.get(submitter, [])
            for p in photos:
                render_photo_component(p['id'], context='free_vote', key_prefix="all")
    
    st.write("") 
    if not st.session_state.get('voting_complete', False):
        if st.button("全ての投票を完了する", type="primary", use_container_width=True):
            with st.spinner("最終投票を保存しています..."):
                try:
                    # [変更後] メインスレッドで全てのデータ変換を完了させる
                    user_name = st.session_state.user_name
                    user_row_index = st.session_state.user_row_index
                    json_voted = json.dumps(st.session_state.voted_for.copy(), ensure_ascii=False)
                    json_free = json.dumps(st.session_state.free_votes.copy(), ensure_ascii=False)
                    json_fav = json.dumps(st.session_state.favorites.copy(), ensure_ascii=False)
                    timestamp_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                    # [変更後] 同期呼び出し (スレッドは使わない)
                    save_all_progress(
                        user_name,
                        user_row_index,
                        json_voted,
                        json_free,
                        json_fav,
                        timestamp_str,
                        st.session_state.save_lock
                    )
                except Exception as e:
                    logger.exception("最終投票の同期保存中に予期せぬエラーが発生。")
                    st.session_state.save_status = f"error: {e}"

            # [変更後] save_status をチェックし、成功時のみBalloonsを出す
            save_status_result = st.session_state.get("save_status", "error: unknown")

            if save_status_result == "success":
                st.balloons(); st.success("投票が完了しました！")
                st.session_state.voting_complete = True
                time.sleep(1.5)
                # save_status は成功したのでクリーンアップ
                if "save_status" in st.session_state: del st.session_state["save_status"]
                st.rerun()
            
            elif save_status_result == "skipped: saving in progress":
                st.warning("現在、他の保存処理が実行中です。少し待ってからもう一度「全ての投票を完了する」ボタンを押してください。")
                # dirty = True のままにして再試行の機会を残す
            
            else:
                st.error(f"最終保存に失敗しました。お手数ですが、ページを再読み込み（リロード）して、もう一度「全ての投票を完了する」ボタンを押してください。 (詳細: {save_status_result})")
                # dirty = True のままにして再試行の機会を残す
            
            # 失敗時は save_status を残してデバッグしやすくする (rerun時にトーストで表示される)

    else:
        st.success("投票お疲れ様でした！")
        if st.button("🏆 最終結果を見る", type="primary", use_container_width=True):
            st.session_state.view = 'results'; st.session_state.needs_scroll = True; st.rerun()

def render_results_page():
    """Phase 3: 集計結果をランキング形式で表示するページ。"""
    if st.session_state.get('needs_scroll', False):
        scroll_to_top(); st.session_state.needs_scroll = False
    
    st.header("🏆 総合結果発表 🏆")
    if st.button("◀️ 投票ページに戻る"): transition_and_save_in_background(view='free_vote')

    # --- 1. データの取得と結合 ---
    scores_data = fetch_processed_results(st.session_state.gc)
    if scores_data is None:
        st.error(f"シート「{RESULTS_SHEET_NAME}」の読み込みに失敗しました。"); return
    if not st.session_state.photo_id_map:
        st.error("写真マスタが読み込まれていません。"); return

    try:
        scores_df = pd.DataFrame(scores_data)
        if not all(col in scores_df.columns for col in ['写真ID', 'スコア']):
            st.error("集計シートに必要な列（'写真ID', 'スコア'）がありません。"); return

        master_df = pd.DataFrame.from_dict(st.session_state.photo_id_map, orient='index').reset_index(names='写真ID')
        results_df = pd.merge(master_df, scores_df, on="写真ID", how="left").fillna(0)
        results_df['スコア'] = pd.to_numeric(results_df['スコア'], errors='coerce').fillna(0).astype(int)
        
        # --- 2. ランキング計算 (同率順位を考慮) ---
        results_df = results_df.sort_values('スコア', ascending=False).reset_index(drop=True)
        results_df['順位'] = results_df['スコア'].rank(method='min', ascending=False).astype(int)
        
        # --- 3. 結果の表示 ---
        st.subheader("🎉 トップ5入賞作品")
        for _, row in results_df.head(5).iterrows():
            st.markdown(f"### <span style='color: gold;'>【第 {row['順位']} 位】</span> スコア: {row['スコア']}", unsafe_allow_html=True)
            render_photo_component(row['写真ID'], context='results') # 結果表示もコンポーネント化

        with st.expander("6位以下の全ランキングを見る"):
            for _, row in results_df.iloc[5:].iterrows():
                st.markdown(f"**【第 {row['順位']} 位】 スコア: {row['スコア']}**")
                render_photo_component(row['写真ID'], context='results')

        st.subheader("マイページ：自分の作品の票数")
        my_results = results_df[results_df['submitter'] == st.session_state.user_name]
        if my_results.empty:
            st.warning(f"「{st.session_state.user_name}」さんの出品作品が見つかりませんでした。")
        else:
            for _, row in my_results.iterrows():
                st.markdown(f"**【全体 {row['順位']} 位】 スコア: {row['スコア']}**")
                render_photo_component(row['写真ID'], context='results')

    except Exception as e:
        st.error(f"結果の表示中にエラーが発生しました: {e}"); logger.exception("結果ページ描画エラー")


# ==============================================================================
# 6. メイン処理とページルーター
# ==============================================================================

def main():
    """
    アプリケーションのメインエントリーポイント。
    初回起動時にsession_stateを初期化し、その後はページルーターとして機能する。
    """
    # --- 1. 初回起動時の初期化処理 ---
    if 'view' not in st.session_state:
        st.session_state.view = 'login'
        st.session_state.user_name = ''
        st.session_state.voted_for = {}
        st.session_state.favorites = []
        st.session_state.free_votes = []
        st.session_state.current_index = 0
        st.session_state.dirty = False
        st.session_state.needs_scroll = False
        st.session_state.voting_complete = False
        
        with st.spinner("アプリを起動しています..."):
            gc, drive = authorize_services()
            st.session_state.gc, st.session_state.drive = gc, drive
            
            try:
                # スプレッドシートから設定と写真メタデータを読み込む
                sheet_settings = st.session_state.gc.open(SPREADSHEET_NAME).worksheet("Settings")
                st.session_state.num_free_votes = int(sheet_settings.acell('B1').value)
                logger.info(f"設定シートから自由票の数 ({st.session_state.num_free_votes}) を読み込み。")
                
                photos_by_submitter, photo_id_map = load_photo_metadata(st.session_state.drive)
                if not photos_by_submitter:
                    st.error("写真データを1件も見つけられませんでした。"); st.stop()
                
                st.session_state.photos_by_submitter = photos_by_submitter
                st.session_state.photo_id_map = photo_id_map
                st.session_state.submitter_list = sorted(list(photos_by_submitter.keys()))
                logger.info("アプリの起動準備が完了。")

            except Exception as e:
                logger.exception("アプリの起動中に致命的なエラーが発生。"); st.error(f"起動失敗: {e}"); st.stop()
    
    # --- 2. 保存完了時のトースト通知 ---
    if st.session_state.get("save_status") and st.session_state.save_status != "pending":
        if st.session_state.save_status == "success":
            st.toast("変更が正常に保存されました！", icon="✅")
        else:
            st.toast(f"エラー: 保存に失敗しました。", icon="❌")
            logger.error(f"保存失敗: {st.session_state.save_status}")
        del st.session_state["save_status"]

    # --- 3. ページルーター ---
    st.title("写真部 投票アプリ")
    view = st.session_state.view
    if view == 'login': render_login_page()
    elif view == 'instructions': render_instructions_page()
    elif view == 'vote': render_vote_page()
    elif view == 'favorites': render_favorites_page()
    elif view == 'free_vote': render_free_vote_page()
    elif view == 'results': render_results_page()

if __name__ == "__main__":
    main()