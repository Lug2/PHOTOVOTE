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
        /* (フェードインアニメーション部分は省略) */
        @keyframes fadeIn {
          from { 
            opacity: 0; 
            /* transform: translateY(10px); */ 
          }
          to { 
            opacity: 1; 
            transform: translateY(0); 
          }
        }
        div[data-testid="stAppViewContainer"] > .main {
            animation: fadeIn 0.3s ease-in-out;
        }


        /* [修正] stImageコンテナに text-align: center を適用 */
        div[data-testid="stImage"] {
            text-align: center; /* このコンテナ内の要素(img)を中央揃えにする */
        }

        /* [削除] img へのスタイル指定は不要です */
        /* div[data-testid="stImage"] img {
             ... (前回の指定を削除) ...
        }
        */

        /* [修正] 画像(img)自体を中央寄せする */
        div[data-testid="stImage"] img {
            /* border-radius: 8px; */  /* ← [削除] 角丸の指定を削除 */
            display: block;         /* 中央寄せのためにブロック要素化 */
            margin-left: auto;      /* 左マージンを自動に */
            margin-right: auto;     /* 右マージンを自動に */
        }

        /* その他UIの微調整 */
        div[data-testid="stImage"] { text-align: center; } /* 画像を中央揃えに */
        div[data-stale="true"] { opacity: 1.0 !important; }
        div[data-stale="true"] * { opacity: 1.0 !important; }
        .stButton>button:disabled {
            opacity: 1.0 !important; color: white !important;
            background-color: #262730 !important;
            border: 1px solid rgba(250, 250, 250, 0.2) !important;
        }
        [data-testid="stDialog"] > div > div {
            width: 95vw; max-width: 95vw; height: 95vh; overflow: auto;
        }
    </style>
    """,
    unsafe_allow_html=True,
)

# --- secrets.tomlから読み込む設定値 ---
TARGET_FOLDER_ID = st.secrets["target_folder_id"]
SPREADSHEET_NAME = st.secrets["spreadsheet_name"]
VOTE_SHEET_NAME = st.secrets["vote_sheet_name"]
FAV_SHEET_NAME = st.secrets["fav_sheet_name"]
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
    [修正] setTimeout を使い、DOMの描画完了を待つ
    """
    components.html(
        """
        <script>
            setTimeout(function() {
                window.parent.document.querySelector(".main").scrollTo({top: 0, behavior: 'auto'});
            }, 50); // 50ミリ秒(0.05秒)待ってから実行
        </script>
        """,
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

def _get_row_ranges(rows):
    """
    [2, 3, 4, 8, 9, 11] のような行番号リストを、[(2, 4), (8, 9), (11, 11)] のような
    連続した範囲のタプルのリストに変換する内部ヘルパー関数。batch_update用。
    """
    if not rows: return []
    ranges, start = [], sorted(list(set(rows)))[0]
    end = start
    for row in sorted(list(set(rows)))[1:]:
        if row == end + 1: end = row
        else: ranges.append((start, end)); start = end = row
    ranges.append((start, end))
    return ranges

# app.py

# [修正] 5つ目の引数として lock を追加
def save_all_progress(user_name, voted_for_map, favorites_list, free_votes_list, lock):
    """
    [デバッグ強化版] ユーザーの全投票データをスプレッドシートに保存する。
    同時に複数の保存処理が走らないようにLockで排他制御を行う。
    """
    
    # --- 0. ロックの試行 ---
    logger.info(f"ユーザー '{user_name}': 保存スレッド開始。ロック取得を試みます。")
    
    # [修正] 'st.session_state.save_lock' ではなく、引数 'lock' を使用する
    #  -> これで引数 'lock' が正しく渡され、NameError も起きなくなる
    if not lock.acquire(blocking=False):
        logger.warning(f"ユーザー '{user_name}': ロック取得失敗。既に別の保存処理が実行中です。このスレッドは終了します。")
        # st.session_state への書き込みは（比較的）安全なため、ここは残す
        st.session_state.save_status = "skipped: saving in progress" 
        return

    logger.info(f"ユーザー '{user_name}': ロック取得成功。保存処理を開始します。")
    
    # [デバッグ] 保存対象のデータ数をログに出力
    logger.info(f"ユーザー '{user_name}': 保存対象データ: "
                f"代表票={len(voted_for_map)}, "
                f"自由票={len(free_votes_list)}, "
                f"お気に入り={len(favorites_list)}")

    try:
        # --- 1. スレッド用認証 ---
        logger.info(f"ユーザー '{user_name}': GSpread認証 (スレッド用) を開始。")
        gc_thread = authorize_services_for_thread()
        if not gc_thread: 
            logger.error(f"ユーザー '{user_name}': GSpread認証 (スレッド用) に失敗。保存を中断。")
            st.session_state.save_status = "error: GSpread認証失敗"; return
        
        logger.info(f"ユーザー '{user_name}': GSpread認証成功。スプレッドシート '{SPREADSHEET_NAME}' を開きます。")
        spreadsheet = gc_thread.open(SPREADSHEET_NAME)

        # --- 2. 既存データの削除 (投票) ---
        logger.info(f"ユーザー '{user_name}': [削除フェーズ-VOTE] '{VOTE_SHEET_NAME}' シートの全レコード取得を開始。")
        sheet_votes = spreadsheet.worksheet(VOTE_SHEET_NAME)
        all_votes_records = sheet_votes.get_all_records()
        logger.info(f"ユーザー '{user_name}': [削除フェーズ-VOTE] 全 {len(all_votes_records)} 件のレコードを取得完了。")
        
        rows_to_delete = [i + 2 for i, r in enumerate(all_votes_records) if r.get('投票者') == user_name]
        
        if rows_to_delete:
            logger.info(f"ユーザー '{user_name}': [削除フェーズ-VOTE] {len(rows_to_delete)} 行の既存データを発見。削除対象行: {rows_to_delete}")
            requests = [{"deleteDimension": {"range": {"sheetId": sheet_votes.id, "dimension": "ROWS", "startIndex": s - 1, "endIndex": e}}} for s, e in reversed(_get_row_ranges(rows_to_delete))]
            logger.info(f"ユーザー '{user_name}': [削除フェーズ-VOTE] batch_update (削除) APIを呼び出します。")
            spreadsheet.batch_update({"requests": requests})
            logger.info(f"ユーザー '{user_name}': [削除フェーズ-VOTE] batch_update (削除) が完了。")
        else:
            logger.info(f"ユーザー '{user_name}': [削除フェーズ-VOTE] 既存の投票データは見つかりませんでした。削除をスキップ。")

        # --- 3. 既存データの削除 (お気に入り) ---
        logger.info(f"ユーザー '{user_name}': [削除フェーズ-FAV] '{FAV_SHEET_NAME}' シートの全レコード取得を開始。")
        sheet_favorites = spreadsheet.worksheet(FAV_SHEET_NAME)
        all_favs_records = sheet_favorites.get_all_records()
        logger.info(f"ユーザー '{user_name}': [削除フェーズ-FAV] 全 {len(all_favs_records)} 件のレコードを取得完了。")
        
        rows_to_delete_favs = [i + 2 for i, r in enumerate(all_favs_records) if r.get('投票者') == user_name]
        
        if rows_to_delete_favs:
            logger.info(f"ユーザー '{user_name}': [削除フェーズ-FAV] {len(rows_to_delete_favs)} 行の既存データを発見。削除対象行: {rows_to_delete_favs}")
            requests_favs = [{"deleteDimension": {"range": {"sheetId": sheet_favorites.id, "dimension": "ROWS", "startIndex": s - 1, "endIndex": e}}} for s, e in reversed(_get_row_ranges(rows_to_delete_favs))]
            logger.info(f"ユーザー '{user_name}': [削除フェーズ-FAV] batch_update (削除) APIを呼び出します。")
            spreadsheet.batch_update({"requests": requests_favs})
            logger.info(f"ユーザー '{user_name}': [削除フェーズ-FAV] batch_update (削除) が完了。")
        else:
            logger.info(f"ユーザー '{user_name}': [削除フェーズ-FAV] 既存のお気に入りデータは見つかりませんでした。削除をスキップ。")
            
        # --- 4. 新しいデータの追加 ---
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        new_vote_rows = [[user_name, pid, '代表票', timestamp] for pid in voted_for_map.values()]
        new_free_vote_rows = [[user_name, pid, '自由票', timestamp] for pid in free_votes_list]
        total_new_votes = new_vote_rows + new_free_vote_rows
        
        if total_new_votes:
            logger.info(f"ユーザー '{user_name}': [追加フェーズ-VOTE] {len(total_new_votes)} 行の新しい投票データを追加します。")
            sheet_votes.append_rows(total_new_votes, value_input_option='USER_ENTERED')
            logger.info(f"ユーザー '{user_name}': [追加フェーズ-VOTE] append_rows (追加) が完了。")
        else:
            logger.info(f"ユーザー '{user_name}': [追加フェーズ-VOTE] 新しい投票データはありません。追加をスキップ。")
        
        new_fav_rows = [[user_name, pid] for pid in favorites_list]
        if new_fav_rows:
            logger.info(f"ユーザー '{user_name}': [追加フェーズ-FAV] {len(new_fav_rows)} 行の新しいお気に入りデータを追加します。")
            sheet_favorites.append_rows(new_fav_rows, value_input_option='USER_ENTERED')
            logger.info(f"ユーザー '{user_name}': [追加フェーズ-FAV] append_rows (追加) が完了。")
        else:
            logger.info(f"ユーザー '{user_name}': [追加フェーズ-FAV] 新しいお気に入りデータはありません。追加をスキップ。")
        
        # --- 5. 完了処理 ---
        logger.info(f"ユーザー '{user_name}': 全てのデータ保存処理が【正常に完了】しました。")
        st.session_state.save_status = "success"
        st.session_state.dirty = False # [修正] 正常に完了した場合のみ dirty フラグを False にする
        
    except Exception as e:
        # [修正] logger.exception を使うと、ターミナルに完全なスタックトレース(エラー詳細)が出力される
        logger.exception(f"ユーザー '{user_name}' のデータ保存中に【重大なエラー】が発生しました。")
        st.session_state.save_status = f"error: {e}"
        # [修正] エラー時は dirty = True のままにして、再試行の機会を残す
    
    finally:
        # --- 6. ロックの解放 ---
        # [修正] 'st.session_state.save_lock' ではなく、引数 'lock' を使用する
        lock.release() 
        logger.info(f"ユーザー '{user_name}': ロックを解放しました。保存スレッドを終了します。")

# app.py

# app.py

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
        
        # [修正] スレッドに渡す引数のタプルに st.session_state.save_lock を追加
        args = (
            st.session_state.user_name, 
            st.session_state.voted_for.copy(), 
            st.session_state.favorites.copy(), 
            st.session_state.free_votes.copy(),
            st.session_state.save_lock  # [修正] ロックオブジェクトそのものを引数として渡す
        )
        logger.info(f"スレッド引数: User='{args[0]}', "
                    f"Votes={len(args[1])}, "
                    f"Favs={len(args[2])}, "
                    f"FreeVotes={len(args[3])}, "
                    f"Lock={args[4]}") # [修正] ロックオブジェクトをログに出力
        
        save_thread = threading.Thread(target=save_all_progress, args=args)
        save_thread.start()
        logger.info(f"スレッド (target=save_all_progress) を .start() しました。")
        
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
        
        with st.spinner("過去の投票履歴を読み込んでいます..."):
            total_loaded = 0 # 読み込んだ履歴の件数をカウントする変数
            try:
                # 1. スプレッドシート接続と全データ取得
                spreadsheet = st.session_state.gc.open(SPREADSHEET_NAME)
                sheet_votes = spreadsheet.worksheet(VOTE_SHEET_NAME)
                all_data = sheet_votes.get_all_records()
                sheet_favs = spreadsheet.worksheet(FAV_SHEET_NAME)
                all_fav_data = sheet_favs.get_all_records()
                logger.info(f"ユーザー '{name}': 履歴読み込み - 投票{len(all_data)}件、お気に入り{len(all_fav_data)}件を取得。")

                # 2. ログインユーザーのデータ抽出
                user_votes = [r for r in all_data if r.get('投票者') == name]
                user_favs = [r for r in all_fav_data if r.get('投票者') == name]

                # 3. 代表票の履歴読み込み処理
                voted_map = {}
                rep_votes_records = [v for v in user_votes if v.get('投票の種類') == '代表票']

                for v_record in rep_votes_records:
                    photo_id = v_record.get('写真ID')
                    if not photo_id: continue
                    if photo_id in st.session_state.photo_id_map:
                        submitter = st.session_state.photo_id_map[photo_id].get('submitter')
                        if submitter: voted_map[submitter] = photo_id
                    else:
                        logger.warning(f"ユーザー '{name}': 履歴の写真ID '{photo_id}' がマスターに存在しません。")

                # 4. 自由票・お気に入り履歴の読み込み処理 (マスターに存在するIDのみ)
                free_votes_list = [v['写真ID'] for v in user_votes if v.get('投票の種類') == '自由票' and v.get('写真ID') and v['写真ID'] in st.session_state.photo_id_map]
                fav_list = [r['写真ID'] for r in user_favs if r.get('写真ID') and r['写真ID'] in st.session_state.photo_id_map]
                
                # 5. session_stateへの最終登録
                st.session_state.voted_for = voted_map
                st.session_state.free_votes = free_votes_list
                st.session_state.favorites = fav_list
                logger.info(f"ユーザー '{name}': 履歴読み込み完了。代表票{len(voted_map)}, 自由票{len(free_votes_list)}, お気に入り{len(fav_list)}")

                # [変更点] 読み込んだ件数をチェック
                total_loaded = len(voted_map) + len(free_votes_list) + len(fav_list)

            except Exception as e:
                logger.exception(f"ユーザー '{name}' の履歴読み込み中にエラーが発生。")
                st.error("履歴の読み込みに失敗しました。投票はリセットされた状態で開始されます。")
                st.session_state.voted_for, st.session_state.free_votes, st.session_state.favorites = {}, [], []
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
                save_all_progress(
                    st.session_state.user_name, 
                    st.session_state.voted_for,
                    st.session_state.favorites, 
                    st.session_state.free_votes, # <-- カンマを追加
                    st.session_state.save_lock
                )
                st.session_state.dirty = False
            
            st.balloons(); st.success("投票が完了しました！")
            st.session_state.voting_complete = True
            time.sleep(1.5)
            st.rerun()
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