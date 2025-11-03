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
import time

# --- 初期設定 ---
Image.MAX_IMAGE_PIXELS = None
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s',
)
logger = logging.getLogger(__name__)
st.set_page_config(layout="centered")

# --- UI改善CSS ---
st.markdown(
    """
    <style>
        /* --- ここからが追加部分 --- */
        @keyframes fadeIn {
          from {
            opacity: 0;
            transform: translateY(10px); /* 少し下からフワッと上がる演出 */
          }
          to {
            opacity: 1;
            transform: translateY(0);
          }
        }

        /* Streamlitのメインコンテンツエリアにアニメーションを適用 */
        div[data-testid="stAppViewContainer"] > .main {
            animation: fadeIn 0.4s ease-in-out;
        }
        /* --- ここまでが追加部分 --- */

        /* 以下は既存のCSS */
        div[data-testid="stImage"] { text-align: center; }
        div[data-stale="true"] { opacity: 1.0 !important; }
        div[data-stale="true"] * { opacity: 1.0 !important; }
        .stButton>button:disabled {
            opacity: 1.0 !important; color: white !important;
            background-color: #262730 !important;
            border: 1px solid rgba(250, 250, 250, 0.2) !important;
        }
        [data-testid="stDialog"] > div > div {
            width: 95vw;
            max-width: 95vw;
            height: 95vh;
            overflow: auto;
        }
    </style>
    """,
    unsafe_allow_html=True,
)

# --- 設定項目 ---
JSON_KEY_FILE = st.secrets["json_key_file"]
TARGET_FOLDER_ID = st.secrets["target_folder_id"]
SPREADSHEET_NAME = st.secrets["spreadsheet_name"]
VOTE_SHEET_NAME = st.secrets["vote_sheet_name"]
FAV_SHEET_NAME = st.secrets["fav_sheet_name"]
RESULTS_SHEET_NAME = st.secrets.get("results_sheet_name", "集計結果")
THUMBNAIL_SIZE_PX = 700
RESULT_THUMBNAIL_SIZE_PX = 1400

# --- グローバル定数 ---
# 正規表現パターンはここで一度だけコンパイルする
FILENAME_PATTERN = re.compile(r"^(.+?)(\d{2})(.+?)\..+$")


# --- 認証とデータ取得 ---

def authorize_services_for_thread():
    try:
        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_file(JSON_KEY_FILE, scopes=scopes)
        gc = gspread.authorize(creds)
        return gc
    except Exception:
        logger.exception("バックグラウンドスレッドでのGoogleサービス認証中にエラーが発生。")
        return None

@st.cache_resource
def authorize_services():
    try:
        logger.info("Googleサービスの認証を開始。")
        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_file(JSON_KEY_FILE, scopes=scopes)
        gc = gspread.authorize(creds)
        settings = {"client_config_backend": "service", "service_config": {"client_json_file_path": JSON_KEY_FILE}}
        gauth = GoogleAuth(settings=settings)
        gauth.ServiceAuth()
        drive = GoogleDrive(gauth)
        logger.info("Googleサービスの認証に成功。")
        return gc, drive
    except Exception:
        logger.exception("Googleサービスの認証中に致命的なエラーが発生。")
        st.error("Googleサービスへの接続に失敗しました。認証情報ファイルを確認してください。")
        st.stop()

@st.cache_resource
def load_photo_metadata(_drive):
    logger.info("写真メタデータの読み込みを開始。")
    photos_by_submitter = defaultdict(list)
    photo_id_map = {}
    query = f"'{TARGET_FOLDER_ID}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
    subfolders = _drive.ListFile({'q': query}).GetList()
    for folder in subfolders:
        # この fields パラメータを追加することで、取得する情報に thumbnailLink を含める
        file_list = _drive.ListFile({'q': f"'{folder['id']}' in parents and trashed=false", 'fields': 'items(id, title, mimeType, thumbnailLink)'}).GetList()
        for file_obj in file_list:
            if 'image' in file_obj['mimeType']:
                filename = file_obj['title']
                match = FILENAME_PATTERN.match(filename)
                if match:
                    submitter, title, photo_id = match.group(1).strip(), match.group(3).strip(), file_obj['id']
                    # ↓↓↓ この行に 'thumbnail' を追加する ↓↓↓
                    photo_info = {
                        'id': photo_id, 
                        'title': title, 
                        'submitter': submitter,
                        'thumbnail': file_obj.get('thumbnailLink') # thumbnailLinkを保存
                    }
                    photos_by_submitter[submitter].append(photo_info)
                    photo_id_map[photo_id] = photo_info
    logger.info(f"{len(photo_id_map)}件の写真メタデータを読み込み完了。")
    return dict(photos_by_submitter), photo_id_map


@st.cache_data
def get_high_res_photo(_drive, photo_id):
    try:
        photo_file = _drive.CreateFile({'id': photo_id})
        photo_file.FetchContent()
        # .content (ファイルオブジェクト) から .read() で中身を読み出し、
        # バイトデータそのものを返す
        return photo_file.content.read()
    except Exception:
        logger.exception(f"高画質写真の読み込みに失敗。Photo ID: {photo_id}")
        return None

# --- ヘルパー関数 ---

# --- この新しい万能コンポーネント関数を追加 ---
def render_photo_component(photo_id, context, key_prefix=""):
    photo_info = st.session_state.photo_id_map.get(photo_id)
    if not photo_info: return

    submitter = photo_info['submitter']
    is_rep_vote = st.session_state.voted_for.get(submitter) == photo_id
    is_free_vote = photo_id in st.session_state.free_votes
    is_favorite = photo_id in st.session_state.favorites

    # --- ヘッダーとアイコン表示 ---
    icons = []
    if is_rep_vote: icons.append("✅")
    if is_free_vote: icons.append("🗳️")
    # Phase 1のページではお気に入りアイコンもヘッダーに表示
    if context == 'vote' and is_favorite: icons.append("⭐")
    
    icon_text = " ".join(icons)
    st.subheader(f"{icon_text} 【{submitter}】 {photo_info['title']}".strip())

    # --- サムネイル画像表示 ---
    original_thumbnail_link = photo_info.get('thumbnail')
    sized_thumbnail_link = get_sized_thumbnail_link(original_thumbnail_link)
    thumbnail_content = get_thumbnail_photo(st.session_state.drive, sized_thumbnail_link)
    if thumbnail_content: st.image(thumbnail_content)
    else: st.error("画像読み込みエラー")

    # --- ボタン表示（コンテキストに応じて切り替え） ---
    if context == 'vote':
        col1, col2, col3 = st.columns([0.4, 0.4, 0.2])
        with col1:
            button_text = "この写真に投票しています" if is_rep_vote else "この作品に投票する"
            if st.button(button_text, key=f"{key_prefix}vote_{photo_id}"):
                st.session_state.voted_for[submitter] = photo_id
                st.session_state.dirty = True
                st.rerun()
        with col2:
            fav_button_text = "⭐ お気に入りから削除" if is_favorite else "⭐ お気に入りに追加"
            if st.button(fav_button_text, key=f"{key_prefix}fav_{photo_id}"):
                if is_favorite: st.session_state.favorites.remove(photo_id)
                else: st.session_state.favorites.append(photo_id)
                st.session_state.dirty = True
                st.rerun()
        with col3:
            if st.button("🖼️ フルサイズ", key=f"{key_prefix}full_{photo_id}"):
                show_fullscreen_dialog(photo_id)
    
    elif context == 'free_vote':
        votes_left = st.session_state.get("num_free_votes", 5) - len(st.session_state.free_votes)
        col1, col2, col3 = st.columns([0.4, 0.4, 0.2])
        with col1:
            if is_rep_vote:
                st.button("✅ 代表票", key=f"{key_prefix}_rep_vote_{photo_id}", disabled=True, use_container_width=True)
            else:
                if st.button("✅ 代表票に変更", key=f"{key_prefix}_rep_vote_{photo_id}", use_container_width=True):
                    st.session_state.voted_for[submitter] = photo_id
                    st.session_state.dirty = True
                    st.rerun()
        with col2:
            if is_free_vote:
                if st.button("🗳️ 投票を取り消す", key=f"{key_prefix}_unvote_{photo_id}", use_container_width=True):
                    st.session_state.free_votes.remove(photo_id)
                    st.session_state.dirty = True
                    st.rerun()
            elif votes_left > 0:
                if st.button("🗳️ 自由票を投票する", key=f"{key_prefix}_vote_{photo_id}", use_container_width=True):
                    st.session_state.free_votes.append(photo_id)
                    st.session_state.dirty = True
                    st.rerun()
            else:
                st.markdown(
                    """<div style="display: flex; align-items: center; justify-content: center; height: 38.4px; border: 1px solid #31333F; border-radius: 0.5rem; background-color: #1E1F26; color: rgba(250, 250, 250, 0.4); font-size: 14px; text-align: center; padding: 0 10px;">投票枠がありません</div>""",
                    unsafe_allow_html=True)
        with col3:
            if st.button("🖼️ フルサイズ", key=f"{key_prefix}_full_{photo_id}", use_container_width=True):
                show_fullscreen_dialog(photo_id)

    st.write("---")

def get_sized_thumbnail_link(original_link, size=THUMBNAIL_SIZE_PX):
    """サムネイルリンクにサイズ指定を追加する"""
    if not original_link:
        return None
    # Google DriveのサムネイルURLの末尾にサイズ指定パラメータを追加
    return f"{original_link.split('=')[0]}=s{size}"

@st.cache_data
def get_thumbnail_photo(_drive, thumbnail_link):
    if not thumbnail_link:
        return None
    try:
        # PyDriveの認証済みHTTPクライアントを使ってサムネイルURLにアクセスする
        http = _drive.auth.http
        resp, content = http.request(thumbnail_link)
        if resp.status == 200:
            return content
        else:
            return None
    except Exception:
        logger.exception(f"サムネイルの読み込みに失敗。Link: {thumbnail_link}")
        return None


def _get_row_ranges(rows):
    """連続する行番号のリストを(start, end)のタプルのリストに変換する"""
    if not rows:
        return []
    sorted_rows = sorted(list(set(rows)))
    ranges = []
    start = sorted_rows[0]
    end = sorted_rows[0]
    for row in sorted_rows[1:]:
        if row == end + 1:
            end = row
        else:
            ranges.append((start, end))
            start = row
            end = row
    ranges.append((start, end))
    # [(start1, end1), (start2, end2), ...]
    return ranges

### フェーズ2: 保存関数を自由票に対応 (API負荷 改善版) ###
def save_all_progress(user_name, voted_for_map, favorites_list, free_votes_list):
    try:
        logger.info(f"ユーザー '{user_name}' のデータ保存処理（バックグラウンド）を開始。")
        gc_thread = authorize_services_for_thread()
        if not gc_thread: 
            st.session_state.save_status = "error: GSpread認証失敗"
            return

        spreadsheet = gc_thread.open(SPREADSHEET_NAME)
        sheet_votes = spreadsheet.worksheet(VOTE_SHEET_NAME)
        sheet_favorites = spreadsheet.worksheet(FAV_SHEET_NAME)
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # --- ここからが修正箇所 (batch_updateによる一括削除) ---
        
        # 1. 投票シートの削除リクエストを作成
        all_votes_records = sheet_votes.get_all_records()
        rows_to_delete_votes = [i + 2 for i, record in enumerate(all_votes_records) if record.get('投票者') == user_name]
        vote_ranges = _get_row_ranges(rows_to_delete_votes)
        
        vote_delete_requests = []
        if vote_ranges:
            sheet_votes_id = sheet_votes.id
            # 範囲を逆順 (行番号が大きい順) に処理し、リクエストを作成
            for start, end in reversed(vote_ranges):
                vote_delete_requests.append({
                    "deleteDimension": {
                        "range": {
                            "sheetId": sheet_votes_id,
                            "dimension": "ROWS",
                            "startIndex": start - 1, # 0-indexed
                            "endIndex": end         # 0-indexed (Exclusive)
                        }
                    }
                })
        
        # 2. お気に入りシートの削除リクエストを作成
        all_favs_records = sheet_favorites.get_all_records()
        rows_to_delete_favs = [i + 2 for i, record in enumerate(all_favs_records) if record.get('投票者') == user_name]
        fav_ranges = _get_row_ranges(rows_to_delete_favs)
        
        fav_delete_requests = []
        if fav_ranges:
            sheet_favorites_id = sheet_favorites.id
            for start, end in reversed(fav_ranges):
                fav_delete_requests.append({
                    "deleteDimension": {
                        "range": {
                            "sheetId": sheet_favorites_id,
                            "dimension": "ROWS",
                            "startIndex": start - 1, # 0-indexed
                            "endIndex": end         # 0-indexed (Exclusive)
                        }
                    }
                })

        # 3. 削除リクエストを一括実行 (APIコールは最大2回)
        if vote_delete_requests:
            spreadsheet.batch_update({"requests": vote_delete_requests})
            logger.info(f"'{user_name}' の古い投票データ {len(rows_to_delete_votes)} 行を削除しました。")
            
        if fav_delete_requests:
            spreadsheet.batch_update({"requests": fav_delete_requests})
            logger.info(f"'{user_name}' の古いお気に入り {len(rows_to_delete_favs)} 行を削除しました。")
            
        # --- ここまでが修正箇所 ---

        # 新しいデータを追加 (この部分は変更なし)
        new_vote_rows = [[user_name, photo_id, '代表票', timestamp] for photo_id in voted_for_map.values()]
        new_free_vote_rows = [[user_name, photo_id, '自由票', timestamp] for photo_id in free_votes_list]
        all_new_votes = new_vote_rows + new_free_vote_rows
        if all_new_votes: sheet_votes.append_rows(all_new_votes, value_input_option='USER_ENTERED')
        
        new_fav_rows = [[user_name, photo_id] for photo_id in favorites_list]
        if new_fav_rows: sheet_favorites.append_rows(new_fav_rows, value_input_option='USER_ENTERED')
        
        logger.info(f"ユーザー '{user_name}' のデータ保存が正常に完了。")
        st.session_state.save_status = "success"
    except Exception as e:
        logger.exception(f"ユーザー '{user_name}' のデータ保存中（バックグラウンド）にエラーが発生。")
        st.session_state.save_status = f"error: {e}"

def scroll_to_top():
    components.html(
        """
        <script>
            window.parent.document.querySelector(".main").scrollTo({top: 0, behavior: 'smooth'});
        </script>
        """,
        height=0
    )

### フェーズ2: バックグラウンド保存を自由票に対応 ###
def transition_and_save_in_background(view=None, index_change=0):
    if st.session_state.dirty:
        st.toast("変更を保存しています...", icon="⏳")
        st.session_state.save_status = "pending"
        # 保存するデータをコピー
        user_name_to_save = st.session_state.user_name
        voted_for_to_save = st.session_state.voted_for.copy()
        favorites_to_save = st.session_state.favorites.copy()
        free_votes_to_save = st.session_state.free_votes.copy() # free_votesもコピー
        
        # スレッドで保存実行
        save_thread = threading.Thread(target=save_all_progress, args=(user_name_to_save, voted_for_to_save, favorites_to_save, free_votes_to_save))
        save_thread.start()
        st.session_state.dirty = False
    
    if view or index_change != 0:
        st.session_state.needs_scroll = True
    if view: st.session_state.view = view
    st.session_state.current_index += index_change
    st.rerun()

@st.dialog("フルサイズ表示")
def show_fullscreen_dialog(photo_id):
    photo_info = st.session_state.photo_id_map.get(photo_id, {})
    st.subheader(f"【{photo_info.get('submitter')}】 {photo_info.get('title')}")
    placeholder = st.empty()
    with placeholder:
        st.spinner("画像を読み込んでいます...")
    
    dialog_photo_bytes = get_high_res_photo(st.session_state.drive, photo_id)
    
    if dialog_photo_bytes:
        # dialog_photo_bytes は既にバイトデータなので、.read() は不要
        b64_image = base64.b64encode(dialog_photo_bytes).decode()
        placeholder.markdown(
            f'<img src="data:image/jpeg;base64,{b64_image}" style="width: 100%;">',
            unsafe_allow_html=True,
        )
    else:
        placeholder.error("画像の読み込みに失敗しました。")



@st.cache_data(ttl=300) # 5分間キャッシュ
def fetch_processed_results(_gc):
    """【新機能】管理者が作成した「集計結果」シートからデータを取得する"""
    try:
        logger.info("集計結果シートの読み込み（キャッシュ）を開始。")
        spreadsheet = _gc.open(SPREADSHEET_NAME)
        # --- ここが 'VOTE_SHEET_NAME' ではない ---
        sheet_results = spreadsheet.worksheet(RESULTS_SHEET_NAME) 
        all_results_data = sheet_results.get_all_records()
        logger.info(f"{len(all_results_data)}件の集計結果行を読み込み完了。")
        return all_results_data
    except Exception as e:
        logger.exception("集計結果シートの読み込み中にエラーが発生。")
        return None

# --- ページごとの描画関数 ---

### フェーズ2: ログイン時の読み込みを自由票に対応 ###
def render_login_page():
    st.header("ようこそ！")
    name = st.text_input("あなたの学年とクラス、名前を入力してください。例:2H森口蓮音")
    if st.button("決定"):
        if name:  # name が空でないことを確認
            st.session_state.user_name = name
            # ... (以降の処理は同じ)
            st.session_state.view = 'instructions'
            st.rerun()
        else:
            # name が空の場合に警告メッセージを表示
            st.warning("名前を入力してください。")
        with st.spinner('投票履歴とお気に入りを確認中...'):
            try:
                spreadsheet = st.session_state.gc.open(SPREADSHEET_NAME)
                sheet_votes = spreadsheet.worksheet(VOTE_SHEET_NAME)
                all_votes_data = sheet_votes.get_all_records()
                
                # 代表票の読み込み
                user_rep_votes = [v for v in all_votes_data if v.get('投票者') == name and v.get('投票の種類') == '代表票']
                voted_for_map = {}
                for vote in user_rep_votes:
                    photo_id = vote.get('写真ID')
                    if photo_id and photo_id in st.session_state.photo_id_map:
                        submitter = st.session_state.photo_id_map[photo_id].get('submitter')
                        if submitter: voted_for_map[submitter] = photo_id
                st.session_state.voted_for = voted_for_map

                # 自由票の読み込み
                user_free_votes = [v.get('写真ID') for v in all_votes_data if v.get('投票者') == name and v.get('投票の種類') == '自由票']
                st.session_state.free_votes = user_free_votes

                # お気に入りの読み込み
                sheet_favorites = spreadsheet.worksheet(FAV_SHEET_NAME)
                all_favs_data = sheet_favorites.get_all_records()
                st.session_state.favorites = [f.get('写真ID') for f in all_favs_data if f.get('投票者') == name and f.get('写真ID')]
                
                st.session_state.needs_scroll = True
            except Exception as e:
                logger.exception(f"ユーザー '{name}' の履歴読み込み中にエラーが発生。")
                st.error(f"履歴の読み込みに失敗しました: {e}")
                st.session_state.voted_for, st.session_state.favorites, st.session_state.free_votes = {}, [], []
        st.session_state.view = 'instructions'
        st.rerun()

def render_instructions_page():
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

        **このアプリについて**
        - 600行ほどの感動するほどクリーンなPythonコードと、streamlitを使って構築されています。
        - UIはちょっとゴミかもだけど、UXはめっちゃ考慮されてるので、感謝して投票してください。
        - 画面遷移時に画面がガクガクするのは仕様です。改善策を知ってるやつは俺に教えてくれマジで

        ---
        """
    )

    with st.spinner("最初の写真を準備しています..."):
        first_submitter = st.session_state.submitter_list[0]
        photos_to_preload = st.session_state.photos_by_submitter.get(first_submitter, [])
        for photo_meta in photos_to_preload:
            # --- ここを修正 ---
            original_thumbnail_link = photo_meta.get('thumbnail')
            sized_thumbnail_link = get_sized_thumbnail_link(original_thumbnail_link)
            get_thumbnail_photo(st.session_state.drive, sized_thumbnail_link)
            # --- ここまで ---

    st.success("準備ができました！")
    if st.button("投票を開始する", type="primary", use_container_width=True):
        st.session_state.view = 'vote'
        st.session_state.needs_scroll = True
        st.rerun()

def render_vote_page():
    if st.session_state.get('needs_scroll', False):
        scroll_to_top()
        st.session_state.needs_scroll = False
    
    current_index = st.session_state.current_index
    submitter_list = st.session_state.submitter_list
    current_submitter = submitter_list[current_index]
    next_submitter = submitter_list[current_index + 1] if (current_index + 1) < len(submitter_list) else None

    st.header(f"({current_index + 1}/{len(submitter_list)}) 「{current_submitter}」さんの作品")
    if st.button(f"⭐ お気に入り一覧を見る ({len(st.session_state.favorites)}件)"):
        transition_and_save_in_background(view='favorites')

    # --- ここが大幅にシンプルになる ---
    photos_metadata = st.session_state.photos_by_submitter.get(current_submitter, [])
    for photo_meta in photos_metadata:
        render_photo_component(photo_meta['id'], context='vote') # コンポーネントを呼び出すだけ
    # --- ここまで ---

    col_nav1, col_nav2 = st.columns(2)
    with col_nav1:
        if current_index > 0 and st.button("◀️ 前の人に戻る"):
            transition_and_save_in_background(index_change=-1)
    with col_nav2:
        if next_submitter:
            if st.button(f"次の人: {next_submitter} へ ▶️"):
                transition_and_save_in_background(index_change=+1)
        else:
            if st.button("🎉 全員の投票が完了！自由投票に進む"):
                transition_and_save_in_background(view='free_vote')
                
    if next_submitter:
        photos_to_preload = st.session_state.photos_by_submitter.get(next_submitter, [])
        for photo_meta in photos_to_preload:
            original_thumbnail_link = photo_meta.get('thumbnail')
            sized_thumbnail_link = get_sized_thumbnail_link(original_thumbnail_link)
            get_thumbnail_photo(st.session_state.drive, sized_thumbnail_link)

def render_favorites_page():
    if st.session_state.get('needs_scroll', False):
        scroll_to_top()
        st.session_state.needs_scroll = False

    st.header("⭐ お気に入り一覧")
    if st.button("◀️ 投票に戻る"):
        transition_and_save_in_background(view='vote')
    st.write("---")
    
    if not st.session_state.favorites:
        st.info("お気に入りに登録された写真はありません。")
    else:
        # --- ここからが修正箇所 ---
        for photo_id in reversed(st.session_state.favorites):
            # 新しいコンポーネントを呼び出す
            render_photo_component(photo_id, context='favorites', key_prefix="fav_page")
        # --- ここまで ---

### フェーズ2: 自由投票ページを新規作成 (最終改善版) ###
def render_free_vote_page():
    st.header("Phase 2: 自由投票")
    st.success("代表票の投票、お疲れ様でした！このページで代表票の変更もできます。")
    
    num_free_votes = st.session_state.get("num_free_votes", 5)
    votes_left = num_free_votes - len(st.session_state.free_votes)
    st.info(f"残り自由票: **{votes_left}** / {num_free_votes}")
    st.write("---")

    # --- ここが大幅にシンプルになる ---
    with st.expander("⭐ お気に入りから選ぶ", expanded=False):
        if not st.session_state.favorites:
            st.write("お気に入りに登録された写真はありません。")
        else:
            for photo_id in st.session_state.favorites:
                render_photo_component(photo_id, context='free_vote', key_prefix="fav")

    for submitter in st.session_state.submitter_list:
        with st.expander(f"「{submitter}」さんの作品一覧", expanded=False):
            photos = st.session_state.photos_by_submitter.get(submitter, [])
            for photo in photos:
                render_photo_component(photo['id'], context='free_vote', key_prefix="all")
    # --- ここまで ---
    
    st.write("") # スペーサー

    # 投票完了フラグに応じて、表示するボタンを切り替える
    if not st.session_state.get('voting_complete', False):
        # --- 1. まだ投票完了ボタンを押していない場合 ---
        if st.button("全ての投票を完了する", type="primary", use_container_width=True):
            with st.spinner("最終投票を保存しています..."):
                save_all_progress(
                    st.session_state.user_name,
                    st.session_state.voted_for,
                    st.session_state.favorites,
                    st.session_state.free_votes
                )
                st.session_state.dirty = False
            
            st.balloons()
            st.success("投票が完了しました！") # メッセージを簡潔に変更
            
            # --- ここでフラグを立て、ページをリロードする ---
            st.session_state.voting_complete = True
            time.sleep(1.5) # バルーンとメッセージを 1.5秒 見せる
            st.rerun()

    else:
        # --- 2. 投票完了ボタンを押した後 ---
        st.success("投票お疲れ様でした！") # 完了メッセージを表示
        
        # 「結果を見る」ボタンを表示
        if st.button("🏆 最終結果を見る", type="primary", use_container_width=True):
            st.session_state.view = 'results'
            st.session_state.needs_scroll = True
            st.rerun()

### フェーズ3: 結果発表ページ (同率順位対応・バグ修正版) ###
def render_results_page():
    if st.session_state.get('needs_scroll', False):
        scroll_to_top()
        st.session_state.needs_scroll = False
    
    st.header("🏆 総合結果発表 🏆")

    if st.button("◀️ 投票ページに戻る"):
        transition_and_save_in_background(view='free_vote')

    # --- 1. スプレッドシートから「集計済みのスコア」を取得 ---
    scores_data = fetch_processed_results(st.session_state.gc)
    if scores_data is None:
        st.error("集計結果シートの読み込みに失敗しました。")
        st.warning(f"スプレッドシートに「{RESULTS_SHEET_NAME}」という名前のシートがあり、データが入力されているか確認してください。")
        return

    # --- 2. アプリが起動時に読み込んだ「写真マスタ」を取得 ---
    if not st.session_state.photo_id_map:
        st.error("写真マスタ（photo_id_map）が読み込まれていません。")
        return

    try:
        # --- 3. 2つのデータをPython（Pandas）で結合 ---
        scores_df = pd.DataFrame(scores_data)
        required_score_cols = ['写真ID', 'スコア']
        if not all(col in scores_df.columns for col in required_score_cols):
            st.error(f"集計シートに必要な列（'写真ID', 'スコア'）がありません。")
            return

        master_df = pd.DataFrame.from_dict(st.session_state.photo_id_map, orient='index')
        master_df.index.name = '写真ID'
        master_df = master_df.reset_index()

        results_df = pd.merge(master_df, scores_df, on="写真ID", how="left")
        
        results_df[['スコア']] = results_df[['スコア']].fillna(0)
        results_df['スコア'] = pd.to_numeric(results_df['スコア'], errors='coerce').fillna(0).astype(int)
        
        # スコア順にソート
        results_df = results_df.sort_values('スコア', ascending=False).reset_index(drop=True)

        # --- ▼▼▼ ここから同率順位の対処 ▼▼▼ ---
        # 'min' method: 同点の場合、グループ内の最小順位を全員に割り当てる
        # (例: スコア 100, 90, 90, 80 -> 順位 1, 2, 2, 4)
        results_df['順位'] = results_df['スコア'].rank(method='min', ascending=False).astype(int)
        # --- ▲▲▲ ここまで ▲▲▲ ---

        display_cols = ['submitter', 'title', 'スコア'] 

        # --- 4. 結果の表示 (画像表示スタイル) ---

        # --- ① トップ5の発表 ---
        st.subheader("🎉 トップ5入賞作品")
        top_5_df = results_df.head(5)
        
        for index, row in top_5_df.iterrows():
            # --- ▼▼▼ 順位の参照を row['順位'] に変更 ▼▼▼ ---
            st.markdown(f"### <span style='color: gold;'>【第 {row['順位']} 位】</span> スコア: {row['スコア']}", unsafe_allow_html=True)
            st.subheader(f"【{row['submitter']}】 {row['title']}")
            
            original_thumbnail_link = row.get('thumbnail')
            # ユーザーが設定した変数 THUMBNAIL_SIZE_PX_RESULT を使用
            sized_thumbnail_link = get_sized_thumbnail_link(original_thumbnail_link, size=THUMBNAIL_SIZE_PX_RESULT)
            thumbnail_content = get_thumbnail_photo(st.session_state.drive, sized_thumbnail_link)
            if thumbnail_content:
                st.image(thumbnail_content)
            else:
                st.error("画像読み込みエラー")
            st.write("---")

        # --- ② 全体ランキング ---
        with st.expander("6位以下の全ランキングを見る"):
            remaining_df = results_df.iloc[5:]
            if remaining_df.empty:
                st.info("6位以下の作品はありません。")
            else:
                for index, row in remaining_df.iterrows():
                    # --- ▼▼▼ 順位の参照を row['順位'] に変更 ▼▼▼ ---
                    st.markdown(f"**【第 {row['順位']} 位】 スコア: {row['スコア']}**", unsafe_allow_html=True)
                    st.subheader(f"【{row['submitter']}】 {row['title']}")
                    
                    original_thumbnail_link = row.get('thumbnail')
                    sized_thumbnail_link = get_sized_thumbnail_link(original_thumbnail_link, size=THUMBNAIL_SIZE_PX_RESULT)
                    thumbnail_content = get_thumbnail_photo(st.session_state.drive, sized_thumbnail_link)
                    if thumbnail_content:
                        st.image(thumbnail_content, use_container_width=True)
                    st.write("---")

        st.write("") # スペーサー
        
        # --- ③ 自分の作品の票数 ---
        st.subheader("マイページ：自分の作品の票数")
        my_name = st.session_state.user_name
        
        my_results_df = results_df[results_df['submitter'] == my_name].sort_values('スコア', ascending=False)
        
        if my_results_df.empty:
            st.warning(f"「{my_name}」さんの出品作品が見つかりませんでした。")
        else:
            for index, row in my_results_df.iterrows():
                # --- ▼▼▼ 順位の参照を row['順位'] に変更 ▼▼▼ ---
                # (バグ修正: index + 1 ではなく、row['順位'] を使う)
                st.markdown(f"**【全体 {row['順位']} 位】 スコア: {row['スコア']}**", unsafe_allow_html=True)
                st.subheader(f"【{row['submitter']}】 {row['title']}")
                
                original_thumbnail_link = row.get('thumbnail')
                sized_thumbnail_link = get_sized_thumbnail_link(original_thumbnail_link, size=THUMBNAIL_SIZE_PX_RESULT)
                thumbnail_content = get_thumbnail_photo(st.session_state.drive, sized_thumbnail_link)
                if thumbnail_content:
                    st.image(thumbnail_content, use_container_width=True)
                st.write("---")

    except Exception as e:
        st.error(f"結果の表示中にエラーが発生しました: {e}")
        logger.exception("結果ページの描画エラー")

# --- メイン処理 ---
def main():
    # このブロックは、ユーザーのセッションが始まった最初の1回だけ実行される
    if 'view' not in st.session_state:
        # まず、全てのsession_state変数をここで定義する
        st.session_state.view = 'login'
        st.session_state.user_name = ''
        st.session_state.voted_for = {}
        st.session_state.favorites = []
        st.session_state.free_votes = []
        st.session_state.current_index = 0
        st.session_state.dirty = False
        st.session_state.needs_scroll = False
        st.session_state.voting_complete = False
        
        # 時間のかかる処理はスピナーの中で行う
        with st.spinner("アプリを起動しています..."):
            # 認証とサービス接続
            gc, drive = authorize_services()
            st.session_state.gc = gc
            st.session_state.drive = drive
            
            try:
                # スプレッドシートを開く
                spreadsheet = st.session_state.gc.open(SPREADSHEET_NAME)
                
                # Settingsシートから自由票の数を読み込む
                try:
                    settings_sheet = spreadsheet.worksheet("Settings")
                    num_votes = int(settings_sheet.acell('B1').value)
                    st.session_state.num_free_votes = num_votes
                    logger.info(f"設定シートから自由票の数 ({num_votes}) を読み込みました。")
                except (gspread.exceptions.WorksheetNotFound, ValueError, TypeError) as e:
                    logger.warning(f"設定シートの読み込みに失敗。デフォルト値(5)を使いますが、書き込み用シートを確認してください。エラー: {e}")
                    st.session_state.num_free_votes = 5 # 失敗時のデフォルト値

                # 写真メタデータの読み込み
                photos_by_submitter, photo_id_map = load_photo_metadata(st.session_state.drive)
                if not photos_by_submitter or not photo_id_map:
                    st.error("写真データを1件も見つけられませんでした。")
                    st.stop()
                
                st.session_state.photos_by_submitter = photos_by_submitter
                st.session_state.photo_id_map = photo_id_map
                st.session_state.submitter_list = sorted(list(st.session_state.photos_by_submitter.keys()))
                logger.info("アプリの起動準備が完了。")

            except Exception as e:
                logger.exception("アプリの起動中に致命的なエラーが発生しました。")
                st.error(f"アプリの起動に失敗しました。管理者にご連絡ください。エラー: {e}")
                st.stop()
    
    # 保存結果のトースト通知
    if st.session_state.get("save_status") and st.session_state.save_status != "pending":
        if st.session_state.save_status == "success":
            st.toast("変更が正常に保存されました！", icon="✅")
        else:
            st.toast(f"エラー: 保存に失敗しました。", icon="❌")
            logger.error(f"保存失敗: {st.session_state.save_status}")
        del st.session_state["save_status"]

    st.title("写真部 投票アプリ")
    if st.session_state.view == 'login':
        render_login_page()
    # ↓↓↓ このelifブロックを追加 ↓↓↓
    elif st.session_state.view == 'instructions':
        render_instructions_page()
    elif st.session_state.view == 'vote':
        render_vote_page()
    elif st.session_state.view == 'favorites':
        render_favorites_page()
    elif st.session_state.view == 'free_vote':
        render_free_vote_page()
    elif st.session_state.view == 'results': # <-- ここから
        render_results_page()                # <-- ここまでを
                                             # <-- まるごと追加
if __name__ == "__main__":
    main()