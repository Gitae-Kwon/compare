import os
from io import BytesIO
from datetime import datetime

import streamlit as st
from PIL import Image
import imagehash
import boto3
from botocore.exceptions import ClientError
import pymysql
import pandas as pd

# =========================
# 설정 / 클라이언트 초기화
# =========================
aws_conf = st.secrets["aws"]
mysql_conf = st.secrets["mysql"]

BUCKET = aws_conf["bucket"]

s3 = boto3.client(
    "s3",
    aws_access_key_id=aws_conf["access_key"],
    aws_secret_access_key=aws_conf["secret_key"],
    region_name=aws_conf["region"],
)


def get_db_conn():
    return pymysql.connect(
        host=mysql_conf["host"],
        port=mysql_conf.get("port", 3306),
        user=mysql_conf["user"],
        password=mysql_conf["password"],
        db=mysql_conf["database"],
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )


# =========================
# 유틸 함수들
# =========================
def calc_phash(file_like):
    """이미지 파일 객체(또는 BytesIO)에서 perceptual hash 계산"""
    img = Image.open(file_like).convert("RGB")
    return imagehash.phash(img)


def similarity(h1, h2):
    """두 pHash 간 해밍거리로 유사도(%) 계산"""
    d = h1 - h2  # Hamming distance (0~64)
    return round((1 - d / 64) * 100, 2)


def upload_to_s3(file_like, original_name, prefix="images"):
    """
    file_like: BytesIO 또는 파일 객체
    original_name: 원본 파일명 (확장자 추출용)
    """
    ext = os.path.splitext(original_name)[1]
    if not ext:
        ext = ".png"

    key = f"{prefix}/{datetime.utcnow().strftime('%Y%m%d%H%M%S%f')}{ext}"

    try:
        s3.upload_fileobj(file_like, BUCKET, key)
    except ClientError as e:
        err = e.response.get("Error", {})
        st.error(
            f"S3 업로드 실패: 코드={err.get('Code')} "
            f"메시지={err.get('Message')}"
        )
        raise

    return key


def load_image_from_s3(key):
    """S3 object key로부터 PIL 이미지 로드"""
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    return Image.open(BytesIO(obj["Body"].read()))


def insert_image_record(file_name, s3_url, phash_str, description=None):
    """image_files 테이블에 한 줄 삽입"""
    conn = get_db_conn()
    with conn:
        with conn.cursor() as cur:
            sql = """
                INSERT INTO image_files (file_name, s3_url, phash, description)
                VALUES (%s, %s, %s, %s)
            """
            cur.execute(sql, (file_name, s3_url, phash_str, description))
        conn.commit()


def load_all_images():
    """image_files 테이블 전체 로드"""
    conn = get_db_conn()
    with conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM image_files ORDER BY id DESC")
            rows = cur.fetchall()
    return pd.DataFrame(rows)


# =========================
# Streamlit UI
# =========================
st.set_page_config(page_title="이미지 유사도 검사", layout="wide")
st.title("🖼 이미지 유사도 검사 (S3 + MySQL + pHash)")

tab1, tab2 = st.tabs(["📥 원본 이미지 등록", "🔍 업로드 이미지 비교"])

# -------------------------
# 탭 1: 원본 이미지 등록
# -------------------------
with tab1:
    st.subheader("📥 원본(레퍼런스) 이미지 등록")

    src_files = st.file_uploader(
        "원본 이미지 여러 장 선택",
        type=["jpg", "jpeg", "png", "webp"],
        accept_multiple_files=True,
        key="src_uploader",
    )

    desc_common = st.text_area(
        "상세 설명 (선택, 여러 장에 공통으로 적용)",
        "",
        placeholder="예) 플랫폼/작품명/캐릭터명 등 메모를 입력하세요.",
    )

    if st.button("💾 원본 이미지 S3 + DB 등록"):
        if not src_files:
            st.warning("먼저 이미지를 선택하세요.")
        else:
            count = 0
            for f in src_files:
                data = f.read()
                if not data:
                    continue

                # pHash 계산
                phash = calc_phash(BytesIO(data))
                phash_str = str(phash)

                # S3 업로드
                s3_key = upload_to_s3(BytesIO(data), f.name, prefix="source-images")
                s3_url = f"s3://{BUCKET}/{s3_key}"

                # DB 기록 (설명 포함)
                insert_image_record(
                    f.name,
                    s3_url,
                    phash_str,
                    description=desc_common if desc_common else None,
                )
                count += 1

            st.success(f"✅ 원본 이미지 {count}개 등록 완료!")

    st.markdown("### DB에 저장된 원본 이미지 목록")
    try:
        df = load_all_images()
        if df.empty:
            st.info("아직 저장된 원본 이미지가 없습니다.")
        else:
            st.write("👉 description 컬럼을 표에서 직접 수정한 뒤, 아래 ‘변경 내용 저장’ 버튼을 눌러주세요.")

            edited_df = st.data_editor(
                df,
                use_container_width=True,
                num_rows="fixed",  # 행 추가/삭제는 막고
                disabled=["id", "file_name", "s3_url", "phash", "uploaded_at"],
                key="image_table_editor",
            )

            # 선택된 행의 이미지 미리보기
            selected_rows = st.session_state.get("image_table_editor", {}).get(
                "selected_rows", []
            )

            if selected_rows:
                sel_idx = selected_rows[0]
                sel_row = edited_df.iloc[sel_idx]

                st.markdown("#### 🖼 선택한 원본 이미지 미리보기")

                try:
                    key = sel_row["s3_url"].split(f"s3://{BUCKET}/", 1)[-1]
                    img = load_image_from_s3(key)
                    st.image(
                        img,
                        caption=f"ID {sel_row['id']} | {sel_row['file_name']}",
                        use_column_width=False,
                    )
                except Exception as e:
                    st.error(f"이미지를 불러오는 중 오류: {e}")

            # 변경 내용 저장 버튼
            if st.button("💾 변경 내용 저장"):
                try:
                    conn = get_db_conn()
                    with conn:
                        with conn.cursor() as cur:
                            for _, row in edited_df.iterrows():
                                sql = "UPDATE image_files SET description = %s WHERE id = %s"
                                cur.execute(sql, (row["description"], row["id"]))
                        conn.commit()
                    st.success("✅ 모든 변경 내용을 DB에 저장했습니다.")
                except Exception as e:
                    st.error(f"설명 저장 중 오류: {e}")

    except Exception as e:
        st.error(f"DB 조회 오류: {e}")

# -------------------------
# 탭 2: 업로드 이미지 비교
# -------------------------
with tab2:
    st.subheader("🔍 업로드 이미지와 원본 DB 유사도 비교")

    cmp_file = st.file_uploader(
        "비교할 이미지 1장을 업로드하세요",
        type=["jpg", "jpeg", "png", "webp"],
        accept_multiple_files=False,
        key="cmp_uploader",
    )

    threshold = st.slider("표시할 최소 유사도(%)", 0, 100, 40, 5)
    top_n = st.slider("상위 몇 개까지 볼까요?", 1, 20, 5)

    if st.button("🔎 유사도 분석 실행"):
        if not cmp_file:
            st.warning("먼저 비교할 이미지를 업로드하세요.")
        else:
            src_df = load_all_images()
            if src_df.empty:
                st.error("원본 이미지가 아직 없습니다. 먼저 '원본 이미지 등록' 탭에서 추가하세요.")
            else:
                data = cmp_file.read()
                if not data:
                    st.error("업로드된 이미지 데이터를 읽을 수 없습니다.")
                else:
                    # 업로드 이미지 pHash
                    cmp_hash = calc_phash(BytesIO(data))

                    st.markdown("#### 업로드한 이미지")
                    st.image(Image.open(BytesIO(data)), width=300)

                    # DB의 phash 문자열 → imagehash 객체
                    src_df["hash_obj"] = src_df["phash"].apply(
                        imagehash.hex_to_hash
                    )

                    results = []
                    for _, row in src_df.iterrows():
                        sim = similarity(cmp_hash, row["hash_obj"])
                        if sim >= threshold:
                            results.append(
                                {
                                    "id": row["id"],
                                    "file_name": row["file_name"],
                                    "s3_url": row["s3_url"],
                                    "similarity": sim,
                                    "description": row.get("description"),
                                }
                            )

                    if not results:
                        st.info(f"유사도 {threshold}% 이상 결과가 없습니다.")
                    else:
                        res_df = (
                            pd.DataFrame(results)
                            .sort_values("similarity", ascending=False)
                            .head(top_n)
                        )

                        st.markdown("#### 유사도 결과")
                        for _, r in res_df.iterrows():
                            col1, col2 = st.columns([1, 2])
                            with col1:
                                key = r["s3_url"].split(f"s3://{BUCKET}/", 1)[-1]
                                img = load_image_from_s3(key)
                                st.image(
                                    img,
                                    caption=f"ID {r['id']} | {r['file_name']}",
                                )
                            with col2:
                                st.write(f"**유사도:** {r['similarity']}%")
                                st.write(f"**파일명:** {r['file_name']}")
                                st.write(f"**S3 경로:** `{r['s3_url']}`")
                                st.write(
                                    f"**설명:** {r['description'] or '설명 없음'}"
                                )
