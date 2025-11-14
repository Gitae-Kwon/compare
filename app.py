import os
from io import BytesIO
from datetime import datetime

import streamlit as st
from PIL import Image
import imagehash
import boto3
import pymysql
import pandas as pd

# -----------------------
# 설정 / 클라이언트 초기화
# -----------------------
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

# -----------------------
# 유틸 함수
# -----------------------
def calc_phash(file_like):
    img = Image.open(file_like).convert("RGB")
    return imagehash.phash(img)

def similarity(h1, h2):
    d = h1 - h2  # Hamming distance (0~64)
    return round((1 - d / 64) * 100, 2)

from botocore.exceptions import ClientError

def upload_to_s3(file, prefix="images"):
    ext = os.path.splitext(file.name)[1]
    key = f"{prefix}/{datetime.utcnow().strftime('%Y%m%d%H%M%S%f')}{ext}"
    try:
        s3.upload_fileobj(file, BUCKET, key)
    except ClientError as e:
        err = e.response.get("Error", {})
        st.error(f"S3 업로드 실패: 코드={err.get('Code')} 메시지={err.get('Message')}")
        raise
    return key

def load_image_from_s3(key):
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    return Image.open(BytesIO(obj["Body"].read()))

def insert_image_record(file_name, s3_url, phash_str):
    conn = get_db_conn()
    with conn:
        with conn.cursor() as cur:
            sql = """
                INSERT INTO image_files (file_name, s3_url, phash)
                VALUES (%s, %s, %s)
            """
            cur.execute(sql, (file_name, s3_url, phash_str))
        conn.commit()

def load_all_images():
    conn = get_db_conn()
    with conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM image_files")
            rows = cur.fetchall()
    return pd.DataFrame(rows)

# -----------------------
# Streamlit UI
# -----------------------
st.set_page_config(page_title="이미지 유사도 검사", layout="wide")
st.title("🖼 이미지 유사도 검사 (S3 + MySQL)")

tab1, tab2 = st.tabs(["📥 원본 이미지 등록", "🔍 업로드 이미지 비교"])

# --- 탭1: 원본 이미지 등록 ---
with tab1:
    st.subheader("📥 원본(레퍼런스) 이미지 등록")

    src_files = st.file_uploader(
        "원본 이미지 여러 장 선택",
        type=["jpg", "jpeg", "png", "webp"],
        accept_multiple_files=True
    )

    if st.button("💾 원본 이미지 S3 + DB 등록"):
        if not src_files:
            st.warning("먼저 이미지를 선택하세요.")
        else:
            count = 0
            for f in src_files:
                # S3 업로드
                s3_key = upload_to_s3(f, prefix="source-images")
                s3_url = f"s3://{BUCKET}/{s3_key}"

                # phash 계산
                f.seek(0)
                phash = calc_phash(f)
                phash_str = str(phash)

                # DB 기록
                insert_image_record(f.name, s3_url, phash_str)
                count += 1

            st.success(f"✅ 원본 이미지 {count}개 등록 완료!")

    st.markdown("### DB에 저장된 원본 이미지 목록")
    try:
        df = load_all_images()
        if df.empty:
            st.info("아직 저장된 이미지가 없습니다.")
        else:
            st.dataframe(df, use_container_width=True)
    except Exception as e:
        st.error(f"DB 조회 오류: {e}")

# --- 탭2: 이미지 비교 ---
with tab2:
    st.subheader("🔍 업로드 이미지와 원본 DB 유사도 비교")

    cmp_file = st.file_uploader(
        "비교할 이미지 1장 업로드",
        type=["jpg", "jpeg", "png", "webp"],
        accept_multiple_files=False
    )

    threshold = st.slider("표시할 최소 유사도(%)", 0, 100, 60, 5)
    top_n = st.slider("상위 몇 개까지 볼까요?", 1, 10, 5)

    if st.button("🔎 유사도 분석 실행"):
        if not cmp_file:
            st.warning("비교할 이미지를 업로드해주세요.")
        else:
            src_df = load_all_images()
            if src_df.empty:
                st.error("먼저 '원본 이미지 등록' 탭에서 이미지를 추가하세요.")
            else:
                # 업로드 이미지 phash
                buf = BytesIO(cmp_file.read())
                cmp_hash = calc_phash(BytesIO(buf.getvalue()))

                st.markdown("#### 업로드한 이미지")
                st.image(Image.open(BytesIO(buf.getvalue())), width=300)

                # DB phash 문자열 → hash 객체
                src_df["hash_obj"] = src_df["phash"].apply(imagehash.hex_to_hash)

                results = []
                for _, row in src_df.iterrows():
                    sim = similarity(cmp_hash, row["hash_obj"])
                    if sim >= threshold:
                        results.append({
                            "id": row["id"],
                            "file_name": row["file_name"],
                            "s3_url": row["s3_url"],
                            "similarity": sim
                        })

                if not results:
                    st.info(f"유사도 {threshold}% 이상인 결과가 없습니다.")
                else:
                    res_df = pd.DataFrame(results).sort_values("similarity", ascending=False).head(top_n)

                    st.markdown("#### 유사도 결과")
                    for _, r in res_df.iterrows():
                        col1, col2 = st.columns([1,2])
                        with col1:
                            # s3_url -> key만 추출
                            key = r["s3_url"].split(f"s3://{BUCKET}/")[-1]
                            img = load_image_from_s3(key)
                            st.image(img, caption=f"{r['file_name']} (ID {r['id']})")
                        with col2:
                            st.write(f"**유사도:** {r['similarity']}%")
                            st.write(f"**파일명:** {r['file_name']}")
                            st.write(f"**S3 경로:** `{r['s3_url']}`")
