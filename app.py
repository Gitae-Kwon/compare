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
import numpy as np
import cv2
from insightface.app import FaceAnalysis

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


@st.cache_resource
def get_face_app():
    """InsightFace ArcFace + RetinaFace 초기화 (CPU)"""
    app = FaceAnalysis(name="buffalo_l")
    app.prepare(ctx_id=-1, det_size=(256, 256))
    return app


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
# 유틸 함수들 (해시 / 임베딩)
# =========================
def calc_phash(pil_img: Image.Image) -> str:
    """이미지 pHash -> hex 문자열"""
    return str(imagehash.phash(pil_img.convert("RGB")))


def calc_arcface_embedding(pil_img: Image.Image) -> np.ndarray | None:
    """PIL 이미지에서 ArcFace 임베딩(512차원) 계산"""
    app = get_face_app()
    nimg = cv2.cvtColor(np.array(pil_img), cv2.COLOR_RGB2BGR)
    faces = app.get(nimg)
    if not faces:
        return None
    faces.sort(
        key=lambda f: (f.bbox[2] - f.bbox[0]) * (f.bbox[3] - f.bbox[1]),
        reverse=True,
    )
    emb = faces[0].normed_embedding.astype(np.float32)
    return emb


def embedding_to_str(vec: np.ndarray | None) -> str | None:
    if vec is None:
        return None
    return ",".join(f"{x:.6f}" for x in vec.tolist())


def str_to_embedding(s: str | None) -> np.ndarray | None:
    if not s:
        return None
    return np.fromstring(s, sep=",", dtype=np.float32)


def cosine_similarity(v1: np.ndarray, v2: np.ndarray) -> float:
    num = float(np.dot(v1, v2))
    den = float(np.linalg.norm(v1) * np.linalg.norm(v2) + 1e-8)
    return num / den  # -1 ~ 1


def phash_similarity(h1: str, h2: str) -> float:
    """pHash 해밍 거리 기반 유사도 (0~100%)"""
    if not h1 or not h2:
        return 0.0
    a = imagehash.hex_to_hash(h1)
    b = imagehash.hex_to_hash(h2)
    d = a - b  # hamming distance
    sim = (1 - d / 64) * 100
    return float(round(sim, 2))


# =========================
# S3 유틸
# =========================
def upload_to_s3(file_like, original_name, prefix="images"):
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
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    return Image.open(BytesIO(obj["Body"].read())).convert("RGB")


# =========================
# DB 유틸
# =========================
def insert_image_record(
    file_name,
    s3_url,
    phash_str: str | None,
    description: str | None = None,
    face_embed: str | None = None,
):
    conn = get_db_conn()
    with conn:
        with conn.cursor() as cur:
            sql = """
                INSERT INTO image_files (file_name, s3_url, phash, description, face_embed)
                VALUES (%s, %s, %s, %s, %s)
            """
            cur.execute(sql, (file_name, s3_url, phash_str, description, face_embed))
        conn.commit()


def load_all_images():
    conn = get_db_conn()
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, file_name, s3_url, phash, description, uploaded_at, face_embed "
                "FROM image_files ORDER BY id DESC"
            )
            rows = cur.fetchall()
    return pd.DataFrame(rows)


def update_descriptions_from_df(df: pd.DataFrame):
    """data_editor로 수정된 description을 DB에 반영"""
    conn = get_db_conn()
    with conn:
        with conn.cursor() as cur:
            for _, row in df.iterrows():
                cur.execute(
                    "UPDATE image_files SET description=%s WHERE id=%s",
                    (row.get("description"), int(row["id"])),
                )
        conn.commit()


# =========================
# Streamlit UI
# =========================
st.set_page_config(page_title="이미지 유사도 검사", layout="wide")
st.title("🖼 이미지 유사도 검사 (S3 + MySQL + ArcFace)")

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

                pil = Image.open(BytesIO(data)).convert("RGB")

                # 1) pHash
                phash_str = calc_phash(pil)

                # 2) ArcFace 임베딩
                emb_vec = calc_arcface_embedding(pil)
                emb_str = embedding_to_str(emb_vec)

                # 3) S3 업로드
                s3_key = upload_to_s3(BytesIO(data), f.name, prefix="source-images")
                s3_url = f"s3://{BUCKET}/{s3_key}"

                # 4) DB 기록
                insert_image_record(
                    f.name,
                    s3_url,
                    phash_str,
                    description=desc_common if desc_common else None,
                    face_embed=emb_str,
                )
                count += 1

            st.success(f"✅ 원본 이미지 {count}개 등록 완료!")

    st.markdown("### DB에 저장된 원본 이미지 목록")

    try:
        df = load_all_images()
    except Exception as e:
        st.error(f"DB 조회 오류: {e}")
        df = pd.DataFrame()

    if df.empty:
        st.info("아직 저장된 원본 이미지가 없습니다.")
    else:
        st.write(
            "👉 description 컬럼을 표에서 직접 수정한 뒤, "
            "아래 ‘변경 내용 저장’ 버튼을 눌러주세요."
        )

        edited_df = st.data_editor(
            df,
            use_container_width=True,
            num_rows="fixed",
            disabled=["id", "file_name", "s3_url", "phash", "uploaded_at", "face_embed"],
            key="image_table_editor",
        )

        if st.button("📝 변경 내용 저장"):
            try:
                update_descriptions_from_df(edited_df)
                st.success("설명 변경 내용을 저장했습니다.")
            except Exception as e:
                st.error(f"설명 저장 중 오류: {e}")

        # 썸네일 + 미리보기
        st.markdown("### 표지 썸네일 & 미리보기")

        for _, row in edited_df.iterrows():
            col_id, col_name, col_desc, col_thumb, col_btn = st.columns(
                [0.5, 2.5, 3, 1, 1]
            )
            with col_id:
                st.write(int(row["id"]))
            with col_name:
                st.write(row["file_name"])
            with col_desc:
                st.write(row.get("description") or "")

            key = row["s3_url"].split(f"s3://{BUCKET}/", 1)[-1]
            with col_thumb:
                try:
                    img = load_image_from_s3(key)
                    st.image(img, width=80)
                except Exception:
                    st.write("썸네일 오류")

            with col_btn:
                if st.button("미리보기", key=f"preview_{row['id']}"):
                    try:
                        img = load_image_from_s3(key)
                        st.image(
                            img,
                            caption=f"ID {row['id']} | {row['file_name']}",
                            use_column_width=True,
                        )
                    except Exception as e:
                        st.error(f"이미지 로딩 오류: {e}")

# -------------------------
# 탭 2: 업로드 이미지 비교
# -------------------------
with tab2:
    st.subheader("🔍 업로드 이미지와 원본 DB 유사도 비교 (ArcFace + pHash)")

    cmp_file = st.file_uploader(
        "비교할 이미지 1장을 업로드하세요",
        type=["jpg", "jpeg", "png", "webp"],
        accept_multiple_files=False,
        key="cmp_uploader",
    )

    min_score = st.slider("표시할 최소 최종 유사도(%)", 0, 100, 40, 5)
    top_n = st.slider("상위 몇 개까지 볼까요?", 1, 20, 5)

    # 가중치 (필요하면 UI로 빼도 됨)
    w_phash = 0.3
    w_embed = 0.7

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
                    pil_cmp = Image.open(BytesIO(data)).convert("RGB")

                    # 1) 업로드 이미지 pHash / 임베딩
                    cmp_phash = calc_phash(pil_cmp)
                    cmp_emb = calc_arcface_embedding(pil_cmp)

                    st.markdown("#### 업로드한 이미지")
                    st.image(pil_cmp, width=260)

                    results = []

                    for _, row in src_df.iterrows():
                        row_phash = row.get("phash")
                        row_emb = str_to_embedding(row.get("face_embed"))

                        phash_sim = phash_similarity(cmp_phash, row_phash)

                        if cmp_emb is not None and row_emb is not None:
                            emb_sim = cosine_similarity(cmp_emb, row_emb)
                            emb_sim_pct = round(max(0.0, emb_sim) * 100, 2)
                        else:
                            emb_sim_pct = 0.0

                        final_score = w_phash * phash_sim + w_embed * emb_sim_pct

                        if final_score >= min_score:
                            results.append(
                                {
                                    "id": row["id"],
                                    "file_name": row["file_name"],
                                    "s3_url": row["s3_url"],
                                    "description": row.get("description"),
                                    "phash_sim": phash_sim,
                                    "embed_sim": emb_sim_pct,
                                    "final_score": round(final_score, 2),
                                }
                            )

                    if not results:
                        st.info(f"최종 유사도 {min_score}% 이상 결과가 없습니다.")
                    else:
                        res_df = (
                            pd.DataFrame(results)
                            .sort_values("final_score", ascending=False)
                            .head(top_n)
                        )

                        st.markdown("#### 유사도 결과 (pHash / ArcFace / 최종)")

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
                                st.write(f"**최종 유사도:** {r['final_score']}%")
                                st.write(
                                    f"- pHash 유사도: {r['phash_sim']}% / "
                                    f"ArcFace 임베딩 유사도: {r['embed_sim']}%"
                                )
                                st.write(f"**파일명:** {r['file_name']}")
                                st.write(f"**S3 경로:** `{r['s3_url']}`")
                                st.write(
                                    f"**설명:** {r['description'] or '설명 없음'}"
                                )
