import os
import json
from io import BytesIO
from datetime import datetime

import streamlit as st
from PIL import Image
import imagehash
import numpy as np
import pandas as pd
import boto3
from botocore.exceptions import ClientError
import pymysql
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
# InsightFace 초기화
# =========================
@st.cache_resource
def get_face_app():
    """CPU용 InsightFace 초기화"""
    app = FaceAnalysis(name="buffalo_l")
    app.prepare(ctx_id=-1, det_size=(256, 256))
    return app


face_app = get_face_app()


# =========================
# 유틸 함수 (이미지/해시/임베딩)
# =========================
def pil_to_cv2(img: Image.Image):
    rgb = np.array(img.convert("RGB"))
    bgr = cv2.cvtColor(rgb, cv2.COLOR_RGB2BGR)
    return bgr


def get_face_embedding_from_pil(img: Image.Image):
    """얼굴 임베딩 512-dim 반환, 실패시 None"""
    bgr = pil_to_cv2(img)
    faces = face_app.get(bgr)
    if not faces:
        return None

    # 가장 큰 얼굴 선택
    areas = []
    for f in faces:
        x1, y1, x2, y2 = f.bbox.astype(int)
        areas.append((x2 - x1) * (y2 - y1))
    best_idx = int(np.argmax(areas))
    best_face = faces[best_idx]

    emb = best_face["embedding"].astype("float32")
    norm = np.linalg.norm(emb)
    if norm > 0:
        emb = emb / norm
    return emb


def cosine_sim(v1, v2):
    v1 = np.asarray(v1, dtype="float32")
    v2 = np.asarray(v2, dtype="float32")
    denom = (np.linalg.norm(v1) * np.linalg.norm(v2))
    if denom == 0:
        return 0.0
    return float(np.dot(v1, v2) / denom)


def crop_center(img: Image.Image, scale_w=0.8, scale_h=0.8):
    w, h = img.size
    cw = int(w * scale_w)
    ch = int(h * scale_h)
    left = (w - cw) // 2
    top = (h - ch) // 2
    return img.crop((left, top, left + cw, top + ch))


def crop_top_face_region(img: Image.Image, scale_h=0.6):
    """윗부분(머리+이마+눈 위주)"""
    w, h = img.size
    th = int(h * scale_h)
    left = int(w * 0.1)
    right = int(w * 0.9)
    return img.crop((left, 0, right, th))


def calc_multi_phash(img: Image.Image):
    """full / center / top pHash 계산"""
    full = imagehash.phash(img)
    center = imagehash.phash(crop_center(img, 0.7, 0.7))
    top = imagehash.phash(crop_top_face_region(img, 0.55))
    return {
        "full": str(full),
        "center": str(center),
        "top": str(top),
    }


def phash_similarity(h1_str, h2_str):
    """pHash 문자열 유사도 (0~100%)"""
    if not h1_str or not h2_str:
        return 0.0
    h1 = imagehash.hex_to_hash(h1_str)
    h2 = imagehash.hex_to_hash(h2_str)
    d = h1 - h2
    return round((1 - d / 64) * 100, 2)


def pixel_cosine_similarity(img1: Image.Image, img2: Image.Image):
    """중앙영역 gray 64x64 코사인유사도 (0~100%)"""
    c1 = crop_center(img1, 0.7, 0.7).convert("L").resize((64, 64))
    c2 = crop_center(img2, 0.7, 0.7).convert("L").resize((64, 64))
    v1 = np.asarray(c1).flatten().astype("float32")
    v2 = np.asarray(c2).flatten().astype("float32")
    s = cosine_sim(v1, v2)
    return round(s * 100, 2)


# =========================
# DB 관련 함수
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
            f"S3 업로드 실패: 코드={err.get('Code')}, "
            f"메시지={err.get('Message')}"
        )
        raise
    return key


def insert_image_record(file_name, s3_url, phash_str, phash_json_str,
                        description=None, face_emb=None):
    """image_files 한 줄 삽입"""
    conn = get_db_conn()
    with conn:
        with conn.cursor() as cur:
            sql = """
                INSERT INTO image_files
                (file_name, s3_url, phash, description, uploaded_at, phash_json, face_embedding)
                VALUES (%s, %s, %s, %s, NOW(), %s, %s)
            """
            face_json = json.dumps(face_emb.tolist()) if face_emb is not None else None
            cur.execute(
                sql,
                (file_name, s3_url, phash_str, description, phash_json_str, face_json),
            )
        conn.commit()


def load_all_images():
    conn = get_db_conn()
    with conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM image_files ORDER BY id DESC")
            rows = cur.fetchall()
    return pd.DataFrame(rows)


def update_descriptions(df_before: pd.DataFrame, df_after: pd.DataFrame):
    """description 변경분만 UPDATE"""
    changed = df_before[["id", "description"]].merge(
        df_after[["id", "description"]],
        on="id",
        how="inner",
        suffixes=("_before", "_after"),
    )
    changed = changed[changed["description_before"] != changed["description_after"]]

    if changed.empty:
        st.info("변경된 description 이 없습니다.")
        return

    conn = get_db_conn()
    with conn:
        with conn.cursor() as cur:
            for _, row in changed.iterrows():
                cur.execute(
                    "UPDATE image_files SET description = %s WHERE id = %s",
                    (row["description_after"], row["id"]),
                )
        conn.commit()
    st.success(f"✅ {len(changed)}건의 description 업데이트 완료")


# =========================
# Streamlit UI
# =========================
st.set_page_config(page_title="이미지 유사도 검사", layout="wide")
st.title("🖼 이미지 유사도 검사 (S3 + MySQL + pHash + FaceEmbedding)")

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

                pil_img = Image.open(BytesIO(data)).convert("RGB")

                # 1) pHash (full/center/top)
                phash_dict = calc_multi_phash(pil_img)
                phash_str = phash_dict["full"]
                phash_json_str = json.dumps(phash_dict)

                # 2) 얼굴 임베딩
                face_emb = get_face_embedding_from_pil(pil_img)

                # 3) S3 업로드
                s3_key = upload_to_s3(BytesIO(data), f.name, prefix="source-images")
                s3_url = f"s3://{BUCKET}/{s3_key}"

                # 4) DB insert
                insert_image_record(
                    f.name,
                    s3_url,
                    phash_str,
                    phash_json_str,
                    description=desc_common if desc_common else None,
                    face_emb=face_emb,
                )
                count += 1

            st.success(f"✅ 원본 이미지 {count}개 등록 완료!")

    st.markdown("### DB에 저장된 원본 이미지 목록 (description 수정 가능)")
    try:
        df = load_all_images()
        if df.empty:
            st.info("아직 저장된 원본 이미지가 없습니다.")
        else:
            before_df = df.copy()

            edited_df = st.data_editor(
                df[
                    [
                        "id",
                        "file_name",
                        "s3_url",
                        "phash",
                        "description",
                        "uploaded_at",
                        "phash_json",
                        "face_embedding",
                    ]
                ],
                use_container_width=True,
                num_rows="fixed",
                disabled=["id", "file_name", "s3_url", "phash",
                          "uploaded_at", "phash_json", "face_embedding"],
                key="image_table_editor",
            )

            if st.button("📝 description 변경 내용 저장"):
                update_descriptions(before_df, edited_df)

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
        key="cmp_uploader_face",
    )

    min_score = st.slider("표시할 최소 최종 유사도(%)", 0, 100, 40, 5)
    top_n = st.slider("상위 몇 개까지 볼까요?", 1, 20, 5)

    st.markdown("#### ⚙️ 가중치 설정")
    w_phash = st.slider("pHash(전체/센터/상단 평균) 비중", 0.0, 1.0, 0.4, 0.05)
    w_pixel = st.slider("픽셀 코사인(중앙) 비중", 0.0, 1.0, 0.2, 0.05)
    w_face = st.slider("얼굴 임베딩 코사인 비중", 0.0, 1.0, 0.4, 0.05)

    total_w = w_phash + w_pixel + w_face
    if total_w == 0:
        w_phash = w_pixel = w_face = 1 / 3
    else:
        w_phash /= total_w
        w_pixel /= total_w
        w_face /= total_w

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
                    query_img = Image.open(BytesIO(data)).convert("RGB")

                    col_u1, _ = st.columns([1, 2])
                    with col_u1:
                        st.markdown("#### 업로드한 이미지")
                        st.image(query_img, width=250)

                    # 쿼리 이미지 특징
                    q_phash_dict = calc_multi_phash(query_img)
                    query_pix_base = query_img.copy()
                    q_face_emb = get_face_embedding_from_pil(query_img)

                    results = []

                    for _, row in src_df.iterrows():
                        # S3에서 이미지 불러오기
                        try:
                            key = row["s3_url"].split(f"s3://{BUCKET}/", 1)[-1]
                            obj = s3.get_object(Bucket=BUCKET, Key=key)
                            db_img = Image.open(BytesIO(obj["Body"].read())).convert("RGB")
                        except Exception:
                            continue

                        # DB의 phash_json
                        if row.get("phash_json"):
                            try:
                                db_phash_dict = json.loads(row["phash_json"])
                            except Exception:
                                db_phash_dict = {"full": row.get("phash")}
                        else:
                            db_phash_dict = {"full": row.get("phash")}

                        full_sim = phash_similarity(
                            q_phash_dict.get("full"),
                            db_phash_dict.get("full"),
                        )
                        center_sim = phash_similarity(
                            q_phash_dict.get("center"),
                            db_phash_dict.get("center"),
                        )
                        top_sim = phash_similarity(
                            q_phash_dict.get("top"),
                            db_phash_dict.get("top"),
                        )

                        valid_vals = [v for v in [full_sim, center_sim, top_sim] if v is not None]
                        phash_mean = sum(valid_vals) / len(valid_vals) if valid_vals else 0.0

                        pixel_sim = pixel_cosine_similarity(query_pix_base, db_img)

                        face_sim = 0.0
                        if q_face_emb is not None and row.get("face_embedding"):
                            try:
                                db_emb_list = json.loads(row["face_embedding"])
                                if isinstance(db_emb_list, list) and len(db_emb_list) > 0:
                                    face_sim = cosine_sim(q_face_emb, db_emb_list) * 100.0
                            except Exception:
                                face_sim = 0.0

                        final_score = (
                            w_phash * phash_mean
                            + w_pixel * pixel_sim
                            + w_face * face_sim
                        )

                        if final_score >= min_score:
                            results.append(
                                {
                                    "id": row["id"],
                                    "file_name": row["file_name"],
                                    "s3_url": row["s3_url"],
                                    "description": row.get("description"),
                                    "phash_full": round(full_sim, 2),
                                    "phash_center": round(center_sim, 2),
                                    "phash_top": round(top_sim, 2),
                                    "phash_mean": round(phash_mean, 2),
                                    "pixel_sim": round(pixel_sim, 2),
                                    "face_sim": round(face_sim, 2),
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

                        st.markdown(f"#### 유사도 결과 (상위 {len(res_df)}개)")
                        for _, r in res_df.iterrows():
                            col1, col2 = st.columns([1, 2])
                            with col1:
                                key = r["s3_url"].split(f"s3://{BUCKET}/", 1)[-1]
                                obj = s3.get_object(Bucket=BUCKET, Key=key)
                                img = Image.open(BytesIO(obj["Body"].read())).convert("RGB")
                                st.image(
                                    img,
                                    caption=f"ID {r['id']} | {r['file_name']}",
                                )
                            with col2:
                                st.write(f"**최종 유사도:** {r['final_score']}%")
                                st.write(
                                    f"- pHash 평균: {r['phash_mean']}% "
                                    f"(full: {r['phash_full']} / center: {r['phash_center']} / top: {r['phash_top']})"
                                )
                                st.write(f"- 픽셀 코사인(중앙): {r['pixel_sim']}%")
                                st.write(f"- 얼굴 임베딩 코사인: {r['face_sim']}%")
                                st.write(f"**파일명:** {r['file_name']}")
                                st.write(f"**S3 경로:** `{r['s3_url']}`")
                                st.write(f"**설명:** {r['description'] or '설명 없음'}")
