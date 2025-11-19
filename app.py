import os
import json
from io import BytesIO
from datetime import datetime

import streamlit as st
from PIL import Image
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
    """
    로컬 CPU 환경용 InsightFace 초기화
    (face-compare.py에서 사용한 것과 동일한 buffalo_l 모델)
    """
    app = FaceAnalysis(name="buffalo_l")
    app.prepare(ctx_id=-1, det_size=(256, 256))  # CPU 사용
    return app


face_app = get_face_app()


# =========================
# 유틸 함수들 (이미지/임베딩/DB)
# =========================
def pil_to_cv2(img: Image.Image):
    """PIL 이미지를 OpenCV BGR 배열로 변환"""
    rgb = np.array(img.convert("RGB"))
    bgr = cv2.cvtColor(rgb, cv2.COLOR_RGB2BGR)
    return bgr


def get_face_embedding_from_pil(img: Image.Image):
    """
    PIL 이미지를 받아 InsightFace 얼굴 임베딩(512-dim) 반환.
    얼굴이 없으면 None.
    여러 얼굴이면 가장 큰 얼굴 기준.
    """
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

    # face.embedding 속성 사용 (로컬 테스트에서 확인했던 방식)
    emb = best_face.embedding.astype("float32")

    # 코사인 유사도 계산을 위한 L2 정규화
    norm = np.linalg.norm(emb)
    if norm > 0:
        emb = emb / norm
    return emb


def cosine_sim(v1, v2):
    """두 벡터의 코사인 유사도 (0~100%)"""
    v1 = np.asarray(v1, dtype="float32")
    v2 = np.asarray(v2, dtype="float32")
    denom = (np.linalg.norm(v1) * np.linalg.norm(v2))
    if denom == 0:
        return 0.0
    s = float(np.dot(v1, v2) / denom)  # -1 ~ 1
    # 0~100 스케일로 변환
    return round((s + 1) / 2 * 100, 2)


def upload_to_s3(file_like, original_name, prefix="images"):
    """업로드 파일을 S3에 저장하고 key 반환"""
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


def insert_image_record(file_name, s3_url, description=None, face_emb=None):
    """
    image_files 테이블에 한 줄 삽입
    - face_embedding: numpy array 또는 None → JSON 문자열로 저장
    """
    conn = get_db_conn()
    with conn:
        with conn.cursor() as cur:
            sql = """
                INSERT INTO image_files
                (file_name, s3_url, description, uploaded_at, face_embedding)
                VALUES (%s, %s, %s, NOW(), %s)
            """
            face_json = json.dumps(face_emb.tolist()) if face_emb is not None else None
            cur.execute(
                sql,
                (file_name, s3_url, description, face_json),
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
    """data_editor에서 수정된 description만 DB에 반영"""
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
st.set_page_config(page_title="이미지 유사도 검사 (InsightFace)", layout="wide")
st.title("🖼 이미지 유사도 검사 (S3 + MySQL + InsightFace 얼굴 임베딩)")

tab1, tab2 = st.tabs(["📥 원본 이미지 등록", "🔍 업로드 이미지 비교"])


# -------------------------
# 탭 1: 원본 이미지 등록
# -------------------------
with tab1:
    st.subheader("📥 원본(레퍼런스) 이미지 등록 (얼굴 임베딩 저장)")

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
            no_face_count = 0
            for f in src_files:
                data = f.read()
                if not data:
                    continue

                pil_img = Image.open(BytesIO(data)).convert("RGB")

                # 1) 얼굴 임베딩
                face_emb = get_face_embedding_from_pil(pil_img)
                if face_emb is None:
                    no_face_count += 1

                # 2) S3 업로드
                s3_key = upload_to_s3(BytesIO(data), f.name, prefix="source-images")
                s3_url = f"s3://{BUCKET}/{s3_key}"

                # 3) DB insert
                insert_image_record(
                    f.name,
                    s3_url,
                    description=desc_common if desc_common else None,
                    face_emb=face_emb,
                )
                count += 1

            msg = f"✅ 원본 이미지 {count}개 등록 완료!"
            if no_face_count > 0:
                msg += f" (이 중 {no_face_count}개는 얼굴을 찾지 못해 임베딩이 저장되지 않았습니다.)"
            st.success(msg)

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
                        "description",
                        "uploaded_at",
                        "face_embedding",
                    ]
                ],
                use_container_width=True,
                num_rows="fixed",
                disabled=["id", "file_name", "s3_url", "uploaded_at", "face_embedding"],
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
    st.subheader("🔍 업로드 이미지와 원본 DB 얼굴 임베딩 유사도 비교")

    cmp_file = st.file_uploader(
        "비교할 이미지 1장을 업로드하세요 (얼굴이 포함된 이미지 권장)",
        type=["jpg", "jpeg", "png", "webp"],
        accept_multiple_files=False,
        key="cmp_uploader_face",
    )

    min_score = st.slider("표시할 최소 유사도(%)", 0, 100, 40, 5)
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
                    query_img = Image.open(BytesIO(data)).convert("RGB")

                    col_u1, _ = st.columns([1, 2])
                    with col_u1:
                        st.markdown("#### 업로드한 이미지")
                        st.image(query_img, width=250)

                    # 업로드 이미지 얼굴 임베딩
                    q_face_emb = get_face_embedding_from_pil(query_img)
                    if q_face_emb is None:
                        st.error("업로드한 이미지에서 얼굴을 찾지 못했습니다. 얼굴이 잘 보이는 이미지를 사용해 주세요.")
                    else:
                        results = []

                        for _, row in src_df.iterrows():
                            # DB에 face_embedding 이 없는 경우는 스킵
                            if not row.get("face_embedding"):
                                continue

                            try:
                                db_emb_list = json.loads(row["face_embedding"])
                                if not isinstance(db_emb_list, list) or len(db_emb_list) == 0:
                                    continue
                                db_emb = np.array(db_emb_list, dtype="float32")
                            except Exception:
                                continue

                            sim = cosine_sim(q_face_emb, db_emb)

                            if sim >= min_score:
                                results.append(
                                    {
                                        "id": row["id"],
                                        "file_name": row["file_name"],
                                        "s3_url": row["s3_url"],
                                        "description": row.get("description"),
                                        "similarity": sim,
                                    }
                                )

                        if not results:
                            st.info(f"유사도 {min_score}% 이상 결과가 없습니다.")
                        else:
                            res_df = (
                                pd.DataFrame(results)
                                .sort_values("similarity", ascending=False)
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
                                    st.write(f"**유사도:** {r['similarity']}%")
                                    st.write(f"**파일명:** {r['file_name']}")
                                    st.write(f"**S3 경로:** `{r['s3_url']}`")
                                    st.write(f"**설명:** {r['description'] or '설명 없음'}")
