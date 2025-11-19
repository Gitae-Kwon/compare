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

from insightface.app import FaceAnalysis  # 얼굴 검출 + 임베딩


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
        port=int(mysql_conf.get("port", 3306)),
        user=mysql_conf["user"],
        password=mysql_conf["password"],
        db=mysql_conf["database"],
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )


@st.cache_resource
def get_face_app():
    """InsightFace RetinaFace + ArcFace 초기화 (CPU 사용)"""
    app = FaceAnalysis(name="buffalo_l")
    app.prepare(ctx_id=-1, det_size=(256, 256))  # CPU
    return app


# =========================
# 유틸 함수들
# =========================
def calc_phash(pil_img) -> imagehash.ImageHash:
    """PIL 이미지에서 perceptual hash 계산"""
    return imagehash.phash(pil_img)


def hash_similarity(h1, h2) -> float:
    """두 pHash 사이의 해밍거리 → 유사도(%)"""
    d = h1 - h2  # 0~64
    return max(0.0, round((1 - d / 64) * 100, 4))


def pixel_cosine_similarity(img1: Image.Image, img2: Image.Image, size=(128, 128)) -> float:
    """
    두 이미지를 그레이스케일로 리사이즈 후
    코사인 유사도(0~100%) 계산
    """
    g1 = img1.convert("L").resize(size)
    g2 = img2.convert("L").resize(size)

    v1 = np.asarray(g1, dtype=np.float32).flatten()
    v2 = np.asarray(g2, dtype=np.float32).flatten()

    n1 = np.linalg.norm(v1)
    n2 = np.linalg.norm(v2)
    if n1 == 0 or n2 == 0:
        return 0.0

    cos = float(np.dot(v1, v2) / (n1 * n2))
    cos = max(-1.0, min(1.0, cos))
    return round((cos + 1) / 2 * 100, 4)


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


def load_image_from_s3(key) -> Image.Image:
    """S3 object key로부터 PIL 이미지 로드"""
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    return Image.open(BytesIO(obj["Body"].read())).convert("RGB")


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


def load_all_images() -> pd.DataFrame:
    """image_files 테이블 전체 로드"""
    conn = get_db_conn()
    with conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM image_files ORDER BY id DESC")
            rows = cur.fetchall()
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def save_descriptions(edited_df: pd.DataFrame):
    """data_editor에서 수정된 description을 DB에 반영"""
    conn = get_db_conn()
    with conn:
        with conn.cursor() as cur:
            for _, row in edited_df.iterrows():
                desc = row.get("description")
                if desc == "":
                    desc = None
                cur.execute(
                    "UPDATE image_files SET description=%s WHERE id=%s",
                    (desc, int(row["id"])),
                )
        conn.commit()


# =========================
# 얼굴 기반 센터 크롭 관련
# =========================
def detect_main_face_bbox(pil_img: Image.Image, app: FaceAnalysis, min_size=80):
    """
    PIL 이미지를 받아서 InsightFace로 가장 큰 얼굴의 bbox를 반환.
    bbox 형식: (x1, y1, x2, y2) / 없으면 None
    """
    img = np.array(pil_img.convert("RGB"))[:, :, ::-1]  # RGB -> BGR
    faces = app.get(img)
    if not faces:
        return None

    valid_faces = []
    for f in faces:
        x1, y1, x2, y2 = f.bbox
        w = x2 - x1
        h = y2 - y1
        if w >= min_size and h >= min_size:
            valid_faces.append(f)

    if not valid_faces:
        return None

    # 가장 큰 얼굴 선택
    main = max(
        valid_faces,
        key=lambda f: (f.bbox[2] - f.bbox[0]) * (f.bbox[3] - f.bbox[1]),
    )
    x1, y1, x2, y2 = main.bbox
    return int(x1), int(y1), int(x2), int(y2)


def crop_face_center_or_center(pil_img: Image.Image, app: FaceAnalysis | None = None) -> Image.Image:
    """
    1) 얼굴 탐지가 되면: bbox 기준으로 눈·코·입 주변(머리/장신구 거의 제외)만 크롭
    2) 얼굴 탐지가 안 되면: 이미지 중앙 기준 크롭으로 fallback
    """
    w, h = pil_img.size

    bbox = None
    if app is not None:
        try:
            bbox = detect_main_face_bbox(pil_img, app)
        except Exception:
            bbox = None

    # 얼굴 탐지 실패 → 단순 중앙 크롭
    if bbox is None:
        side = int(min(w, h) * 0.6)
        left = (w - side) // 2
        top = (h - side) // 2
        return pil_img.crop((left, top, left + side, top + side))

    x1, y1, x2, y2 = bbox
    fw, fh = x2 - x1, y2 - y1

    # 얼굴 bbox 안에서 "중앙 부분"만 다시 줄여서 사용
    #   - 좌우 60%만 사용 → 양쪽 머리카락/귀/장신구 잘림
    #   - 세로는 약 50%만 사용, 중심을 약간 아래로 → 눈·코·입 중심
    inner_w = int(fw * 0.6)
    inner_h = int(fh * 0.5)

    cx = x1 + fw // 2
    cy = y1 + int(fh * 0.55)   # 얼굴 전체보다 살짝 아래(코/입 쪽)

    left = max(cx - inner_w // 2, 0)
    top = max(cy - inner_h // 2, 0)
    right = min(left + inner_w, w)
    bottom = min(top + inner_h, h)

    return pil_img.crop((left, top, right, bottom))


def crop_top(pil_img: Image.Image) -> Image.Image:
    """이미지 상단(머리+이마 쪽) 위주 크롭"""
    w, h = pil_img.size
    side = int(min(w, h) * 0.6)
    left = (w - side) // 2
    top = max(int(h * 0.05), 0)
    bottom = min(top + side, h)
    return pil_img.crop((left, top, left + side, bottom))


# =========================
# Streamlit UI
# =========================
st.set_page_config(page_title="이미지 유사도 검사", layout="wide")
st.title("🖼 이미지 유사도 검사 (S3 + MySQL + pHash + 얼굴센터)")

tab1, tab2 = st.tabs(["📥 원본 이미지 등록/관리", "🔍 업로드 이미지 비교"])


# -------------------------
# 탭 1: 원본 이미지 등록/관리
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

                # 전체 phash만 DB에 저장(센터/상단은 비교 시에 계산)
                phash = calc_phash(pil)
                phash_str = str(phash)

                # S3 업로드
                s3_key = upload_to_s3(BytesIO(data), f.name, prefix="source-images")
                s3_url = f"s3://{BUCKET}/{s3_key}"

                # DB 기록
                insert_image_record(
                    f.name,
                    s3_url,
                    phash_str,
                    description=desc_common if desc_common else None,
                )
                count += 1

            st.success(f"✅ 원본 이미지 {count}개 등록 완료!")

    st.markdown("---")
    st.markdown("### DB에 저장된 원본 이미지 목록")

    df = load_all_images()
    if df.empty:
        st.info("아직 저장된 원본 이미지가 없습니다.")
    else:
        # 편집 가능한 테이블 (description만 수정 가능)
        st.write(
            "👉 `description` 컬럼을 표에서 직접 수정한 뒤, 아래 **변경 내용 저장** 버튼을 눌러주세요."
        )
        edited_df = st.data_editor(
            df,
            use_container_width=True,
            num_rows="fixed",
            disabled=["id", "file_name", "s3_url", "phash", "uploaded_at"],
            key="image_table_editor",
        )

        if st.button("💾 변경 내용 저장"):
            save_descriptions(edited_df)
            st.success("설명이 DB에 반영되었습니다. (다시 실행하면 최신 내용으로 보입니다)")

        st.markdown("---")
        st.markdown("### 표지 썸네일 & 미리보기")

        face_app = get_face_app()

        for _, row in df.iterrows():
            col1, col2, col3, col4 = st.columns([0.7, 2.5, 1.0, 1.0])
            with col1:
                st.markdown(f"**ID**: {row['id']}")
            with col2:
                st.markdown(f"**파일명**: {row['file_name']}")
                st.markdown(f"**설명**: {row['description'] or '설명 없음'}")
            with col3:
                # 썸네일 (얼굴 중심 기준 썸네일)
                key = row["s3_url"].split(f"s3://{BUCKET}/", 1)[-1]
                try:
                    img = load_image_from_s3(key)
                    thumb = crop_face_center_or_center(img, face_app)
                    st.image(thumb, width=140)
                except Exception as e:
                    st.write("썸네일 오류")
            with col4:
                if st.button("미리보기", key=f"preview_{row['id']}"):
                    st.session_state["preview_id"] = row["id"]

        # 큰 미리보기
        preview_id = st.session_state.get("preview_id")
        if preview_id:
            st.markdown("---")
            st.markdown("#### 🔍 선택된 이미지 미리보기")

            row = df[df["id"] == preview_id].iloc[0]
            key = row["s3_url"].split(f"s3://{BUCKET}/", 1)[-1]
            img = load_image_from_s3(key)
            st.image(img, caption=f"ID {row['id']} | {row['file_name']}", use_column_width=True)


# -------------------------
# 탭 2: 업로드 이미지 비교
# -------------------------
with tab2:
    st.subheader("🔍 업로드 이미지와 원본 DB 유사도 비교 (pHash 멀티크롭 + 얼굴센터 + 픽셀 코사인)")

    cmp_file = st.file_uploader(
        "비교할 이미지 1장을 업로드하세요",
        type=["jpg", "jpeg", "png", "webp"],
        accept_multiple_files=False,
        key="cmp_uploader_tab2",
    )

    min_score = st.slider("표시할 최소 최종 유사도(%)", 0, 100, 40, 5)
    top_n = st.slider("상위 몇 개까지 볼까요?", 1, 20, 5)

    st.markdown("### 🛠 가중치 설정")
    w_full = st.slider("전체 pHash 비중", 0.0, 1.0, 0.10, 0.05)
    w_center = st.slider("얼굴센터 pHash 비중", 0.0, 1.0, 0.30, 0.05)
    w_top = st.slider("상단 pHash 비중", 0.0, 1.0, 0.20, 0.05)
    w_pixel = st.slider("픽셀 코사인(얼굴센터) 비중", 0.0, 1.0, 0.40, 0.05)

    total_w = w_full + w_center + w_top + w_pixel
    if total_w == 0:
        w_full = w_center = w_top = w_pixel = 0.25
    else:
        w_full /= total_w
        w_center /= total_w
        w_top /= total_w
        w_pixel /= total_w

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
                    face_app = get_face_app()

                    # 업로드 이미지 준비
                    pil_cmp = Image.open(BytesIO(data)).convert("RGB")
                    cmp_full = pil_cmp
                    cmp_center = crop_face_center_or_center(pil_cmp, face_app)
                    cmp_top = crop_top(pil_cmp)

                    cmp_full_hash = calc_phash(cmp_full)
                    cmp_center_hash = calc_phash(cmp_center)
                    cmp_top_hash = calc_phash(cmp_top)

                    st.markdown("#### 업로드한 이미지")
                    st.image(pil_cmp, width=260)

                    results = []
                    for _, row in src_df.iterrows():
                        key = row["s3_url"].split(f"s3://{BUCKET}/", 1)[-1]
                        try:
                            src_img = load_image_from_s3(key)
                        except Exception:
                            continue

                        src_full = src_img
                        src_center = crop_face_center_or_center(src_img, face_app)
                        src_top = crop_top(src_img)

                        # full pHash는 DB에 저장된 값 사용
                        try:
                            src_full_hash = imagehash.hex_to_hash(row["phash"])
                        except Exception:
                            src_full_hash = calc_phash(src_full)

                        src_center_hash = calc_phash(src_center)
                        src_top_hash = calc_phash(src_top)

                        full_sim = hash_similarity(cmp_full_hash, src_full_hash)
                        center_sim = hash_similarity(cmp_center_hash, src_center_hash)
                        top_sim = hash_similarity(cmp_top_hash, src_top_hash)
                        pixel_sim = pixel_cosine_similarity(cmp_center, src_center)

                        final_score = (
                            w_full * full_sim
                            + w_center * center_sim
                            + w_top * top_sim
                            + w_pixel * pixel_sim
                        )

                        if final_score >= min_score:
                            results.append(
                                {
                                    "id": row["id"],
                                    "file_name": row["file_name"],
                                    "s3_url": row["s3_url"],
                                    "description": row.get("description"),
                                    "full_sim": round(full_sim, 2),
                                    "center_sim": round(center_sim, 2),
                                    "top_sim": round(top_sim, 2),
                                    "pixel_sim": round(pixel_sim, 2),
                                    "final_score": round(final_score, 2),
                                }
                            )

                    if not results:
                        st.info(f"유사도 {min_score}% 이상 결과가 없습니다.")
                    else:
                        res_df = (
                            pd.DataFrame(results)
                            .sort_values("final_score", ascending=False)
                            .head(top_n)
                        )

                        st.markdown("### 유사도 결과 (pHash 전체/얼굴센터/상단 + 픽셀 코사인 + 최종)")
                        for _, r in res_df.iterrows():
                            col1, col2 = st.columns([1.2, 2.0])
                            with col1:
                                key = r["s3_url"].split(f"s3://{BUCKET}/", 1)[-1]
                                img = load_image_from_s3(key)
                                thumb = crop_face_center_or_center(img, face_app)
                                st.image(
                                    thumb,
                                    caption=f"ID {r['id']} | {r['file_name']}",
                                    use_column_width=True,
                                )
                            with col2:
                                st.write(f"**최종 유사도:** {r['final_score']}%")
                                st.write(
                                    f"• pHash 전체: {r['full_sim']}% / "
                                    f"얼굴센터: {r['center_sim']}% / 상단: {r['top_sim']}%"
                                )
                                st.write(f"• 픽셀 코사인(얼굴센터, Gray): {r['pixel_sim']}%")
                                st.write(f"**설명:** {r['description'] or '설명 없음'}")
                                st.write(f"**S3 경로:** `{r['s3_url']}`")
                                st.markdown("---")
