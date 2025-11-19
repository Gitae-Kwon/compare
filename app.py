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
import mediapipe as mp


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
def get_face_mesh():
    """Mediapipe FaceMesh 초기화 (CPU, 정지 이미지용)"""
    mp_face_mesh = mp.solutions.face_mesh
    face_mesh = mp_face_mesh.FaceMesh(
        static_image_mode=True,
        max_num_faces=1,
        refine_landmarks=False,
        min_detection_confidence=0.5,
        min_tracking_confidence=0.5,
    )
    return face_mesh


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
# Mediapipe 얼굴 랜드마크 기반 crop
# =========================
def get_face_bbox_from_landmarks(pil_img: Image.Image, face_mesh, min_size=60):
    """
    Mediapipe FaceMesh로 얼굴 랜드마크 탐지 후
    가장 큰 얼굴의 bbox 반환 (x1, y1, x2, y2)
    """
    img = np.array(pil_img.convert("RGB"))
    h, w, _ = img.shape

    results = face_mesh.process(img)
    if not results.multi_face_landmarks:
        return None

    lm = results.multi_face_landmarks[0]
    xs, ys = [], []
    for pt in lm.landmark:
        xs.append(pt.x * w)
        ys.append(pt.y * h)

    x1, x2 = min(xs), max(xs)
    y1, y2 = min(ys), max(ys)

    fw = x2 - x1
    fh = y2 - y1
    if fw < min_size or fh < min_size:
        return None

    return int(x1), int(y1), int(x2), int(y2)


def crop_face_center_or_center(pil_img: Image.Image, face_mesh) -> Image.Image:
    """
    1) 얼굴 랜드마크가 잡히면: 얼굴 내부(눈·코·입 중심)만 crop
    2) 실패하면: 이미지 중앙 기준 crop
    """
    w, h = pil_img.size
    bbox = None
    try:
        bbox = get_face_bbox_from_landmarks(pil_img, face_mesh)
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

    # 얼굴 bbox 안에서 더 줄여서, 머리카락/장신구는 최대한 제외
    inner_w = int(fw * 0.65)
    inner_h = int(fh * 0.55)

    cx = x1 + fw // 2
    cy = y1 + int(fh * 0.55)  # 약간 아래쪽(코/입 중심)

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


def safe_phash_score(s: float, cutoff: float = 30.0) -> float:
    """
    pHash 유사도가 cutoff 아래면 기여하지 않도록 0 처리.
    (너무 낮은 건 '다름'이라고 보고 벌점 대신 무시)
    """
    return s if s >= cutoff else 0.0


# =========================
# Streamlit UI
# =========================
st.set_page_config(page_title="이미지 유사도 검사", layout="wide")
st.title("🖼 이미지 유사도 검사 (S3 + MySQL + 얼굴 랜드마크 기반 pHash)")


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

        face_mesh = get_face_mesh()

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
                    thumb = crop_face_center_or_center(img, face_mesh)
                    st.image(thumb, width=140)
                except Exception:
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
    st.subheader("🔍 업로드 이미지와 원본 DB 유사도 비교 (얼굴 랜드마크 기반 pHash + 픽셀 코사인)")

    cmp_file = st.file_uploader(
        "비교할 이미지 1장을 업로드하세요",
        type=["jpg", "jpeg", "png", "webp"],
        accept_multiple_files=False,
        key="cmp_uploader_tab2",
    )

    min_score = st.slider("표시할 최소 최종 유사도(%)", 0, 100, 40, 5)
    top_n = st.slider("상위 몇 개까지 볼까요?", 1, 20, 5)

    st.markdown("### 🛠 가중치 설정")
    w_full = st.slider("전체 pHash 비중", 0.0, 1.0, 0.05, 0.05)
    w_center = st.slider("얼굴센터 pHash 비중", 0.0, 1.0, 0.35, 0.05)
    w_top = st.slider("상단 pHash 비중", 0.0, 1.0, 0.10, 0.05)
    w_pixel = st.slider("픽셀 코사인(얼굴센터) 비중", 0.0, 1.0, 0.50, 0.05)

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
                    face_mesh = get_face_mesh()

                    # 업로드 이미지 준비
                    pil_cmp = Image.open(BytesIO(data)).convert("RGB")
                    cmp_full = pil_cmp
                    cmp_center = crop_face_center_or_center(pil_cmp, face_mesh)
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
                        src_center = crop_face_center_or_center(src_img, face_mesh)
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

                        # pHash 컷오프로 너무 낮은 값은 기여하지 않도록
                        full_sim_safe = safe_phash_score(full_sim, cutoff=30.0)
                        center_sim_safe = safe_phash_score(center_sim, cutoff=35.0)
                        top_sim_safe = safe_phash_score(top_sim, cutoff=35.0)

                        # 얼굴 구조가 너무 다르면(센터 pHash+픽셀 둘 다 낮으면) 과감히 버리기
                        if center_sim < 30 and pixel_sim < 65:
                            final_score = 0.0
                        else:
                            final_score = (
                                w_full * full_sim_safe
                                + w_center * center_sim_safe
                                + w_top * top_sim_safe
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

                        st.markdown("### 유사도 결과 (얼굴 랜드마크 기반 pHash + 픽셀 코사인 + 최종)")
                        for _, r in res_df.iterrows():
                            col1, col2 = st.columns([1.2, 2.0])
                            with col1:
                                key = r["s3_url"].split(f"s3://{BUCKET}/", 1)[-1]
                                img = load_image_from_s3(key)
                                thumb = crop_face_center_or_center(img, face_mesh)
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
