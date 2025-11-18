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
# 이미지 유틸 (해시 / 크롭 / 픽셀 유사도)
# =========================
def calc_phash(pil_img: Image.Image) -> str:
    """이미지 pHash -> hex 문자열"""
    return str(imagehash.phash(pil_img.convert("RGB")))


def phash_similarity(h1: str | None, h2: str | None) -> float:
    """pHash 해밍 거리 기반 유사도 (0~100%)"""
    if not h1 or not h2:
        return 0.0
    a = imagehash.hex_to_hash(h1)
    b = imagehash.hex_to_hash(h2)
    d = a - b  # hamming distance (0~64)
    sim = (1 - d / 64) * 100
    return float(round(sim, 2))


def crop_center(pil_img: Image.Image, scale: float = 0.6) -> Image.Image:
    """
    가운데 부분만 크롭.
    scale=0.6이면 가로/세로의 60% 영역.
    """
    w, h = pil_img.size
    new_w = int(w * scale)
    new_h = int(h * scale)
    left = (w - new_w) // 2
    top = (h - new_h) // 2
    right = left + new_w
    bottom = top + new_h
    return pil_img.crop((left, top, right, bottom))


def crop_top(pil_img: Image.Image, height_ratio: float = 0.5) -> Image.Image:
    """
    위쪽 일부만 크롭 (얼굴이 위쪽에 많이 있을 때).
    height_ratio=0.5이면 상단 50%만.
    """
    w, h = pil_img.size
    new_h = int(h * height_ratio)
    return pil_img.crop((0, 0, w, new_h))


def pixel_cosine_similarity(img1: Image.Image, img2: Image.Image, size: int = 64) -> float:
    """
    중앙 크롭 기준 픽셀 코사인 유사도 (0~100%)
    - 배경 영향 줄이기 위해 중앙 크롭 후 비교 권장
    """
    # 동일 크기로 리사이즈 & 그레이스케일
    i1 = img1.convert("L").resize((size, size))
    i2 = img2.convert("L").resize((size, size))

    v1 = np.asarray(i1, dtype=np.float32).flatten()
    v2 = np.asarray(i2, dtype=np.float32).flatten()

    num = float(np.dot(v1, v2))
    den = float(np.linalg.norm(v1) * np.linalg.norm(v2) + 1e-8)
    cos = num / den  # -1~1
    cos = max(0.0, cos)  # 음수는 0으로 처리
    return float(round(cos * 100, 2))


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


def load_image_from_s3(key: str) -> Image.Image:
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    return Image.open(BytesIO(obj["Body"].read())).convert("RGB")


# =========================
# DB 유틸
# =========================
def insert_image_record(
    file_name: str,
    s3_url: str,
    phash_str: str | None,
    description: str | None = None,
):
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
    conn = get_db_conn()
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, file_name, s3_url, phash, description, uploaded_at "
                "FROM image_files ORDER BY id DESC"
            )
            rows = cur.fetchall()
    return pd.DataFrame(rows)


def update_descriptions_from_df(df: pd.DataFrame):
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
st.title("🖼 이미지 유사도 검사 (S3 + MySQL + pHash + 픽셀코사인)")

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

                # 전체 이미지 pHash
                phash_str = calc_phash(pil)

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
            disabled=["id", "file_name", "s3_url", "phash", "uploaded_at"],
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
    st.subheader("🔍 업로드 이미지와 원본 DB 유사도 비교 (pHash 멀티크롭 + 픽셀 코사인)")

    # 1) 비교할 이미지 업로드
    cmp_file = st.file_uploader(
        "비교할 이미지 1장을 업로드하세요",
        type=["jpg", "jpeg", "png", "webp"],
        accept_multiple_files=False,
        key="cmp_uploader",
    )

    # 2) 필터 및 상위 개수 설정
    min_score = st.slider("표시할 최소 최종 유사도(%)", 0, 100, 40, 5)
    top_n = st.slider("상위 몇 개까지 볼까요?", 1, 20, 5)

    # 3) 가중치 설정 슬라이더
    st.markdown("#### 🔧 가중치 설정")
    w_full   = st.slider("전체 pHash 비중", 0.0, 1.0, 0.10, 0.05)
    w_center = st.slider("센터 pHash 비중", 0.0, 1.0, 0.20, 0.05)
    w_top    = st.slider("상단 pHash 비중", 0.0, 1.0, 0.20, 0.05)
    w_pixel  = st.slider("픽셀 코사인 비중", 0.0, 1.0, 0.50, 0.05)

    # 합이 1.0이 되도록 정규화
    total_w = w_full + w_center + w_top + w_pixel
    if total_w == 0:
        w_full = w_center = w_top = w_pixel = 0.25
    else:
        w_full   /= total_w
        w_center /= total_w
        w_top    /= total_w
        w_pixel  /= total_w

    # 4) 유사도 분석 버튼
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
                    # 업로드 이미지 로드
                    pil_cmp = Image.open(BytesIO(data)).convert("RGB")

                    # 업로드 이미지 여러 크롭
                    cmp_full = pil_cmp
                    cmp_center = crop_center(pil_cmp)
                    cmp_top = crop_top(pil_cmp)

                    # 업로드 이미지 pHash
                    cmp_full_ph = calc_phash(cmp_full)
                    cmp_center_ph = calc_phash(cmp_center)
                    cmp_top_ph = calc_phash(cmp_top)

                    st.markdown("#### 업로드한 이미지")
                    st.image(pil_cmp, width=260)

                    results = []

                    for _, row in src_df.iterrows():
                        # S3에서 원본 이미지 로드
                        key = row["s3_url"].split(f"s3://{BUCKET}/", 1)[-1]
                        try:
                            src_img = load_image_from_s3(key)
                        except Exception:
                            continue

                        src_full = src_img
                        src_center = crop_center(src_img)
                        src_top = crop_top(src_img)

                        # pHash 유사도들
                        full_sim = phash_similarity(cmp_full_ph, row.get("phash"))
                        center_sim = phash_similarity(
                            cmp_center_ph, calc_phash(src_center)
                        )
                        top_sim = phash_similarity(
                            cmp_top_ph, calc_phash(src_top)
                        )

                        # 픽셀 코사인 (중앙 영역 기준)
                        pixel_sim = pixel_cosine_similarity(cmp_center, src_center)

                        # 최종 유사도
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
                        st.info(f"최종 유사도 {min_score}% 이상 결과가 없습니다.")
                    else:
                        res_df = (
                            pd.DataFrame(results)
                            .sort_values("final_score", ascending=False)
                            .head(top_n)
                        )

                        st.markdown("#### 유사도 결과 (pHash 전체/센터/상단 + 픽셀코사인 + 최종)")

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
                                    f"- pHash 전체: {r['full_sim']}% / "
                                    f"센터: {r['center_sim']}% / "
                                    f"상단: {r['top_sim']}%"
                                )
                                st.write(f"- 픽셀 코사인(센터): {r['pixel_sim']}%")
                                st.write(f"**파일명:** {r['file_name']}")
                                st.write(f"**S3 경로:** `{r['s3_url']}`")
                                st.write(
                                    f"**설명:** {r['description'] or '설명 없음'}"
                                )
