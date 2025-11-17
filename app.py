import os
import json
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
# 이미지 해시 / 크롭 / 벡터 유틸
# =========================
def center_crop(img: Image.Image, ratio: float = 0.6) -> Image.Image:
    """
    이미지 중앙 기준으로 ratio 비율만큼 크롭 (좌우/상하 모두 중앙)
    """
    w, h = img.size
    cw, ch = int(w * ratio), int(h * ratio)
    left = (w - cw) // 2
    top = (h - ch) // 2
    return img.crop((left, top, left + cw, top + ch))


def top_center_crop(img: Image.Image, width_ratio: float = 0.6, height_ratio: float = 0.6) -> Image.Image:
    """
    얼굴이 위쪽에 있을 가능성을 고려한 상단 중심 크롭
    - 좌우는 중앙 width_ratio 비율
    - 상단 height_ratio 비율만 사용
    """
    w, h = img.size
    tw = int(w * width_ratio)
    th = int(h * height_ratio)
    left = (w - tw) // 2       # 좌우 중앙 정렬
    top = 0                    # 맨 위부터
    return img.crop((left, top, left + tw, top + th))


def calc_single_phash(img: Image.Image) -> imagehash.ImageHash:
    """
    단일 이미지 pHash
    """
    return imagehash.phash(img.convert("RGB"))


def calc_multi_phash_objects(img: Image.Image) -> dict:
    """
    하나의 PIL 이미지에 대해
    - full
    - center
    - top
    3가지 pHash를 ImageHash 객체로 반환
    """
    img = img.convert("RGB")

    full = calc_single_phash(img)
    center = calc_single_phash(center_crop(img, ratio=0.6))
    top = calc_single_phash(top_center_crop(img, width_ratio=0.6, height_ratio=0.6))

    return {
        "full": full,
        "center": center,
        "top": top,
    }


def calc_multi_phash_str(img: Image.Image) -> dict:
    """
    DB에 저장하기 위한 문자열 버전 해시(dict of str) 반환
    """
    hobj = calc_multi_phash_objects(img)
    return {k: str(v) for k, v in hobj.items()}


def similarity(h1: imagehash.ImageHash, h2: imagehash.ImageHash) -> float:
    """
    두 pHash 간 해밍거리로 유사도(%) 계산
    - hamming distance: 0 ~ 64
    - 유사도 = (1 - d/64) * 100
    """
    d = h1 - h2
    return round((1 - d / 64) * 100, 2)


def compare_hash_sets(cmp_hashes_obj: dict, db_hashes_str: dict):
    """
    비교 이미지 해시(cmp_hashes_obj: dict of ImageHash)
    DB 저장 해시(db_hashes_str: dict of hex str)
    를 받아서

    - full, center, top 각각 유사도 (있을 때만)
    - 단순 평균 유사도

    를 반환
    """
    sims = {}
    total = 0.0
    count = 0

    for key in ["full", "center", "top"]:
        ch = cmp_hashes_obj.get(key)
        dh_str = db_hashes_str.get(key)
        if ch is None or not dh_str:
            continue
        try:
            dh = imagehash.hex_to_hash(dh_str)
            s = similarity(ch, dh)
            sims[key] = s
            total += s
            count += 1
        except Exception:
            continue

    avg = round(total / count, 2) if count > 0 else None
    return sims, avg


def img_to_vec_center(img: Image.Image, size: int = 64) -> np.ndarray:
    """
    중앙 얼굴 영역(center crop)을 기준으로
    그레이스케일 + size x size 로 축소한 후 1차원 벡터(0~1)로 변환
    """
    cropped = center_crop(img, ratio=0.6).convert("L")
    resized = cropped.resize((size, size))
    arr = np.asarray(resized, dtype=np.float32).flatten()
    if arr.size == 0:
        return arr
    if arr.max() > 0:
        arr = arr / 255.0
    return arr


def cosine_similarity(v1: np.ndarray, v2: np.ndarray) -> float:
    """
    두 벡터 사이의 코사인 유사도 (0~100%)
    """
    if v1.size == 0 or v2.size == 0:
        return 0.0
    denom = np.linalg.norm(v1) * np.linalg.norm(v2)
    if denom == 0:
        return 0.0
    return float(np.dot(v1, v2) / denom * 100.0)


# =========================
# S3 / DB 유틸
# =========================
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


def insert_image_record(
    file_name: str,
    s3_url: str,
    phash_json_str: str,
    description: str | None = None,
):
    """
    image_files 테이블에 한 줄 삽입
    컬럼 예시: id, file_name, s3_url, phash_json(JSON), description, uploaded_at ...
    """
    conn = get_db_conn()
    with conn:
        with conn.cursor() as cur:
            sql = """
                INSERT INTO image_files (file_name, s3_url, phash_json, description)
                VALUES (%s, %s, %s, %s)
            """
            cur.execute(sql, (file_name, s3_url, phash_json_str, description))
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
st.title("🖼 이미지 유사도 검사 (pHash + 중앙 얼굴 픽셀 코사인)")


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

                img_full = Image.open(BytesIO(data)).convert("RGB")

                # full/center/top pHash (문자열)
                phash_dict_str = calc_multi_phash_str(img_full)
                phash_json_str = json.dumps(phash_dict_str)

                # S3 업로드
                s3_key = upload_to_s3(BytesIO(data), f.name, prefix="source-images")
                s3_url = f"s3://{BUCKET}/{s3_key}"

                # DB 기록
                insert_image_record(
                    f.name,
                    s3_url,
                    phash_json_str=phash_json_str,
                    description=desc_common if desc_common else None,
                )
                count += 1

            st.success(f"✅ 원본 이미지 {count}개 등록 완료!")

    # -------------------------
    # DB 목록 + 썸네일 + 설명 수정 + 미리보기
    # -------------------------
    st.markdown("### 표지 썸네일 & 미리보기 (설명 직접 수정)")

    try:
        df = load_all_images()

        if df.empty:
            st.info("아직 저장된 원본 이미지가 없습니다.")
        else:
            # 전체 리스트 CSV 다운로드
            csv = df.to_csv(index=False).encode("utf-8-sig")
            st.download_button(
                "⬇️ 전체 목록 CSV 다운로드",
                data=csv,
                file_name="image_files.csv",
                mime="text/csv",
            )

            st.markdown("")

            # 헤더 라인
            header_cols = st.columns([1, 3, 4, 2, 1])
            header_cols[0].markdown("**ID**")
            header_cols[1].markdown("**파일명**")
            header_cols[2].markdown("**설명 (수정 가능)**")
            header_cols[3].markdown("**썸네일**")
            header_cols[4].markdown("**액션**")

            st.divider()

            updated_rows = []  # id, description 저장용

            for _, row in df.iterrows():
                row_cols = st.columns([1, 3, 4, 2, 1])

                with row_cols[0]:
                    st.write(row["id"])

                with row_cols[1]:
                    st.write(row["file_name"])

                # 설명 편집용 text_input
                with row_cols[2]:
                    new_desc = st.text_input(
                        label="",
                        value=row.get("description") or "",
                        key=f"desc_{row['id']}",
                        placeholder="설명을 입력하세요",
                    )
                updated_rows.append({"id": row["id"], "description": new_desc})

                # 썸네일
                with row_cols[3]:
                    try:
                        key = row["s3_url"].split(f"s3://{BUCKET}/", 1)[-1]
                        thumb = load_image_from_s3(key)
                        st.image(thumb, width=90)
                    except Exception:
                        st.write("이미지 오류")

                # 미리보기 버튼
                with row_cols[4]:
                    if st.button("미리보기", key=f"preview_{row['id']}"):
                        st.session_state["preview_image_id"] = row["id"]

            # 설명 저장 버튼
            if st.button("💾 설명 변경 내용 저장"):
                try:
                    conn = get_db_conn()
                    with conn:
                        with conn.cursor() as cur:
                            for r in updated_rows:
                                sql = "UPDATE image_files SET description = %s WHERE id = %s"
                                cur.execute(sql, (r["description"], r["id"]))
                        conn.commit()
                    st.success("설명 변경 내용이 저장되었습니다.")
                except Exception as e:
                    st.error(f"설명 저장 중 오류: {e}")

            # 선택한 이미지 큰 미리보기
            if "preview_image_id" in st.session_state:
                sel_id = st.session_state["preview_image_id"]
                try:
                    sel_row = df[df["id"] == sel_id].iloc[0]

                    st.markdown("---")
                    st.markdown("### 🔍 선택한 이미지 미리보기")

                    key = sel_row["s3_url"].split(f"s3://{BUCKET}/", 1)[-1]
                    img = load_image_from_s3(key)

                    st.image(img, width=400, caption=sel_row["file_name"])
                    st.write(f"**ID:** {sel_row['id']}")
                    st.write(f"**파일명:** {sel_row['file_name']}")
                    st.write(f"**설명:** {sel_row.get('description') or '없음'}")
                    st.write(f"**업로드 시간:** {sel_row.get('uploaded_at', '')}")
                    st.write(f"**S3 URL:** `{sel_row['s3_url']}`")
                except Exception as e:
                    st.error(f"미리보기 로드 중 오류: {e}")

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

    threshold = st.slider("표시할 최소 최종 유사도(%)", 0, 100, 40, 5)
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
                    # 업로드 이미지 전체 로딩
                    cmp_img = Image.open(BytesIO(data)).convert("RGB")

                    # 업로드 이미지의 full/center/top 해시 (ImageHash 객체)
                    cmp_hashes_obj = calc_multi_phash_objects(cmp_img)
                    # 업로드 이미지의 중앙 얼굴 벡터 (2차 필터용)
                    cmp_vec_center = img_to_vec_center(cmp_img)

                    st.markdown("#### 업로드한 이미지")
                    st.image(cmp_img, width=300)

                    results = []

                    for _, row in src_df.iterrows():
                        phash_json_val = row.get("phash_json")
                        if not phash_json_val:
                            continue

                        # JSON → dict (문자열/JSON 타입 모두 대응)
                        if isinstance(phash_json_val, dict):
                            db_hashes_str = phash_json_val
                        else:
                            try:
                                db_hashes_str = json.loads(phash_json_val)
                            except Exception:
                                continue

                        # 1차: pHash 기반 유사도들
                        sims, avg_phash = compare_hash_sets(cmp_hashes_obj, db_hashes_str)
                        if avg_phash is None:
                            continue

                        # 2차: 픽셀 기반 코사인 유사도 (중앙 얼굴 영역)
                        try:
                            key = row["s3_url"].split(f"s3://{BUCKET}/", 1)[-1]
                            db_img = load_image_from_s3(key)
                            db_vec_center = img_to_vec_center(db_img)
                            pixel_sim = cosine_similarity(cmp_vec_center, db_vec_center)
                        except Exception:
                            pixel_sim = 0.0

                        # 최종 종합 유사도: pHash 평균과 픽셀 유사도의 단순 평균
                        final_sim = round((avg_phash + pixel_sim) / 2, 2)

                        # 최종 유사도가 threshold 이상인 것만 남김
                        if final_sim < threshold:
                            continue

                        results.append(
                            {
                                "id": row["id"],
                                "file_name": row["file_name"],
                                "s3_url": row["s3_url"],
                                "sim_final": final_sim,
                                "sim_avg_phash": avg_phash,
                                "sim_pixel": pixel_sim,
                                "sim_full": sims.get("full"),
                                "sim_center": sims.get("center"),
                                "sim_top": sims.get("top"),
                                "description": row.get("description"),
                            }
                        )

                    if not results:
                        st.info(f"최종 유사도 {threshold}% 이상 결과가 없습니다.")
                    else:
                        res_df = (
                            pd.DataFrame(results)
                            .sort_values("sim_final", ascending=False)
                            .head(top_n)
                        )

                        st.markdown("#### 유사도 결과 (pHash / 픽셀 / 최종)")

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
                                st.write(f"**최종 종합 유사도:** {r['sim_final']}%")
                                st.write(
                                    f"- pHash 평균: {r['sim_avg_phash']}% "
                                    f"(full: {r['sim_full']} / center: {r['sim_center']} / top: {r['sim_top']})"
                                )
                                st.write(
                                    f"- 픽셀 코사인 유사도(중앙 얼굴 영역): {round(r['sim_pixel'], 2)}%"
                                )
                                st.write(f"**파일명:** {r['file_name']}")
                                st.write(f"**S3 경로:** `{r['s3_url']}`")
                                st.write(
                                    f"**설명:** {r['description'] or '설명 없음'}"
                                )
