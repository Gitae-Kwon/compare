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
        port=mysql_conf.get("port", 3306),
        user=mysql_conf["user"],
        password=mysql_conf["password"],
        db=mysql_conf["database"],
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )


# =========================
# 유틸 함수들 (전체 phash)
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


# =========================
# 얼굴 검출 + 얼굴 phash
# =========================
mp_face_detection = mp.solutions.face_detection


def crop_main_face(pil_img, expand_ratio=0.25):
    """
    PIL 이미지를 받아서 가장 큰 얼굴 영역만 잘라서 반환.
    얼굴 못 찾으면 None 반환.
    """
    img = np.array(pil_img)  # RGB
    img_height, img_width, _ = img.shape

    with mp_face_detection.FaceDetection(
        model_selection=1, min_detection_confidence=0.5
    ) as face_detection:
        results = face_detection.process(cv2.cvtColor(img, cv2.COLOR_RGB2BGR))

        if not results.detections:
            return None

        detection = results.detections[0]
        bbox = detection.location_data.relative_bounding_box

        x = int(bbox.xmin * img_width)
        y = int(bbox.ymin * img_height)
        w = int(bbox.width * img_width)
        h = int(bbox.height * img_height)

        # 살짝 여유 있게 확장
        cx, cy = x + w // 2, y + h // 2
        half_w = int(w * (1 + expand_ratio) / 2)
        half_h = int(h * (1 + expand_ratio) / 2)

        x1 = max(0, cx - half_w)
        y1 = max(0, cy - half_h)
        x2 = min(img_width, cx + half_w)
        y2 = min(img_height, cy + half_h)

        face_img = img[y1:y2, x1:x2]
        if face_img.size == 0:
            return None

        return Image.fromarray(face_img)


def calc_face_phash(pil_img):
    """
    PIL 이미지에서 얼굴 영역만 잘라 phash 계산.
    얼굴을 못 찾으면 None 반환.
    """
    face = crop_main_face(pil_img)
    if face is None:
        return None
    return imagehash.phash(face)


# =========================
# DB 관련
# =========================
def insert_image_record(
    file_name,
    s3_url,
    phash_str,
    face_phash_str=None,
    description=None,
):
    """
    image_files 테이블에 한 줄 삽입
    (컬럼: file_name, s3_url, phash, face_phash, description ...)
    """
    conn = get_db_conn()
    with conn:
        with conn.cursor() as cur:
            sql = """
                INSERT INTO image_files (file_name, s3_url, phash, face_phash, description)
                VALUES (%s, %s, %s, %s, %s)
            """
            cur.execute(
                sql,
                (file_name, s3_url, phash_str, face_phash_str, description),
            )
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
st.title("🖼 이미지 유사도 검사 (S3 + MySQL + pHash + Face pHash)")

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

                # 전체 phash
                phash = calc_phash(BytesIO(data))
                phash_str = str(phash)

                # 얼굴 phash
                img_full = Image.open(BytesIO(data)).convert("RGB")
                face_ph = calc_face_phash(img_full)
                face_phash_str = str(face_ph) if face_ph is not None else None

                # S3 업로드
                s3_key = upload_to_s3(BytesIO(data), f.name, prefix="source-images")
                s3_url = f"s3://{BUCKET}/{s3_key}"

                # DB 기록
                insert_image_record(
                    f.name,
                    s3_url,
                    phash_str,
                    face_phash_str=face_phash_str,
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
                    st.write(f"**업로드 시간:** {sel_row['uploaded_at']}")
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
                    # 업로드 이미지 전체 phash
                    cmp_hash = calc_phash(BytesIO(data))

                    # 업로드 이미지 얼굴 phash
                    cmp_img_full = Image.open(BytesIO(data)).convert("RGB")
                    cmp_face_hash = calc_face_phash(cmp_img_full)

                    st.markdown("#### 업로드한 이미지")
                    st.image(Image.open(BytesIO(data)), width=300)

                    # DB phash 준비
                    src_df["hash_obj"] = src_df["phash"].apply(imagehash.hex_to_hash)

                    results = []

                    for _, row in src_df.iterrows():
                        # 1) 전체 이미지 유사도
                        sim_full = similarity(cmp_hash, row["hash_obj"])

                        # 2) 얼굴 유사도 (둘 중 하나라도 없으면 None)
                        sim_face = None
                        if cmp_face_hash is not None and row.get("face_phash"):
                            try:
                                face_hash_db = imagehash.hex_to_hash(row["face_phash"])
                                sim_face = similarity(cmp_face_hash, face_hash_db)
                            except Exception:
                                sim_face = None

                        # 3) 전체 + 얼굴 가중 평균
                        if sim_face is not None:
                            final_sim = round(sim_full * 0.4 + sim_face * 0.6, 2)
                        else:
                            final_sim = sim_full

                        if final_sim >= threshold:
                            results.append(
                                {
                                    "id": row["id"],
                                    "file_name": row["file_name"],
                                    "s3_url": row["s3_url"],
                                    "similarity": final_sim,
                                    "sim_full": sim_full,
                                    "sim_face": sim_face,
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
                                st.write(f"**최종 유사도:** {r['similarity']}%")
                                st.write(
                                    f"(전체: {r['sim_full']}% / 얼굴: "
                                    f"{r['sim_face'] if r['sim_face'] is not None else 'N/A'}%)"
                                )
                                st.write(f"**파일명:** {r['file_name']}")
                                st.write(f"**S3 경로:** `{r['s3_url']}`")
                                st.write(
                                    f"**설명:** {r['description'] or '설명 없음'}"
                                )
