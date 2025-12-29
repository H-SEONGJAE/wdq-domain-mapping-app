import streamlit as st
import pandas as pd
from logic import run_wdq_mapping

# =========================
# 페이지 설정
# =========================
st.set_page_config(page_title="WDQ 검증룰 자동화", layout="wide")

st.title("📊 WDQ 검증룰 자동화 도구")

st.markdown("""
이 도구는  
- **WDQ 검증룰 관리 기준 파일** 과  
- **사용자가 업로드한 수집 데이터 파일**을 기반으로  
컬럼 단위 검증룰 매칭을 수행하기 위한 사전 점검 화면입니다.
""")

# =========================
# 📌 업로드 예시 이미지
# =========================
st.markdown("## 📌 업로드 파일 예시 (아래와 같이 파일 전처리 후 업로드)")

col_img1, col_img2 = st.columns(2)

with col_img1:
    st.image(
        "도메인 규칙관리_최종파일예시.png",
        caption="업로드 할 도메인 규칙관리 최종파일예시",
        use_column_width=True
    )

with col_img2:
    st.image(
        "WDQ 수집데이터_파일예시.png",
        caption="업로드 할 WDQ 수집데이터 파일예시",
        use_column_width=True
    )

st.divider()

# =========================
# 🔒 고정 파일 로드
# =========================
st.markdown("## 🔒 WDQ 검증룰 관리 기준")

ref_df = pd.read_excel("WDQ검증룰관리.xlsx")

with st.expander("📄 WDQ 검증룰 관리 파일 미리보기"):
    st.write(f"행 수: {len(ref_df)} / 컬럼 수: {len(ref_df.columns)}")
    st.dataframe(ref_df.head(30))

# =========================
# 📂 사용자 파일 업로드
# =========================
st.markdown("## 📂 수집 데이터 파일 업로드")

file1 = st.file_uploader(
    "① 도메인 규칙관리 최종파일 업로드", type=["csv"]
)
file2 = st.file_uploader(
    "② WDQ 수집데이터 파일 업로드", type=["csv"]
)

# =========================
# 📄 업로드 파일 미리보기
# =========================

if file1:
    df1 = pd.read_csv(file1, encoding="utf-8", low_memory=False)
    with st.expander("📄 도메인 규칙관리 최종파일 미리보기"):
        st.write(f"행 수: {len(df1)} / 컬럼 수: {len(df1.columns)}")
        st.dataframe(df1.head(30))

if file2:
    df2 = pd.read_csv(file2, encoding="utf-8", low_memory=False)
    with st.expander("📄 WDQ 수집데이터 파일 미리보기"):
        st.write(f"행 수: {len(df2)} / 컬럼 수: {len(df2.columns)}")
        st.dataframe(df2.head(30))

st.info("※ 현재 단계는 **파일 구조 및 데이터 확인 단계**입니다. 다음 단계에서 검증룰 자동 매칭이 수행됩니다.")


# =========================
# 🚀 검증룰 자동 매칭 실행
# =========================
import io

if file1 and file2:
    if st.button("🚀 검증룰 자동 매칭 실행"):
        with st.spinner("검증룰 매칭 중입니다..."):
            df_result = run_wdq_mapping(
                df_domain=df1,
                df_wise=df2,
                df_rules=ref_df
            )

        st.success("검증룰 매핑 완료")

        st.markdown("## ✅ 매핑 결과 미리보기")
        st.dataframe(df_result.head(50))

        # =========================
        # 📥 결과 엑셀(XLSX) 다운로드
        # =========================
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            df_result.to_excel(
                writer,
                index=False,
                sheet_name="WDQ_값진단결과"
            )

        st.download_button(
            label="📥 결과 엑셀 다운로드 (파일명 변경 필)",
            data=output.getvalue(),
            file_name="WDQ 값진단 최종파일_DB명.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )



