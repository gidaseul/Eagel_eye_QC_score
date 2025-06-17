# src/ui_app.py

import streamlit as st
import pandas as pd
import requests
import time
import ast
import io
import json

# --- 설정 ---
# 로컬에서 실행 중인 FastAPI 서버의 주소입니다.
API_BASE_URL = "http://127.0.0.1:8000"

# --- 유틸리티 함수 ---
def parse_string_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    데이터프레임의 특정 컬럼들이 문자열일 경우,
    안전하게 파이썬 리스트/딕셔너리로 변환합니다.
    """
    cols_to_parse = ['menu_list', 'review_info', 'review_category', 'theme_mood', 'theme_topic', 'theme_purpose']
    
    for col in cols_to_parse:
        if col in df.columns:
            # ▼▼▼ [오류 수정] 모호성 오류를 해결하는 더 안전한 변환 함수 ▼▼▼
            def safe_literal_eval(cell_value):
                # 1. 값이 문자열인 경우에만 변환을 시도합니다.
                if isinstance(cell_value, str):
                    try:
                        # '[]', '{}' 형태의 문자열을 실제 파이썬 객체로 변환
                        return ast.literal_eval(cell_value)
                    except (ValueError, SyntaxError):
                        # 변환에 실패하면 (예: 일반 텍스트), 오류 없이 원본 문자열을 그대로 반환
                        return cell_value
                
                # 2. 문자열이 아니면 (이미 리스트, dict, 숫자, NaN 등) 그대로 반환합니다.
                return cell_value
            
            # .apply()에 새로 만든 안전한 함수를 적용
            df[col] = df[col].apply(safe_literal_eval)
            
    return df

# --- UI 화면 구성 ---
st.set_page_config(page_title="데이터 처리 파이프라인", layout="wide")
st.title("🚀 데이터 처리 파이프라인 실행기")
st.markdown("CSV 파일을 업로드하거나 텍스트를 직접 입력하여 파이프라인을 실행하세요.")

# --- 세션 상태 초기화 ---
# Streamlit이 재실행되어도 값을 유지하기 위해 session_state를 사용합니다.
if 'task_id' not in st.session_state:
    st.session_state.task_id = None
if 'result_df' not in st.session_state:
    st.session_state.result_df = None
if 'error_info' not in st.session_state:
    st.session_state.error_info = None

# --- 입력 UI ---
with st.sidebar:
    st.header("1. 입력 데이터 선택")
    input_method = st.radio(
        "입력 방식을 선택하세요",
        ("CSV 파일 업로드", "직접 텍스트 입력"),
        label_visibility="collapsed"
    )
    
    stores_to_process = []
    
    if input_method == "CSV 파일 업로드":
        uploaded_file = st.file_uploader("CSV 파일을 여기에 끌어다 놓으세요.", type=["csv"])
        if uploaded_file is not None:
            try:
                df = pd.read_csv(uploaded_file)
                if 'name' not in df.columns or 'location' not in df.columns:
                    st.error("오류: CSV 파일에 'name'과 'location' 컬럼이 반드시 포함되어야 합니다.")
                else:
                    st.success(f"{len(df)}개의 가게 정보를 읽었습니다.")
                    stores_to_process = df[['name', 'location']].to_dict('records')
            except Exception as e:
                st.error(f"파일을 읽는 중 오류가 발생했습니다: {e}")

    elif input_method == "직접 텍스트 입력":
        text_input = st.text_area("한 줄에 '가게이름,지역' 형식으로 입력", height=150, placeholder="온도 홍대합정점,마포구\n카와카츠 본점,마포구")
        if text_input:
            lines = text_input.strip().split('\n')
            for line in lines:
                parts = line.split(',')
                if len(parts) >= 2:
                    name = parts[0].strip()
                    location = ",".join(parts[1:]).strip()
                    stores_to_process.append({"name": name, "location": location})
            st.success(f"{len(stores_to_process)}개의 가게 정보를 입력했습니다.")

# --- 파이프라인 실행 버튼 ---
if st.sidebar.button("파이프라인 실행", disabled=(not stores_to_process), type="primary", use_container_width=True):
    # 새로운 작업을 시작하므로, 이전 결과와 오류 정보를 모두 초기화합니다.
    st.session_state.task_id = None
    st.session_state.result_df = None
    st.session_state.error_info = None

    payload = {"stores": stores_to_process}
    try:
        with st.spinner("API 서버에 작업을 요청하는 중..."):
            response = requests.post(f"{API_BASE_URL}/run-pipeline", json=payload, timeout=20)
            response.raise_for_status()
        
        task_info = response.json()
        st.session_state.task_id = task_info['task_id']
        st.rerun() # 상태가 변경되었으므로 UI를 즉시 새로고침

    except requests.exceptions.RequestException as e:
        st.error(f"API 서버에 연결할 수 없습니다: {e}")
        st.error("백엔드 API 서버(uvicorn src.api_server:app)가 실행 중인지 확인해주세요.")

# --- 메인 화면 표시 로직 ---

# 1. 작업이 실행 중이고 아직 결과가 없는 경우
if st.session_state.task_id and st.session_state.result_df is None:
    st.markdown("---")
    st.subheader(f"작업 진행 상태 (Task ID: {st.session_state.task_id})")
    status_placeholder = st.empty()

    with st.spinner("파이프라인이 실행 중입니다..."):
        while True:
            try:
                status_response = requests.get(f"{API_BASE_URL}/status/{st.session_state.task_id}", timeout=10)
                if status_response.status_code == 404:
                    st.session_state.error_info = {"error": "서버에서 작업을 찾을 수 없습니다. 서버가 재시작되었을 수 있습니다."}
                    st.session_state.task_id = None
                    st.rerun()
                    break
                
                status_response.raise_for_status()
                status_data = status_response.json()
                status = status_data.get("status", "unknown")
                status_placeholder.info(f"현재 상태: **{status}**")

                if status == "completed":
                    st.session_state.result_df = pd.DataFrame(status_data.get("result", []))
                    st.session_state.task_id = None # 작업이 끝났으므로 ID 초기화
                    st.rerun()
                    break
                elif status == "failed":
                    st.session_state.error_info = status_data.get("result", {})
                    st.session_state.task_id = None
                    st.rerun()
                    break
                
                time.sleep(5)
            except requests.exceptions.RequestException as e:
                st.session_state.error_info = {"error": f"상태 확인 중 API 서버 연결 실패: {e}"}
                st.session_state.task_id = None
                st.rerun()
                break

# 2. 작업이 성공적으로 완료되어 결과가 있는 경우
if st.session_state.result_df is not None:
    st.success("✅ 파이프라인 작업이 성공적으로 완료되었습니다!")
    result_df = st.session_state.result_df
    
    # 데이터프레임 타입 변환 (필요한 경우)
    result_df = parse_string_columns(result_df)
    
    st.markdown("---")
    st.subheader("결과 탐색기")
    store_names = result_df['name'].tolist()
    selected_name = st.selectbox('결과를 확인할 가게를 선택하세요:', options=store_names, index=0)

    if selected_name:
        selected_store_data = result_df[result_df['name'] == selected_name].iloc[0].to_dict()
        st.json(selected_store_data)

    st.markdown("---")
    st.subheader("전체 데이터 다운로드")
    col1, col2, col3 = st.columns(3)

    with col1:
        csv_data = result_df.to_csv(index=False, encoding='utf-8-sig')
        st.download_button("💾 CSV로 다운로드", csv_data, f"result.csv", "text/csv", use_container_width=True)

    with col2:
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            result_df.to_excel(writer, index=False, sheet_name='Result')
        excel_data = output.getvalue()
        st.download_button("📄 Excel로 다운로드", excel_data, f"result.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)

    with col3:
        json_data = result_df.to_json(orient='records', indent=4, force_ascii=False)
        st.download_button("📝 JSON으로 다운로드", json_data, f"result.json", "application/json", use_container_width=True)

# 3. 작업 중 오류가 발생한 경우
if st.session_state.error_info:
    st.error("❌ 파이프라인 작업 중 오류가 발생했습니다.")
    st.json(st.session_state.error_info)
