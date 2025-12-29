import pandas as pd
import re

def run_wdq_mapping(df_domain, df_wise, df_rules):

    # ==============================
    # 2. 수집데이터 전처리
    # ==============================
    def preprocess_value(val):
        if pd.isna(val):
            return ""
        val_str = str(val).strip()
        if re.match(r"^\d{4}-\d{2}-\d{2}", val_str):
            val_str = val_str.split(".")[0]
        if re.match(r"^[0-9,]+$", val_str):
            return val_str.replace(",", "")
        return val_str

    # ==============================
    # 3. WDQ 검증룰 전처리
    # ==============================
    def compile_rule(rule, rule_name):
        rule = str(rule).strip()
        mapping = {
            "YYYY-MM-DD": r"^\d{4}-\d{2}-\d{2}$",
            "YYYY/MM/DD": r"^\d{4}/\d{2}/\d{2}$",
            "YYYYMMDD": r"^\d{8}$",
            "YYYY-MM-DD HH24:MI:SS": r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$",
            "YYYYMMDDHH24MISS": r"^\d{14}$",
            "HH24:MI:SS": r"^\d{2}:\d{2}:\d{2}$",
        }
        if rule in mapping:
            return (rule_name, "regex", re.compile(mapping[rule]))
        if rule.startswith("^") or rule.endswith("$"):
            try:
                return (rule_name, "regex", re.compile(rule))
            except:
                return None
        if "컬럼" in rule:
            expr = rule.replace("컬럼", "x")
            return (rule_name, "expr", lambda x: eval(expr))
        if "," in rule and not rule.startswith("^"):
            candidates = [r.strip() for r in rule.split(",")]
            return (rule_name, "enum", candidates)
        return None

    # ==============================
    # 3-1. 룰 사전 생성
    # ==============================
    compiled_rules = []
    rule_map = {}

    for _, row in df_rules.iterrows():
        rule = row.get("검증룰(*)")
        rule_name = row.get("검증룰명(*)")

        compiled = compile_rule(rule, rule_name)
        if compiled:
            compiled_rules.append(compiled)

        rule = str(rule).strip()
        rule_name = str(rule_name).strip()
        if rule_name and rule not in ["", "nan", "None"]:
            rule_map[rule] = rule_name

    valid_rule_names = set(rule_map.values())

    # ==============================
    # 4. 제외 / 강제 매핑
    # ==============================
    def check_exclusion_or_override(value, colname, datatype, current_rule):
        val_str = "" if pd.isna(value) else str(value).strip()
        colname = str(colname).upper()
        tokens = colname.split("_")

        # 1) CD, CODE, STT → 검증룰/컬럼의견 둘 다 없음
        if tokens[-1] in ["CD", "CODE", "STT"]:
            return "", "", ""

        # 2) 수집데이터 없음
        if val_str == "":
            return "", "", "컬럼내 데이터 없음"

        # 3) 값, 량 도메인 (수집데이터 % 또는 검증룰 %값진단)
        if "%" in val_str or str(current_rule).strip() == "%값진단":
            return "", "값, 량", ""

        # 4) _SEQ, _ID
        if tokens[-1] in ["SEQ", "ID"]:
            return "", "사번, ID 일련번호", ""

        # 5) 여부 도메인 (WDQ 사전 활용)
        values = [v.strip() for v in val_str.split(",")]
        for _, row in df_rules.iterrows():
            rule = str(row.get("검증룰(*)")).strip()
            rule_name = str(row.get("검증룰명(*)")).strip()
            if "," in rule:
                candidates = [r.strip() for r in rule.split(",")]
                if set(values).issubset(set(candidates)):
                    return "", rule_name, ""   # ← WDQ 사전의 검증룰명 사용

        # 6) TEXT (한글 포함, 여부 제외)
        if re.search(r"[가-힣]", val_str):
            return "", "", "TEXT형식 제외"
    
        return None

    # ==============================
    # 5. 검증룰 매칭 (계약번호 제거 버전)
    # ==============================
    def match_rule(value):
        val_str = preprocess_value(value)
        if val_str == "":
            return ""
    
        if val_str.isdigit():
            if len(val_str) == 8:
                try:
                    pd.to_datetime(val_str, format="%Y%m%d")
                    return "[기본]날짜 - YYYYMMDD"
                except:
                    return "값, 량"
            elif len(val_str) == 6:
                try:
                    pd.to_datetime(val_str, format="%Y%m")
                    return "[기본]날짜 - YYYYMM"
                except:
                    return "값, 량"
            elif len(val_str) == 4:
                try:
                    pd.to_datetime(val_str, format="%Y")
                    return "[기본]날짜 - YYYY"
                except:
                    return "값, 량"
            else:
                return "값, 량"

        for rule_name, rtype, cond in compiled_rules:
            try:
                if rtype == "regex":
                    if cond.match(val_str):
                        return rule_name
                elif rtype == "expr":
                    if val_str.isdigit() and cond(int(val_str)):
                        return rule_name
                elif rtype == "enum":
                    values = [v.strip() for v in val_str.split(",")]
                    if all(v in cond for v in values):
                        return rule_name
            except:
                continue
    
        return "값, 량"

    # ==============================
    # 6. 매핑 적용 (CD/CODE/STT는 무조건 빈칸)
    # ==============================
    for idx, row in df_domain.iterrows():
        col = str(row["컬럼명"]).upper()
        tokens = col.split("_")
        last = tokens[-1] if tokens else ""
    
        # ✅ 코드 컬럼은 무조건 공백 유지
        if last in ["CD", "CODE", "STT"]:
            df_domain.at[idx, "검증룰명"] = ""
            df_domain.at[idx, "컬럼의견"] = ""
            continue
    
        match = df_wise[(df_wise["테이블명"] == row["테이블명"]) &
                        (df_wise["컬럼명"] == row["컬럼명"])]
    
        if not match.empty:
            value = match.iloc[0]["수집데이터"]
            current_rule = row.get("검증룰", "")
            exclusion = check_exclusion_or_override(value, row["컬럼명"], row["DataType"], current_rule)
    
            if exclusion:
                _, rule_name, opinion = exclusion
                df_domain.at[idx, "컬럼의견"] = opinion
    
                if opinion and opinion.strip() != "":
                    df_domain.at[idx, "검증룰명"] = ""
                else:
                    if rule_name not in valid_rule_names:
                        rule_name = "값, 량"
                    df_domain.at[idx, "검증룰명"] = rule_name
            else:
                new_rule = match_rule(value)
                if new_rule not in valid_rule_names:
                    new_rule = "값, 량"
                df_domain.at[idx, "검증룰명"] = new_rule
                df_domain.at[idx, "컬럼의견"] = ""
    
    
    # ==============================
    # 7. 후처리: %값진단 완전 제거
    # ==============================
    mask_val_qty = df_domain["검증룰명"].astype(str).str.contains("%값진단", na=False)
    df_domain.loc[mask_val_qty, "검증룰명"] = "값, 량"
    
    
    # ==============================
    # 7-2. 후처리 개선: 날짜 검증 (수집데이터 + 타입 동시 판별)
    # ==============================
    
    def validate_date(val, fmt):
        try:
            pd.to_datetime(val, format=fmt)
            return True
        except:
            return False
    
    for idx, row in df_domain[df_domain["검증룰명"] == "값, 량"].iterrows():
        tbl, col = row["테이블명"], row["컬럼명"]
        dtype = str(row["DataType"]).upper()
    
        # 원본 수집데이터
        match = df_wise[(df_wise["테이블명"] == tbl) & (df_wise["컬럼명"] == col)]
        if match.empty:
            continue
        raw_val = str(match.iloc[0]["수집데이터"]).strip()
        if raw_val == "" or raw_val.lower() == "nan":
            continue
    
        # 숫자만 추출
        candidate = re.sub(r"[^0-9]", "", raw_val)
        clen = len(candidate)
        matched = False
    
        # --------------------------
        # 타입 길이 확인
        # --------------------------
        max_len = None
        if "CHAR" in dtype or "VARCHAR" in dtype:
            m = re.search(r"\((\d+)\)", dtype)
            if m:
                max_len = int(m.group(1))
        elif "DATE" in dtype and clen >= 8:
            max_len = 8   # DATE → YYYYMMDD
        elif "DATETIME" in dtype and clen >= 14:
            max_len = 14  # DATETIME → YYYYMMDDHH24MISS
    
        # 타입이 있으면 타입 길이에 맞춰 자름
        if max_len and clen > max_len:
            candidate = candidate[:max_len]
            clen = len(candidate)
    
        # --------------------------
        # 값 길이별 매핑
        # --------------------------
        # 14자리: YYYYMMDDHH24MISS
        if not matched and clen == 14 and "YYYYMMDDHH24MISS" in rule_map:
            if validate_date(candidate, "%Y%m%d%H%M%S"):
                df_domain.at[idx, "검증룰명"] = rule_map["YYYYMMDDHH24MISS"]
                matched = True
    
        # 12자리: YYYYMMDDHH24MI
        if not matched and clen == 12 and "YYYYMMDDHH24MI" in rule_map:
            if validate_date(candidate, "%Y%m%d%H%M"):
                df_domain.at[idx, "검증룰명"] = rule_map["YYYYMMDDHH24MI"]
                matched = True
    
        # 10자리: YYYYMMDDHH24
        if not matched and clen == 10 and "YYYYMMDDHH24" in rule_map:
            if validate_date(candidate, "%Y%m%d%H%M"):
                df_domain.at[idx, "검증룰명"] = rule_map["YYYYMMDDHH24"]
                matched = True
    
        # 8자리: YYYYMMDD
        if not matched and clen == 8 and "YYYYMMDD" in rule_map:
            if validate_date(candidate, "%Y%m%d"):
                df_domain.at[idx, "검증룰명"] = rule_map["YYYYMMDD"]
                matched = True
    
        # 6자리: YYYYMM
        if not matched and clen == 6 and "YYYYMM" in rule_map:
            if validate_date(candidate, "%Y%m"):
                df_domain.at[idx, "검증룰명"] = rule_map["YYYYMM"]
                matched = True
    
        # 4자리: YYYY
        if not matched and clen == 4 and "YYYY" in rule_map:
            year = int(candidate)
            if 1900 <= year <= 2100:
                df_domain.at[idx, "검증룰명"] = rule_map["YYYY"]
                matched = True
    
        # --------------------------
        # 끝까지 안 걸리면 "값, 량" 유지
        # --------------------------
    
    
    # ==============================
    # 9. 후처리 2단계: 검증룰명 공백인데 코드 컬럼 아님 → 재검증
    # ==============================
    for idx, row in df_domain.iterrows():
        col = str(row["컬럼명"]).upper()
        tokens = col.split("_")
        last = tokens[-1] if tokens else ""
    
        # 대상: 검증룰명 공백 + 코드 컬럼 아님 + 컬럼의견 비어있음
        if row["검증룰명"] != "" or last in ["CD", "CODE", "STT"] or str(row["컬럼의견"]).strip() != "":
            continue
    
        # 수집데이터 가져오기
        match = df_wise[(df_wise["테이블명"] == row["테이블명"]) &
                        (df_wise["컬럼명"] == row["컬럼명"])]
        if match.empty:
            continue
    
        raw_val = str(match.iloc[0]["수집데이터"]).strip()
        if raw_val == "" or raw_val.lower() == "nan":
            continue
    
        # 수집데이터 정규화
        candidate = re.sub(r"[^0-9a-zA-Z가-힣]", "", raw_val)
    
        # WDQ 룰사전과 비교
        new_rule = None
        for rule_name, rtype, cond in compiled_rules:
            try:
                if rtype == "regex" and cond.match(candidate):
                    new_rule = rule_name
                    break
                elif rtype == "expr" and candidate.isdigit() and cond(int(candidate)):
                    new_rule = rule_name
                    break
                elif rtype == "enum":
                    values = [v.strip() for v in raw_val.split(",")]
                    if all(v in cond for v in values):
                        new_rule = rule_name
                        break
            except:
                continue
    
        # 매칭 성공 → 해당 검증룰명
        if new_rule:
            df_domain.at[idx, "검증룰명"] = new_rule
        else:
            df_domain.at[idx, "검증룰명"] = "값, 량"
    
    # ==============================
    # 10. 후처리 3단계: 계약번호 → 사번, ID 일련번호 강제 매핑
    # ==============================
    mask_contract = df_domain["검증룰명"] == "계약번호"
    df_domain.loc[mask_contract, "검증룰명"] = "사번, ID 일련번호"
    
    final_cols = [
        "테이블명","테이블한글명",
        "컬럼명","컬럼한글명",
        "DataType","검증룰명",
        "코드분류ID","컬럼의견"
    ]
    return df_domain[final_cols]