import streamlit as st
import re
import json
from snowflake.snowpark.context import get_active_session

session = get_active_session()

st.title("CoCo-Morph: Legacy Code Modernization Tool")
st.caption("Convert legacy Codes to Snowflake-optimized code with AI-powered suggestions")

if "optimized_code" not in st.session_state:
    st.session_state.optimized_code = None
if "suggestions" not in st.session_state:
    st.session_state.suggestions = None
if "detected_dialect" not in st.session_state:
    st.session_state.detected_dialect = None
if "score_data" not in st.session_state:
    st.session_state.score_data = {}
if "legacy_sql" not in st.session_state:
    st.session_state.legacy_sql = ""

SYSTEM_PROMPT = """You are an expert SQL modernization engineer. Your job is to convert legacy SQL code 
(Oracle PL/SQL, T-SQL, Teradata, MySQL, PostgreSQL, etc.) into Snowflake-optimized SQL.

You MUST structure your response using these exact delimiters:

===CODE_TYPE_START===
(Identify the source SQL dialect/platform, e.g. "Oracle PL/SQL", "T-SQL (SQL Server)", "Teradata", "MySQL", "PostgreSQL", "IBM DB2", "Hive SQL", "Standard SQL", etc. Return ONLY the dialect name, nothing else.)
===CODE_TYPE_END===

===OPTIMIZED_CODE_START===
(Place the fully modernized Snowflake SQL code here)
===OPTIMIZED_CODE_END===

===SUGGESTIONS_START===
(Place optimization suggestions here as numbered bullet points)
Each suggestion should follow this format:
<number>. [<SEVERITY>] <CATEGORY>
- Original: <what the original code had>
- Changed to: <what it was changed to>
- Reason: <why this improves the code>

Where SEVERITY is one of: Critical, Recommended, Nice-to-have
And CATEGORY is one of: Syntax Modernization, Performance, Snowflake-Native Features, Best Practices, Cost Optimization
===SUGGESTIONS_END===

===SCORE_START===
Analyze the original SQL and list ALL legacy/proprietary patterns found.
For each pattern, indicate whether it was converted in the optimized code.

Return ONLY a JSON object with these exact keys:
{
  "patterns_detected": [
    {"pattern": "<pattern name>", "original": "<what was in original>", "converted_to": "<what it became in optimized, or 'Not changed' if still present>"}
  ],
  "summary": "<2-3 sentence plain English summary of what was modernized and any remaining considerations>"
}

Legacy patterns include: proprietary functions (NVL, DECODE, ISNULL, GETDATE, ZEROIFNULL, OREPLACE, etc.), proprietary syntax (SELECT TOP, WITH NOLOCK, (+) joins, SEL, LOCKING ROW, etc.), procedural constructs (CURSOR, LOOP, FETCH, EXEC, COLLECT STATISTICS, etc.), temp table patterns (VOLATILE TABLE, GLOBAL TEMPORARY, #temp, etc.), proprietary DDL (PRIMARY INDEX, FALLBACK, MULTISET TABLE, etc.), and any other non-Snowflake syntax.
===SCORE_END===

Rules for optimization:
- Replace proprietary functions with Snowflake equivalents (NVL->COALESCE, DECODE->CASE, (+)->ANSI JOIN, etc.)
- Replace cursors/loops with set-based operations
- Use QUALIFY instead of subqueries for window function filtering
- Use CREATE OR REPLACE where appropriate
- Suggest CLUSTER BY for large tables if applicable
- Replace temp tables with CTEs or TRANSIENT tables where appropriate
- Use COPY INTO for bulk loads instead of row-by-row inserts
- Push filters earlier for better performance
- Use Snowflake semi-structured functions (FLATTEN, PARSE_JSON) where applicable
- Use IDENTIFIER() for dynamic SQL instead of string concatenation
- Recommend TRANSIENT tables for temporary data
- IMPORTANT: Minimize the number of lines in the optimized code. Combine statements where possible, remove unnecessary line breaks, use inline expressions, and consolidate CTEs. Only keep separate lines when required for readability or correctness.

Return ONLY the delimited sections above. No extra text."""


def get_databases():
    return session.sql("SHOW DATABASES").collect()


def get_schemas(database):
    return session.sql(f"SHOW SCHEMAS IN DATABASE \"{database}\"").collect()


def get_objects(database, schema, object_type):
    type_map = {
        "Tables (DDL)": "SHOW TABLES",
        "Views": "SHOW VIEWS",
        "Materialized Views": "SHOW MATERIALIZED VIEWS",
        "Stored Procedures": "SHOW PROCEDURES",
        "Functions": "SHOW USER FUNCTIONS",
        "Tasks": "SHOW TASKS",
        "Streams": "SHOW STREAMS",
        "Pipes": "SHOW PIPES",
        "Sequences": "SHOW SEQUENCES",
        "File Formats": "SHOW FILE FORMATS",
        "Stages": "SHOW STAGES",
        "Dynamic Tables": "SHOW DYNAMIC TABLES",
    }
    cmd = type_map.get(object_type, "SHOW TABLES")
    rows = session.sql(f"{cmd} IN \"{database}\".\"{schema}\"").collect()
    if object_type in ("Stored Procedures", "Functions"):
        results = []
        for row in rows:
            name = row["name"]
            try:
                args = row["arguments"]
            except Exception:
                args = ""
            sig = ""
            if args:
                match = re.search(r'\(.*?\)', args)
                sig = match.group(0) if match else "()"
            results.append({"name": name, "signature": f"{name}{sig}"})
        return results
    return [{"name": row["name"], "signature": row["name"]} for row in rows]


def get_ddl(object_type, full_name):
    ddl_type_map = {
        "Tables (DDL)": "TABLE",
        "Views": "VIEW",
        "Materialized Views": "MATERIALIZED VIEW",
        "Stored Procedures": "PROCEDURE",
        "Functions": "FUNCTION",
        "Tasks": "TASK",
        "Streams": "STREAM",
        "Pipes": "PIPE",
        "Sequences": "SEQUENCE",
        "File Formats": "FILE FORMAT",
        "Stages": "STAGE",
        "Dynamic Tables": "DYNAMIC TABLE",
    }
    ddl_type = ddl_type_map.get(object_type, "TABLE")
    try:
        result = session.sql(f"SELECT GET_DDL('{ddl_type}', '{full_name}')").collect()
        return result[0][0] if result else "Could not retrieve DDL."
    except Exception as e:
        return f"Error retrieving DDL: {e}"


def optimize_sql(legacy_code):
    escaped = legacy_code.replace("'", "''")
    prompt = f"{SYSTEM_PROMPT}\n\nLegacy SQL to modernize:\n{escaped}"
    query = f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            'claude-3-5-sonnet',
            '{prompt.replace(chr(39), chr(39)+chr(39))}'
        ) AS response
    """
    try:
        result = session.sql(query).collect()
        raw = result[0]["RESPONSE"]
        type_match = re.search(
            r'===CODE_TYPE_START===\s*(.*?)\s*===CODE_TYPE_END===',
            raw, re.DOTALL
        )
        code_match = re.search(
            r'===OPTIMIZED_CODE_START===\s*(.*?)\s*===OPTIMIZED_CODE_END===',
            raw, re.DOTALL
        )
        sugg_match = re.search(
            r'===SUGGESTIONS_START===\s*(.*?)\s*===SUGGESTIONS_END===',
            raw, re.DOTALL
        )
        score_match = re.search(
            r'===SCORE_START===\s*(.*?)\s*===SCORE_END===',
            raw, re.DOTALL
        )
        dialect = type_match.group(1).strip() if type_match else "Unknown"
        code = code_match.group(1).strip() if code_match else raw.strip()
        suggestions = sugg_match.group(1).strip() if sugg_match else ""

        score_data = {}
        if score_match:
            try:
                score_json = re.search(r'\{.*\}', score_match.group(1).strip(), re.DOTALL)
                if score_json:
                    score_data = json.loads(score_json.group(0))
            except Exception:
                score_data = {}

        return code, suggestions, dialect, score_data
    except Exception as e:
        return f"Error: {e}", "", "Unknown", {}


input_mode = st.radio(
    "Input Mode",
    options=["Paste Legacy Code", "Browse File"],
    horizontal=True,
)

if input_mode == "Paste Legacy Code":
    pasted = st.text_area(
        "Paste your legacy SQL code here",
        height=300,
        placeholder="-- Paste your Oracle PL/SQL, T-SQL, Teradata, or other legacy SQL here...",
        key="paste_sql_input",
    )
    st.session_state.legacy_sql = pasted

elif input_mode == "Browse File":
    col1, col2 = st.columns(2)
    with col1:
        databases = get_databases()
        db_names = [row["name"] for row in databases]
        selected_db = st.selectbox("Database", db_names)

    with col2:
        if selected_db:
            schemas = get_schemas(selected_db)
            schema_names = [row["name"] for row in schemas]
            selected_schema = st.selectbox("Schema", schema_names)
        else:
            selected_schema = st.selectbox("Schema", [])

    col3, col4 = st.columns(2)
    with col3:
        object_type = st.selectbox(
            "Object Type",
            [
                "Tables (DDL)",
                "Views",
                "Materialized Views",
                "Stored Procedures",
                "Functions",
                "Tasks",
                "Streams",
                "Pipes",
                "Sequences",
                "File Formats",
                "Stages",
                "Dynamic Tables",
            ],
        )

    obj_map = {}
    with col4:
        if selected_db and selected_schema:
            try:
                objects = get_objects(selected_db, selected_schema, object_type)
                obj_map = {}
                for obj in objects:
                    name = obj["name"]
                    sig = obj["signature"]
                    if name not in obj_map:
                        obj_map[name] = sig
                obj_names = sorted(obj_map.keys())
                selected_obj = st.selectbox("Object", obj_names)
            except Exception as e:
                st.warning(f"Could not list objects: {e}")
                selected_obj = None
        else:
            selected_obj = st.selectbox("Object", [])

    if selected_db and selected_schema and selected_obj:
        sig = obj_map.get(selected_obj, selected_obj)
        if object_type in ("Stored Procedures", "Functions"):
            full_name = f'"{selected_db}"."{selected_schema}".{sig}'
        else:
            full_name = f'"{selected_db}"."{selected_schema}"."{selected_obj}"'
        if st.button("Fetch DDL", type="secondary"):
            with st.spinner("Fetching DDL..."):
                fetched = get_ddl(object_type, full_name)
                st.session_state["fetched_sql"] = fetched
                st.session_state.legacy_sql = fetched

        if "fetched_sql" in st.session_state:
            st.session_state.legacy_sql = st.session_state["fetched_sql"]
            st.code(st.session_state.legacy_sql, language="sql")

st.divider()

if st.button("Optimize Code", type="primary", use_container_width=True):
    if not st.session_state.legacy_sql:
        st.warning("Please paste or fetch SQL before optimizing.")
    else:
        with st.spinner("Analyzing and optimizing your SQL..."):
            optimized, suggestions, dialect, score_data = optimize_sql(st.session_state.legacy_sql)
            st.session_state.optimized_code = optimized
            st.session_state.suggestions = suggestions
            st.session_state.detected_dialect = dialect
            st.session_state.score_data = score_data

if st.session_state.optimized_code:
    st.divider()

    tab1, tab2, tab3, tab4 = st.tabs([
        "Optimized Code",
        "Optimization Suggestions",
        "Pattern Analysis",
        "Side-by-Side",
    ])

    with tab1:
        st.subheader("Snowflake-Optimized SQL")
        st.code(st.session_state.optimized_code, language="sql")

    with tab2:
        st.subheader("Optimization Suggestions")
        if st.session_state.detected_dialect:
            st.info(f"**Code Type Detected:** {st.session_state.detected_dialect}")
        suggestions = st.session_state.suggestions
        if suggestions:
            points = suggestions.split("\n")
            for point in points:
                stripped = point.strip()
                if not stripped:
                    continue
                if "[Critical]" in stripped:
                    st.error(stripped)
                elif "[Recommended]" in stripped:
                    st.warning(stripped)
                elif "[Nice-to-have]" in stripped:
                    st.success(stripped)
                else:
                    st.write(stripped)
        else:
            st.info("No suggestions returned. Review the optimized code above.")

    with tab3:
        st.subheader("Pattern Analysis")
        sd = st.session_state.score_data
        if sd:
            summary = sd.get("summary", "")
            if summary:
                st.info(summary)

            patterns = sd.get("patterns_detected", [])
            if patterns:
                converted = [p for p in patterns if p.get("converted_to", "").lower() != "not changed"]
                not_converted = [p for p in patterns if p.get("converted_to", "").lower() == "not changed"]

                m1, m2 = st.columns(2)
                with m1:
                    st.metric("Patterns Detected", len(patterns))
                with m2:
                    st.metric("Patterns Converted", len(converted))

                st.subheader("Legacy Patterns Converted")
                for p in converted:
                    st.markdown(f":green[**{p.get('pattern', '')}**]")
                    st.caption(f"`{p.get('original', '')}` → `{p.get('converted_to', '')}`")

                if not_converted:
                    st.subheader("Patterns Remaining")
                    for p in not_converted:
                        st.markdown(f":orange[**{p.get('pattern', '')}**]")
                        st.caption(f"`{p.get('original', '')}`")
            else:
                st.info("No legacy patterns detected. SQL may already be Snowflake-compatible.")
        else:
            st.info("Run optimization to see pattern analysis.")

        with tab4:
            st.subheader("Comparison")
            orig = st.session_state.legacy_sql if st.session_state.legacy_sql else ""
            opt = st.session_state.optimized_code
            orig_lines = len([l for l in orig.strip().splitlines() if l.strip()]) if orig else 0
            opt_lines = len([l for l in opt.strip().splitlines() if l.strip()]) if opt else 0
            diff = orig_lines - opt_lines
    
            if diff > 0:
                st.success(f"**{diff} lines reduced** ({orig_lines} → {opt_lines})")
            elif diff < 0:
                st.info(f"**{abs(diff)} lines added** ({orig_lines} → {opt_lines}) — expanded for clarity")
            else:
                st.info(f"**Same line count** ({orig_lines} lines)")
    
            c1, c2 = st.columns(2)
            with c1:
                st.caption("Original")
                st.code(orig if orig else "No original code", language="sql")
            with c2:
                st.caption("Optimized")
                st.code(opt, language="sql")
