import streamlit as st
import re
from snowflake.snowpark.context import get_active_session

session = get_active_session()

st.title("Legacy SQL Modernization Tool")
st.caption("Convert legacy SQL to Snowflake-optimized code with AI-powered suggestions")

if "optimized_code" not in st.session_state:
    st.session_state.optimized_code = None
if "suggestions" not in st.session_state:
    st.session_state.suggestions = None
if "detected_dialect" not in st.session_state:
    st.session_state.detected_dialect = None
if "complexity_reduction" not in st.session_state:
    st.session_state.complexity_reduction = None
if "complexity_breakdown" not in st.session_state:
    st.session_state.complexity_breakdown = None
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

Return ONLY the delimited sections above. No extra text."""


def compute_complexity(original_sql, optimized_sql):
    def score_sql(sql):
        s = sql.upper()
        scores = {}

        join_score = 0
        implicit_joins = len(re.findall(r'\bFROM\s+\w+\s*,\s*\w+', s))
        join_score += implicit_joins * 5
        if "(+)" in s:
            join_score += 5
        join_hints = len(re.findall(r'/\*\+.*?(ORDERED|USE_NL|USE_HASH|LEADING).*?\*/', s))
        join_score += join_hints * 3
        scores["Join Complexity"] = min(join_score, 25)

        subquery_score = 0
        subquery_count = len(re.findall(r'\(\s*SELECT\b', s))
        subquery_score += subquery_count * 8
        correlated = len(re.findall(r'\(\s*SELECT\b.*?WHERE\s+\w+\.\w+\s*=\s*\w+\.\w+', s, re.DOTALL))
        subquery_score += correlated * 5
        scores["Subquery Depth"] = min(subquery_score, 25)

        pred_score = 0
        nonsargable_fns = ["YEAR(", "MONTH(", "DAY(", "UPPER(", "LOWER(", "TRIM(", "TO_CHAR(", "TRUNC(", "NVL(", "SUBSTR("]
        for fn in nonsargable_fns:
            if fn in s:
                pred_score += 4
        or_chains = len(re.findall(r'\bOR\b', s))
        if or_chains > 3:
            pred_score += 5
        scores["Predicate Complexity"] = min(pred_score, 25)

        proc_score = 0
        if re.search(r'\bCURSOR\b', s):
            proc_score += 12
        if re.search(r'\bLOOP\b', s) or re.search(r'\bWHILE\b', s):
            proc_score += 8
        temp_tables = len(re.findall(r'\bCREATE\s+(OR\s+REPLACE\s+)?(GLOBAL\s+)?TEMP(ORARY)?\s+TABLE\b', s))
        proc_score += temp_tables * 4
        if re.search(r'\bEXECUTE\s+IMMEDIATE\b', s):
            proc_score += 3
        scores["Procedural Constructs"] = min(proc_score, 15)

        func_score = 0
        proprietary_fns = ["NVL(", "NVL2(", "ISNULL(", "DECODE(", "GETDATE()", "SYSDATE",
                           "ROWNUM", "TOP ", "CONNECT BY", "START WITH", "DATEPART(",
                           "CHARINDEX(", "PATINDEX(", "LEN(", "DATEDIFF("]
        for fn in proprietary_fns:
            if fn in s:
                func_score += 2
        scores["Function Overhead"] = min(func_score, 10)

        return scores

    orig_scores = score_sql(original_sql)
    opt_scores = score_sql(optimized_sql)

    orig_total = sum(orig_scores.values())
    opt_total = sum(opt_scores.values())

    if orig_total == 0:
        reduction = 0.0
    else:
        reduction = ((orig_total - opt_total) / orig_total) * 100

    reduction = round(max(min(reduction, 100), 0), 1)

    breakdown = {}
    for key in orig_scores:
        breakdown[key] = {
            "before": orig_scores[key],
            "after": opt_scores.get(key, 0),
        }

    return {
        "original_score": orig_total,
        "optimized_score": opt_total,
        "reduction_percent": reduction,
        "breakdown": breakdown,
    }


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
        dialect = type_match.group(1).strip() if type_match else "Unknown"
        code = code_match.group(1).strip() if code_match else raw.strip()
        suggestions = sugg_match.group(1).strip() if sugg_match else ""
        return code, suggestions, dialect
    except Exception as e:
        return f"Error: {e}", "", "Unknown"


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

if st.button("Optimize SQL", type="primary", use_container_width=True):
    if not st.session_state.legacy_sql:
        st.warning("Please paste or fetch SQL before optimizing.")
    else:
        with st.spinner("Analyzing and optimizing your SQL..."):
            optimized, suggestions, dialect = optimize_sql(st.session_state.legacy_sql)
            complexity = compute_complexity(st.session_state.legacy_sql, optimized)
            st.session_state.optimized_code = optimized
            st.session_state.suggestions = suggestions
            st.session_state.detected_dialect = dialect
            st.session_state.complexity_reduction = complexity["reduction_percent"]
            st.session_state.complexity_breakdown = complexity

if st.session_state.optimized_code:
    st.divider()

    tab1, tab2, tab3, tab4 = st.tabs([
        "Optimized Code",
        "Optimization Suggestions",
        "Complexity Analysis",
        "Side-by-Side",
    ])

    with tab1:
        st.subheader("Snowflake-Optimized SQL")
        st.code(st.session_state.optimized_code, language="sql")
        if st.session_state.complexity_reduction is not None:
            reduction = st.session_state.complexity_reduction
            if reduction >= 50:
                st.success(f"**Code Complexity Reduced: {reduction}%**")
            elif reduction >= 20:
                st.info(f"**Code Complexity Reduced: {reduction}%**")
            elif reduction > 0:
                st.warning(f"**Code Complexity Reduced: {reduction}%** (minor improvements)")
            else:
                st.info("**No measurable complexity reduction.** SQL may already be well-optimized.")

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
        st.subheader("Complexity Analysis")
        cb = st.session_state.complexity_breakdown
        if cb:
            m1, m2, m3 = st.columns(3)
            with m1:
                st.metric("Original Score", f"{cb['original_score']} / 100")
            with m2:
                st.metric("Optimized Score", f"{cb['optimized_score']} / 100")
            with m3:
                st.metric("Reduction", f"{cb['reduction_percent']}%")

            if cb["original_score"] > 0:
                st.progress(min(cb["reduction_percent"] / 100.0, 1.0))
            else:
                st.progress(0.0)

            st.subheader("Breakdown by Category")
            for comp, vals in cb["breakdown"].items():
                before = vals["before"]
                after = vals["after"]
                if before > 0:
                    change_pct = round(((before - after) / before) * 100)
                    icon = "+" if change_pct >= 0 else ""
                    label = f"{comp}: {before} -> {after} ({icon}{change_pct}% reduction)"
                else:
                    label = f"{comp}: {before} -> {after} (no change)"

                col_label, col_bar = st.columns([2, 3])
                with col_label:
                    if before > after:
                        st.markdown(f":green[{label}]")
                    elif before == after and before == 0:
                        st.markdown(f":gray[{label}]")
                    elif before == after:
                        st.markdown(f":orange[{label}]")
                    else:
                        st.markdown(f":red[{label}]")
                with col_bar:
                    max_score = {"Join Complexity": 25, "Subquery Depth": 25,
                                 "Predicate Complexity": 25, "Procedural Constructs": 15,
                                 "Function Overhead": 10}.get(comp, 25)
                    if max_score > 0:
                        st.progress(min(after / max_score, 1.0))

            st.caption("Scores: Join (0-25), Subquery (0-25), Predicate (0-25), Procedural (0-15), Functions (0-10)")

            if cb["original_score"] == 0:
                st.info("The original SQL has no detectable legacy patterns. It may already be Snowflake-compatible.")
        else:
            st.info("Run optimization to see complexity analysis.")

    with tab4:
        st.subheader("Comparison")
        c1, c2 = st.columns(2)
        with c1:
            st.caption("Original")
            st.code(st.session_state.legacy_sql if st.session_state.legacy_sql else "No original code", language="sql")
        with c2:
            st.caption("Optimized")
            st.code(st.session_state.optimized_code, language="sql")