# Snowflake SQL Modernization Tool

## Overview

This project focuses on building a **Snowflake SQL Modernization Tool** that converts legacy SQL queries into **Snowflake‑optimized SQL** and provides **performance optimization recommendations**.

The tool leverages **Snowflake COCO (Snowflake Agent))** and **Streamlit UI** to allow users to:

- Paste legacy SQL
- Select SQL directly from Snowflake objects
- Convert to Snowflake optimized SQL
- Get optimization recommendations
- Reduced query complexity percent

---

# Problem Statement

Organizations migrating to Snowflake often have **legacy SQL code** written for:

- Oracle, Teradata, SQL Server, T-SQL, DB2, etc.

These legacy queries are not optimized for Snowflake and needs a long migration process in Snowflake while manual writing of large codes, validating those consumes lot of time.

This tool helps **automatically modernize legacy SQL into Snowflake optimized SQL**.

---

# Prompts Used for System Development using COCO

Below are the prompts used to design the SQL modernization system.

---

# Prompt 1 — Overall System Design

## Prompt

```
I am building a Snowflake SQL modernization tool.

The goal is:
- Take legacy SQL code
- Convert into Snowflake optimized SQL
- Suggest optimization improvements

Before implementation:
Explain what components are required to build this system.
```

## Purpose

This prompt defines:

- High level architecture
- Required components
- System boundaries


---

# Prompt 2 — Architecture Design

## Prompt

```
Based on the SQL modernization tool:

1. What are the major components?
2. What should be the workflow?
3. What Snowflake features can be used?
4. What are potential challenges?

Do not generate code yet.
Only provide architecture.
```

## Purpose

This prompt helps:

- Define system architecture
- Identify Snowflake features
- Understand workflow


---

# Prompt 3 — Legacy SQL Issues

## Prompt

```
For SQL modernization in Snowflake:

What are common legacy SQL issues?

Examples:
- inefficient joins
- subqueries
- temp tables
- cursor usage

```

## Purpose

Identify legacy SQL anti‑patterns:

### Legacy Issues

- Nested subqueries
- Cursor based processing
- Temp tables
- Multiple aggregations
- Complex joins
- Non‑optimized filters, etc.


---

# Prompt 4 — Output Format Design

## Prompt

```
Define output format for modernization tool.

Expected output:

1. Original SQL
2. Optimized SQL
3. Optimization Suggestions
4. Code Complexity reduction
```

## Purpose

Define structured output.


### 1. Original SQL

Legacy SQL code

### 2. Optimized SQL

Snowflake optimized SQL

### 3. Optimization Suggestions

- Explains the Optimization suggestions with reason

### 4. Complexity Reduction

- Gives the percent of code reduced after optimization

---

# Prompt 5 — Load SQL from Snowflake

## Prompt

```
I don't want only pasted SQL.

I want users to select SQL from Snowflake account.

- Select database
- Select schema
- Select object
- Load SQL

Explain approach.
```

## Purpose

Allow users to:

- Select database
- Select schema
- Select table/view
- Load SQL automatically


---

# Prompt 6 — Streamlit UI Design

## Prompt

```
I want to build Streamlit app for SQL modernization.

Features:
1. Paste SQL
2. Select SQL from Snowflake
3. Show Original code type
4. Show optimized SQL 
5. Show suggestions
6. Code Complexity reduction & Complexity analysis

Design UI layout and code for it.

```

## Purpose

- Create Streamlit UI and code

---


# Setup Instructions

### Just COPY the prompts and run it in the COCO UI Agent of your Snowflake account last prompt gives you the code that you can run as follows :

## Step 0 - Dependencies Required in Snowflake Streamlit file

```
- snowflake-snowpark-python
- streamlit
```

## Step 1 — Copy Code

- Copy the code from file

```
System.py
```


## Step 2 — Create Streamlit file

- Create a Streamlit file in the Snowflake account and paste the code into the Streamlit file.


## Step 3 — Run the file
Run your Streamlit file

## Step 4 — Streamlit UI 
When you run the streamlit file the Streamlit UI appers in the right pane ready to use.

## UI Layout (Headline - Legacy SQL Modernization Tool)

### There are two options - 
- Paste SQL
- Browse file
- 
### Paste SQL UI
![Paste SQL UI](images/[paste_sql.png](https://github.com/Kunalsrp/CoCoders/blob/main/images/Paste_Code.jpg))

### Browse File UI
![Browse File UI](https://github.com/Kunalsrp/CoCoders/blob/main/images/Browse_File.jpg)

### Output UI
![Output UI](https://github.com/Kunalsrp/CoCoders/blob/main/images/Output.jpg)

### When you select Paste SQL button 
- There is a text box to paste code

### When you paste code press optimize code button to Optimize code

### When you select Browse file button 
- There are dropdowns to select
  - Database selector
  - Schema selector
  - Object type selector
  - Object type selector

### After selecting click on Fetch DDL which will fetch the code from respective object then click on Optimize Code button to optimize code



#### Output has -
- Original Code Type
- Optimized SQL
- Suggestions
- Code Complexity reduction percent & Complexity Analysis
- Side by Side code view




