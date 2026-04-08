import streamlit as st
import pandas as pd
import uuid
from pendulum import now
from google.oauth2 import service_account
from google.cloud import bigquery
from datetime import datetime

# =====================================================
# PAGE CONFIG
# =====================================================
st.set_page_config(
    page_title="Distributor Operational Assessment",
    layout="wide"
)

st.title("📋 Distributor Operational Assessment Form")

# =====================================================
# MONTH YEAR SELECTION
# =====================================================
current_year = datetime.now().year
years = [current_year - 1, current_year, current_year + 1]

months = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December"
]

st.subheader("📅 Assessment Period")

col1, col2 = st.columns(2)

with col1:
    selected_month = st.selectbox("Select Month", months)

with col2:
    selected_year = st.selectbox("Select Year", years)

assessment_period = f"{selected_month} {selected_year}"


# =====================================================
# REPRESENTATIVE NAME INPUT
# =====================================================
representative_name = st.text_input(
    "Representative Name",
    placeholder="Enter your full name"
)

# =====================================================
# BIGQUERY CONNECTION (USING STREAMLIT SECRETS)
# =====================================================
gcp_secrets = dict(st.secrets["connections"]["bigquery"])
gcp_secrets["private_key"] = gcp_secrets["private_key"].replace("\\n", "\n")

credentials = service_account.Credentials.from_service_account_info(gcp_secrets)

PROJECT_ID = st.secrets["bigquery"]["project"]
DATASET = st.secrets["bigquery"]["dataset"]
TABLE = "distributor_assessment"

bq_client = bigquery.Client(
    credentials=credentials,
    project=PROJECT_ID
)

# =====================================================
# HELPER FUNCTIONS
# =====================================================
def get_sla_grade(inner, outer):
    if inner == "<80%" or outer == "<80%":
        return "C", 0
    elif inner == "99%-80%" or outer == "99%-80%":
        return "B", 4
    else:
        return "A", 8

# =====================================================
# LOAD MASTER DISTRIBUTOR
# =====================================================
@st.cache_data(ttl=600)
def load_master_distributor():
    query = f"""
        SELECT DISTINCT
            region,
            distributor_company,
            distributor
        FROM `{PROJECT_ID}.{DATASET}.master_distributor`
        WHERE status = 'Active' AND brand IN (
        "SKT & G2G & TPH & FR & BB & NP",
        "SKT & G2G & FR & BB & NP",
        "SKT & TPH & FR")
        ORDER BY region, distributor_company
    """
    df = bq_client.query(query).to_dataframe()

    for col in ["region", "distributor_company", "distributor"]:
        df[col] = df[col].astype(str).str.strip()

    return df

# =====================================================
# LOAD YTD SELL THROUGH
# =====================================================
@st.cache_data(ttl=600)
def get_ytd_sell_through(distributor_name, selected_year):
    
    query = f"""
        SELECT 
            SUM(value) AS ytd_value
        FROM `pbi_gt_dataset.fact_sell_through_all`
        WHERE distributor_name = @distributor_name
        AND EXTRACT(YEAR FROM calendar_date) = @selected_year
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("distributor_name", "STRING", distributor_name),
            bigquery.ScalarQueryParameter("selected_year", "INT64", selected_year),
        ]
    )

    df = bq_client.query(query, job_config=job_config).to_dataframe()

    if df.empty or df["ytd_value"].iloc[0] is None:
        return 0

    return df["ytd_value"].iloc[0]


master_df = load_master_distributor()

# =====================================================
# CASCADED FILTER (REGION → DISTRIBUTOR)
# =====================================================
region_options = sorted(master_df["region"].dropna().unique())
region = st.selectbox("Select Region", ["- Select Region -"] + region_options)

if region != "- Select Region -":
    dist_options = (
        master_df[master_df["region"] == region]["distributor"]
        .dropna()
        .unique()
    )
    dist_options = sorted(dist_options)

    distributor = st.selectbox(
        "Select Distributor",
        ["- Select Distributor -"] + dist_options
    )
else:
    distributor = "- Select Distributor -"

# =====================================================
# QUESTIONS CONFIG
# =====================================================
questions = {
    "OPERATIONAL LEADER (SPV / OPERATIONAL MANAGER)": {
        "A": ("Exist, exclusive for SKINTIFIC", 5),
        "B": ("Exist, mix with other principle", 3),
        "C": ("Do not exist", 0),
    },
    "SALESMAN": {
        "A": ("Exist, exclusive for SKINTIFIC", 7),
        "B": ("Exist, mix with other principle", 5),
        "C": ("Exist (do not meet qty requirement)", 3),
        "D": ("Do not exist", 0),
    },
    "ADMINISTRATIVE & AR SUPPORT": {
        "A": ("Exist, exclusive for SKINTIFIC", 3),
        "B": ("Exist, mix with other principle", 1),
        "C": ("Do not exist", 0),
    },
    "WAREHOUSE FACILITY STANDARD": {
        "A": ("Dedicated warehouse + temperature control", 3),
        "B": ("Warehouse exists but limited facility", 1),
        "C": ("No dedicated warehouse", 0),
    },    
    "DELIVERY SLA COMPLIANCE": {
    "A": ("≥ 95% on-time delivery", 8),
    "B": ("Partial SLA compliance", 4),
    "C": ("Below SLA requirement", 0),
    },
    "INVENTORY CONTROL & STOCK OPNAME": {
        "A": ("Stock opname ≥ 2x/year", 6),
        "B": ("Stock opname 1x/year", 4),
        "C": ("No regular stock opname", 0),
    },
    "DATA REPORTING COMPLIANCE": {
        "A": ("On Time", 8),
        "B": ("+ 1 Days", 4),
        "C": ("+ more than 1 Days", 0),
    },
    "ACCOUNT RECEIVABLE (AR) PERFORMANCE": {
        "A": ("On Time", 4),
        "B": ("+ 2 Days", 2),
        "C": ("+ more than 2 Days", 0),
    },
    "BAD STOCK HANDLING PERFORMANCE": {
        "A": ("100% compliance", 2),
        "B": ("≥ 80%", 1),
        "C": ("< 80%", 0),
    },
    "BANK GUARANTEE UPDATE COMPLIANCE": {
        "A": ("100% BG updated within agreed timeline", 4),
        "B": ("BG updated with delay / negotiation", 2),
        "C": ("BG not willing used", 0),
    }
}

# =====================================================
# FORM SECTION
# =====================================================
if (
    selected_month and
    selected_year and
    representative_name.strip() != "" and
    region != "- Select Region -" and
    distributor != "- Select Distributor -"
):

    st.success("Please complete all assessment questions")

    answers = {}

    with st.form("assessment_form"):

        for question, grades in questions.items():
            st.subheader(question)

            # ==========================================
            # 1️⃣ SPECIAL CASE → DELIVERY SLA
            # ==========================================
            if question == "DELIVERY SLA COMPLIANCE":

                st.info("""
                📌 **Scoring Logic:**
                - If either Inner or Outer < 80% → 0 points
                - If either Inner or Outer is 99%-80% (and none <80%) → 4 points
                - If both Inner and Outer are 100% → 8 points
                """)

                st.markdown("### INNER CITY (2 x 24 Hours)")
                inner_city = st.radio(
                    "Inner City SLA",
                    ["100%", "99%-80%", "<80%"],
                    key="inner_city_sla"
                )

                st.markdown("### OUTER CITY (3 x 24 Hours)")
                outer_city = st.radio(
                    "Outer City SLA",
                    ["100%", "99%-80%", "<80%"],
                    key="outer_city_sla"
                )

                answers[question] = {
                    "inner": inner_city,
                    "outer": outer_city
                }

                continue

            # ==========================================
            # SPECIAL CASE → BAD STOCK HANDLING
            # ==========================================
            if question == "BAD STOCK HANDLING PERFORMANCE":

                ytd_value = get_ytd_sell_through(distributor, selected_year)
                bs_allowance = ytd_value * 0.005

                st.markdown("### 📊 YTD Sell Through (Value)")
                st.info(f"Rp {ytd_value:,.0f}")

                st.markdown("### 💰 Bad Stock Allowance (0.5%)")
                st.info(f"Rp {bs_allowance:,.0f}")

                grade_option = st.radio(
                    "Select Compliance Level",
                    options=list(grades.keys()),
                    format_func=lambda x: f"{x} - {grades[x][0]} ({grades[x][1]} pts)",
                    key=f"grade_{question}"
                )

                answers[question] = {
                    "grade": grade_option,
                    "ytd_value": ytd_value,
                    "bs_allowance": bs_allowance
                }

                continue

            # ==========================================
            # 2️⃣ NORMAL QUESTIONS (WITH GRADE)
            # ==========================================
            grade_option = st.radio(
                "Select Grade",
                options=list(grades.keys()),
                format_func=lambda x: f"{x} - {grades[x][0]} ({grades[x][1]} pts)",
                key=f"grade_{question}"
            )

            # ==========================================
            # 3️⃣ SPECIAL NAME HANDLING
            # ==========================================
            if question == "SALESMAN":

                st.info("If the number of salesmen is fewer than 5, please enter '-' in the remaining name fields.")

                salesman_names = []

                for i in range(1, 6):
                    name_input = st.text_input(
                        f"Salesman Name {i}",
                        key=f"name_{question}_{i}"
                    )
                    salesman_names.append(name_input)

                answers[question] = {
                    "grade": grade_option,
                    "person_name": salesman_names
                }

            elif question in [
                "OPERATIONAL LEADER (SPV / OPERATIONAL MANAGER)",
                "ADMINISTRATIVE & AR SUPPORT"
            ]:

                person_name = st.text_input(
                    f"Name for {question}",
                    key=f"name_{question}"
                )

                answers[question] = {
                    "grade": grade_option,
                    "person_name": person_name
                }

            else:

                answers[question] = {
                    "grade": grade_option
                }

        submitted = st.form_submit_button("🚀 Submit Assessment")

    # =====================================================
    # INSERT TO BIGQUERY
    # =====================================================
    if submitted:

        error_messages = []

        if not selected_month or not selected_year:
            error_messages.append("Assessment Month & Year must be selected.")
        # 1️⃣ Representative name validation
        
        if representative_name.strip() == "":
            error_messages.append("Representative Name must be filled.")

        # 2️⃣ Person name validation for specific metrics
        required_name_metrics = [
            "OPERATIONAL LEADER (SPV / OPERATIONAL MANAGER)",
            "SALESMAN",
            "ADMINISTRATIVE & AR SUPPORT"
        ]

        for question in required_name_metrics:

            selected_grade = answers[question]["grade"]

            do_not_exist_grades = [
                key for key, val in questions[question].items()
                if "Do not exist" in val[0]
            ]

            # 🔹 SALESMAN VALIDATION
            if question == "SALESMAN":

                salesman_names = [
                    st.session_state.get(f"name_{question}_{i}", "").strip()
                    for i in range(1, 6)
                ]

                if selected_grade in do_not_exist_grades:
                    # If Do Not Exist → all must be empty
                    if any(name != "" for name in salesman_names):
                        error_messages.append(
                            "SALESMAN: You selected 'Do not exist' but filled names."
                        )
                else:
                    # Must fill exactly 5 names
                    filled_count = sum(1 for name in salesman_names if name != "")
                    if filled_count != 5:
                        error_messages.append(
                            "SALESMAN: You must fill exactly 5 salesman names."
                        )

            # 🔹 OTHER METRICS (1 NAME)
            else:

                person_name = st.session_state.get(f"name_{question}", "").strip()

                if selected_grade in do_not_exist_grades and person_name != "":
                    error_messages.append(
                        f"{question}: You selected 'Do not exist' but still filled a name."
                    )

                if selected_grade not in do_not_exist_grades and person_name == "":
                    error_messages.append(
                        f"{question}: Name must be filled because role exists."
                    )

        # ==========================================
        # IF ERROR → STOP INSERT
        # ==========================================

        if error_messages:
            st.error("❌ Please fill the form below:")
            for msg in error_messages:
                st.write(f"- {msg}")
            st.stop()

        # ==========================================
        # INSERT TO BIGQUERY
        # ==========================================

        submission_id = str(uuid.uuid4())
        submitted_at = now("Asia/Jakarta").to_datetime_string()

        rows_to_insert = []

        total_score = 0

        for question, value in answers.items():

            if question == "DELIVERY SLA COMPLIANCE":
                inner = value["inner"]
                outer = value["outer"]

                if inner == "<80%" or outer == "<80%":
                    total_score += 0
                elif inner == "99%-80%" or outer == "99%-80%":
                    total_score += 4
                else:
                    total_score += 8

            else:
                grade = value.get("grade")
                total_score += questions[question][grade][1]

        for question, value in answers.items():

            # ======================================
            # DELIVERY SLA
            # ======================================
            if question == "DELIVERY SLA COMPLIANCE":

                inner = value["inner"]
                outer = value["outer"]

                grade, point = get_sla_grade(inner, outer)

                rows_to_insert.append({
                    "submission_id": submission_id,
                    "submitted_at": submitted_at,
                    "representative_name": representative_name.strip(),
                    "region": region,
                    "distributor": distributor,
                    "metric": question,
                    "grade": grade,
                    "person_name": None,
                    "point": point,
                    "assessment_period": assessment_period,
                    "total_score": total_score
                })

                continue


            # ======================================
            # NORMAL QUESTIONS
            # ======================================

            grade = value.get("grade")

            do_not_exist_grades = [
                key for key, val in questions[question].items()
                if "Do not exist" in val[0]
            ]


            # ======================================
            # SALESMAN
            # ======================================
            if question == "SALESMAN":

                salesman_names = [
                    st.session_state.get(f"name_{question}_{i}", "").strip()
                    for i in range(1, 6)
                ]

                for name in salesman_names:

                    if grade in do_not_exist_grades:
                        name = None

                    rows_to_insert.append({
                        "submission_id": submission_id,
                        "submitted_at": submitted_at,
                        "representative_name": representative_name.strip(),
                        "region": region,
                        "distributor": distributor,
                        "metric": question,
                        "grade": grade,
                        "person_name": name,
                        "point": questions[question][grade][1],
                        "assessment_period": assessment_period,
                        "total_score": total_score
                    })


            # ======================================
            # OTHER QUESTIONS
            # ======================================
            else:

                raw_name = st.session_state.get(f"name_{question}", "").strip()

                if grade in do_not_exist_grades:
                    raw_name = None

                rows_to_insert.append({
                    "submission_id": submission_id,
                    "submitted_at": submitted_at,
                    "representative_name": representative_name.strip(),
                    "region": region,
                    "distributor": distributor,
                    "metric": question,
                    "grade": grade,
                    "person_name": raw_name,
                    "point": questions[question][grade][1],
                    "assessment_period": assessment_period,
                    "total_score": total_score
                })

        table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"

        errors = bq_client.insert_rows_json(
            table_id,
            rows_to_insert
        )

        if errors:
            st.error("❌ Failed to submit")
            st.json(errors)
        else:
            st.success("✅ Assessment successfully submitted!")
            st.balloons()
