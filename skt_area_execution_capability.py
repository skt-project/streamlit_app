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
        "B": ("Exist", 5),
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
        "A": ("≥ 95% on-time reports", 8),
        "B": ("80–94% compliance", 4),
        "C": ("< 80%", 1),
    },
    "ACCOUNT RECEIVABLE (AR) PERFORMANCE": {
        "A": ("100% within credit terms", 4),
        "B": ("≥ 90%", 3),
        "C": ("≥ 70%", 1),
        "D": ("< 70%", 0),
    },
    "BAD STOCK HANDLING PERFORMANCE": {
        "A": ("100% compliance", 2),
        "B": ("≥ 90%", 1),
        "C": ("≥ 70%", 0),
        "D": ("< 70%", 0),
    },
    "BANK GUARANTEE UPDATE COMPLIANCE": {
        "A": ("100% updated on time", 4),
        "B": ("Updated with delay", 2),
        "C": ("Not updated", 0),
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

            # 1️⃣ Grade selection FIRST
            grade_option = st.radio(
                "Select Grade",
                options=list(grades.keys()),
                format_func=lambda x: f"{x} - {grades[x][0]} ({grades[x][1]} pts)",
                key=f"grade_{question}"
            )

            person_name = None

            # 2️⃣ Special handling for 3 metrics
            if question in [
                "OPERATIONAL LEADER (SPV / OPERATIONAL MANAGER)",
                "SALESMAN",
                "ADMINISTRATIVE & AR SUPPORT"
            ]:
                
                # SPECIAL CASE → SALESMAN (5 names required)
                if question == "SALESMAN":

                    salesman_names = []

                    for i in range(1, 6):  # 5 salesman
                        name_input = st.text_input(
                            f"Salesman Name {i}",
                            key=f"name_{question}_{i}"
                        )
                        salesman_names.append(name_input)

                    answers[question] = {
                        "grade": grade_option,
                        "person_name": salesman_names  # LIST of 5 names
                    }

                # NORMAL CASE → Only 1 name
                else:
                    person_name = st.text_input(
                        f"Name for {question}",
                        key=f"name_{question}"
                    )

                    answers[question] = {
                        "grade": grade_option,
                        "person_name": person_name
                    }

                continue

        total_score = sum(
            questions[q][answers[q]["grade"]][1] for q in answers
        )

        st.divider()
        st.metric("Total Score Preview", total_score)

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
            st.error("❌ Please fix the following errors:")
            for msg in error_messages:
                st.write(f"- {msg}")
            st.stop()

        # ==========================================
        # INSERT TO BIGQUERY
        # ==========================================

        submission_id = str(uuid.uuid4())
        submitted_at = now("Asia/Jakarta").to_datetime_string()

        rows_to_insert = []

        for question, value in answers.items():
            grade = value["grade"]

            # detect do not exist
            do_not_exist_grades = [
                key for key, val in questions[question].items()
                if "Do not exist" in val[0]
            ]

            raw_name = st.session_state.get(f"name_{question}", "").strip()

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
