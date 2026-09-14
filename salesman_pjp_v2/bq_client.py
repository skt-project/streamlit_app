"""
BigQuery credential and client factory.
Uses st.secrets exclusively — no local-path fallback.
"""

import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery


def get_credentials():
    """Return (Credentials, project_id) from st.secrets only."""
    try:
        s = st.secrets["connections"]["bigquery"]
        creds = service_account.Credentials.from_service_account_info({
            "type":                        s["type"],
            "project_id":                  s["project_id"],
            "private_key_id":              s["private_key_id"],
            "private_key":                 s["private_key"].replace("\\n", "\n"),
            "client_email":                s["client_email"],
            "client_id":                   s["client_id"],
            "auth_uri":                    s["auth_uri"],
            "token_uri":                   s["token_uri"],
            "auth_provider_x509_cert_url": s["auth_provider_x509_cert_url"],
            "client_x509_cert_url":        s["client_x509_cert_url"],
        })
        return creds, s["project_id"]
    except Exception as exc:
        st.error(
            "BigQuery credentials tidak ditemukan di secrets. "
            "Pastikan [connections.bigquery] dikonfigurasi dengan benar."
        )
        raise


def get_client() -> bigquery.Client:
    creds, project = get_credentials()
    return bigquery.Client(credentials=creds, project=project)
