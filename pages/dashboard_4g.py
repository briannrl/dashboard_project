import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import streamlit as st
import plotly.express as px

# PRESETS AND CONSTANTS
pd.options.plotting.backend = "plotly"
st.set_page_config(layout="wide")
CHART_COL_NUMBER = 4

# FUNCTIONS
@st.cache_data
def import_files(file_path):
    df_ = pd.read_csv(file_path)
    return df_

@st.cache_data
def fix_columns_with_percentage(series):
    series = (series.astype(str)
              .apply(lambda x: float(x.replace("%", ""))/100 
                    if x.endswith("%")
                    else float(x)/100
                    if float(x)>1
                    else float(x)
                    )
            )
    return series

@st.cache_data
def fix_date_columns(series):
    series = series.apply(lambda x: (datetime(1899, 12, 30) + timedelta(days=int(x))).strftime("%d/%m/%Y"))
    series = pd.to_datetime(series, dayfirst=True)
    return series

@st.cache_data
def clean_used_columns(series):
    series = series.astype(float)
    return series

def plot(df_, date_start_filter, date_end_filter, *args):
    filter_list = ["Cell Name", "Vendor LC", "Vendor GS", "Cluster", "Subnetwork Name", "Spotbeam", "PROJECT", "TECHNOLOGY COLO", "Days per Week", "BTS VENDOR", "REGIONAL"]
    filter = []
    for i, (arg, col) in enumerate(zip(args, filter_list)):
        print(i, arg, col)
        if arg:
            filter.append(arg)
        else:
            filter.append(df_[col].unique())

    query = "(`Start Time` >= @date_start_filter) & (`Start Time` <= @date_end_filter) "
    df_["Start Time"] = df_["Start Time"].dt.date
    for i, elm in enumerate(filter_list):
        query += f"& (`{elm}` in @filter[{i}]) "
    query = query.rstrip()
    df_ = df_.query(query)

    if len(df_.index) == 0:
        st.warning("Filters result in empty DataFrame. Change the filters!")
    else:
        col1, col2, col3, col4 = st.columns(CHART_COL_NUMBER)

        with col1:
            avail_2g = (df_
                        .groupby("Start Time").agg({"AVAILABILITY 2G NUM":"sum", "AVAILABILITY 2G DENUM":"sum"})
                        .reset_index()
                        .assign(avail2g=lambda df_: df_["AVAILABILITY 2G NUM"]/df_["AVAILABILITY 2G DENUM"]))
            fig = px.line(avail_2g, x="Start Time", y="avail2g", title="Availability 2G", markers=True)
            st.plotly_chart(fig, theme="streamlit", use_container_width=True)

            ifho = (df_
                    .groupby("Start Time").agg({"Num IFHO SR NFJ":"sum", "Denum IFHO SR NFJ":"sum"})
                    .reset_index()
                    .assign(IFHO=lambda df_: df_["Num IFHO SR NFJ"]/df_["Denum IFHO SR NFJ"]))
            fig = px.line(ifho, x="Start Time", y="IFHO", title="IFHO (%)", markers=True)
            st.plotly_chart(fig, theme="streamlit", use_container_width=True)

            erab_drop = (df_
                         .groupby("Start Time").agg({"Num E-RAB Drop Rate NFJ":"sum", "Denum E-RAB Drop Rate NFJ":"sum"})
                         .reset_index()
                         .assign(erabdrop=lambda df_:df_["Num E-RAB Drop Rate NFJ"]/df_["Denum E-RAB Drop Rate NFJ"]))
            fig = px.line(erab_drop, x="Start Time", y="erabdrop", title="E-RAB Drop (%)", markers=True)
            st.plotly_chart(fig, theme="streamlit", use_container_width=True)

            bts_count = (df_
                         .groupby("Start Time").agg({"ManagedElement":"count"})
                         .reset_index())
            fig = px.line(bts_count, x="Start Time", y="ManagedElement", title="BTS Count", markers=True)
            st.plotly_chart(fig, theme="streamlit", use_container_width=True)

            tranpsort = (df_
                     .groupby("Start Time").agg({"Received Speed(Kbps)":"mean", "Send Speed(Kbps)":"mean"})
                     .reset_index())
            fig = px.line(tranpsort, x="Start Time", y=["Received Speed(Kbps)", "Send Speed(Kbps)"], title="Transport Received (DL) vs Send (UL)", markers=True)
            st.plotly_chart(fig, theme="streamlit", use_container_width=True)

        with col2:
            payload_ul_dl_tb = (df_.assign(payload_dl_tb=lambda df_: df_["Payload DL (MB)"] / 1048576,
                                        payload_ul_tb=lambda df_: df_["Payload UL (MB)"] / 1048576)
                                .groupby("Start Time").agg({"payload_dl_tb":"sum", "payload_ul_tb":"sum"})
                                .reset_index())
            fig = px.line(payload_ul_dl_tb, x="Start Time", y=["payload_dl_tb", "payload_ul_tb"], title="Total Payload (TB)", markers=True)
            st.plotly_chart(fig, theme="streamlit", use_container_width=True)

            rrc_est = (df_
                       .groupby("Start Time").agg({"Num RRC Setup SR NFJ":"sum", "Denum RRC Setup SR NFJ":"sum"})
                       .reset_index()
                       .assign(rrc_est_sr=lambda df_: df_["Num RRC Setup SR NFJ"]/df_["Denum RRC Setup SR NFJ"])) 
            fig = px.line(rrc_est, x="Start Time", y="rrc_est_sr", title="RRC Est SR (%)", markers=True)
            st.plotly_chart(fig, theme="streamlit", use_container_width=True)

            csfb_prep = (df_
                         .groupby("Start Time").agg({"Num CSFB SR NFJ":"sum", "Denum CSFB SR NFJ":"sum"})
                         .reset_index()
                         .assign(csfbprep=lambda df_: df_["Num CSFB SR NFJ"] / df_["Denum CSFB SR NFJ"]))
            fig = px.line(csfb_prep, x="Start Time", y="csfbprep", title="CSFB Prep SR (%)", markers=True)
            st.plotly_chart(fig, theme="streamlit", use_container_width=True)

            s1_signal = (df_
                         .groupby("Start Time").agg({"S1 Signaling SR (NF) Num":"sum", "S1 Signaling SR (NF) Denum":"sum"})
                         .reset_index()
                         .assign(s1signal=lambda df_: df_["S1 Signaling SR (NF) Num"] / df_["S1 Signaling SR (NF) Denum"]))
            fig = px.line(s1_signal, x="Start Time", y="s1signal", title="S1 Signalling (%)", markers=True)
            st.plotly_chart(fig, theme="streamlit", use_container_width=True)
        
        with col3:
            access = (df_.groupby("Start Time").agg({"Num E-RAB Setup SR NFJ":"sum", "Num RRC Setup SR NFJ":"sum", "Denum E-RAB Setup SR NFJ":"sum", "Denum RRC Setup SR NFJ":"sum"})
                        .reset_index()
                        .assign(accessibility=lambda df_: (df_["Num E-RAB Setup SR NFJ"]+df_["Num RRC Setup SR NFJ"]) / (df_["Denum E-RAB Setup SR NFJ"]+df_["Denum RRC Setup SR NFJ"])))
            fig = px.line(access, x="Start Time", y="accessibility", title="Accessibility (%)", markers=True)
            st.plotly_chart(fig, theme="streamlit", use_container_width=True)

            erab_sr = (df_
                       .groupby("Start Time").agg({"Num E-RAB Setup SR NFJ":"sum", "Denum E-RAB Setup SR NFJ":"sum"})
                       .reset_index()
                       .assign(erabsr=lambda df_: df_["Num E-RAB Setup SR NFJ"]/df_["Denum E-RAB Setup SR NFJ"])) 
            fig = px.line(erab_sr, x="Start Time", y="erabsr", title="E-RAB SR (%)", markers=True)
            st.plotly_chart(fig, theme="streamlit", use_container_width=True)

            dl_prb = (df_
                      .groupby("Start Time").agg({"DL PRB Utilization (%) NFJ Num":"sum", "DL PRB Utilization (%) NFJ Denum":"sum"})
                      .reset_index()
                      .assign(dlprb=lambda df_: df_["DL PRB Utilization (%) NFJ Num"] / df_["DL PRB Utilization (%) NFJ Denum"]))
            fig = px.line(dl_prb, x="Start Time", y="dlprb", title="DL PRB (%)", markers=True)
            st.plotly_chart(fig, theme="streamlit", use_container_width=True)

            zero_ava = (df_
                        .groupby("Start Time").agg({"Zero Avail":"sum"})
                        .reset_index())
            fig = px.line(zero_ava, x="Start Time", y="Zero Avail", title="Total Zero Availability", markers=True)
            st.plotly_chart(fig, theme="streamlit", use_container_width=True)


        with col4:
            retain = (df_
                    .groupby("Start Time").agg({"Denum E-RAB Drop Rate NFJ":"sum", "Num E-RAB Drop Rate NFJ":"sum"})
                    .reset_index()
                    .assign(retainability=lambda df_: (df_["Denum E-RAB Drop Rate NFJ"]-df_["Num E-RAB Drop Rate NFJ"]) / df_["Denum E-RAB Drop Rate NFJ"]))
            fig = px.line(retain, x="Start Time", y="retainability", title="Retainability (%)", markers=True)
            st.plotly_chart(fig, theme="streamlit", use_container_width=True)

            rrc_user = (df_
                        .groupby("Start Time").agg({"[LTE]RRCConnectedUserLicenseUtilization Num_ranq":"sum"})
                        .reset_index())
            fig = px.line(rrc_user, x="Start Time", y="[LTE]RRCConnectedUserLicenseUtilization Num_ranq", title="RRC User", markers=True)
            st.plotly_chart(fig, theme="streamlit", use_container_width=True)

            good_cqi = (df_
                        .groupby("Start Time").agg({"CQI>=7 Num":"sum", "CQI>=7 Denum":"sum"})
                        .reset_index()
                        .assign(goodcqi=lambda df_: df_["CQI>=7 Num"] / df_["CQI>=7 Denum"]))
            fig = px.line(good_cqi, x="Start Time", y="goodcqi", title="Good CQI (%)", markers=True)
            st.plotly_chart(fig, theme="streamlit", use_container_width=True)

            pl_lat_jit_1 = (df_
                            .groupby("Start Time").agg({"Packet Loss Rate (dst 1st)":"mean", "Avg Time Delay(ms)  (dst 1st)":"mean", "Avg Delay Jitter(ms)  (dst 1st)":"mean"})
                            .reset_index())
            fig = px.line(pl_lat_jit_1, x="Start Time", y=["Packet Loss Rate (dst 1st)", "Avg Time Delay(ms)  (dst 1st)", "Avg Delay Jitter(ms)  (dst 1st)"], title="PL(%) - Latency(ms) - Jitter(ms)", markers=True)
            st.plotly_chart(fig, theme="streamlit", use_container_width=True)

            pl_lat_jit_2 = (df_
                .groupby("Start Time").agg({"Packet Loss Rate (dst 2nd)":"mean", "Avg Time Delay(ms)  (dst 2nd)":"mean", "Avg Delay Jitter(ms)  (dst 2nd)":"mean"})
                .reset_index())
            fig = px.line(pl_lat_jit_2, x="Start Time", y=["Packet Loss Rate (dst 2nd)", "Avg Time Delay(ms)  (dst 2nd)", "Avg Delay Jitter(ms)  (dst 2nd)"], title="PL(%) - Latency(ms) - Jitter(ms)", markers=True)
            st.plotly_chart(fig, theme="streamlit", use_container_width=True)

        st.success(f"Plot Berhasil Dibuat!")

@st.cache_data
def create_filter_list(df_):
    min_date = df_["Start Time"].min()
    max_date = df_["Start Time"].max()
    cell_name_list = df_["Cell Name"].unique()
    vendor_lc_list = df_["Vendor LC"].unique()
    vendor_gs_list = df_["Vendor GS"].unique()
    cluster_list = df_["Cluster"].unique()
    subnetwork_name_list = df_["Subnetwork Name"].unique()
    spotbeam_list = df_["Spotbeam"].unique()
    project_list = df_["PROJECT"].unique()
    technology_colo_list = df_["TECHNOLOGY COLO"].unique()
    days_per_week_list = df_["Days per Week"].unique()
    bts_vendor_list = df_["BTS VENDOR"].unique()
    regional_list = df_["REGIONAL"].unique()
    return (min_date, max_date, cell_name_list, vendor_lc_list, vendor_gs_list, cluster_list, subnetwork_name_list, spotbeam_list, 
            project_list, technology_colo_list, days_per_week_list, bts_vendor_list, regional_list)

def create_sidebar_filter(df_, min_date, max_date, cell_name_list, vendor_lc_list, vendor_gs_list, cluster_list, subnetwork_name_list, spotbeam_list, 
            project_list, technology_colo_list, days_per_week_list, bts_vendor_list, regional_list):
    # print(df_["Start Time"].min())
    # print(type(df_["Start Time"].min()))
    with st.sidebar.form("filter_form_sidebar"):
        date_start_filter = st.date_input("Start Time", key="date_start", value=max_date, min_value=min_date, max_value=max_date)
        date_end_filter = st.date_input("End Time", key="date_end", value=max_date, min_value=min_date, max_value=max_date)
        # date_slider = st.slider("Date", value=(df_["Start Time"].min(), df_["Start Time"].max()), step=timedelta(days=1))
        cell_name = st.multiselect("Cell Name", options=cell_name_list)
        vendor_lc = st.multiselect("Vendor LC", key="vendor_lc", options=vendor_lc_list)
        vendor_gs = st.multiselect("Vendor GS", options=vendor_gs_list)
        cluster = st.multiselect("Cluster", options=cluster_list)
        subnetwork_name = st.multiselect("Subnetwork Name", options=subnetwork_name_list)
        spotbeam = st.multiselect("Spotbeam", options=spotbeam_list)
        project = st.multiselect("Project", options=project_list)
        technology_colo = st.multiselect("Technology COLO", options=technology_colo_list)
        days_per_week = st.multiselect("Days per Week", options=days_per_week_list)
        bts_vendor = st.multiselect("BTS Vendor", options=bts_vendor_list)
        regional = st.multiselect("Regional", options=regional_list)
        # text = st.text_input("Test") 
        # st.text(f"{st.session_state.date_start} {st.session_state.date_end} {st.session_state.vendor_lc}")
        filter_button = st.form_submit_button("Plot")
    if filter_button:
        plot(df_, date_start_filter, date_end_filter, cell_name, vendor_lc, vendor_gs, cluster, 
            subnetwork_name, spotbeam, project, technology_colo, days_per_week, bts_vendor, regional)

def page_header():
    st.title("4G Dashboard")
    st.markdown("---")

def main():
    # PAGE HEADER
    page_header()
    # IMPORT FILE
    df = import_files(file_path="data/4G DASHBOARD_DAILY_NPI USO_2023.csv")

    # REPLACE "", "NIL", AND "#N/A" wtih np.nan
    df = df.replace(["", "NIL", "#N/A", "0x2a", "-"], np.nan)

    # FIX COLUMNS WITH PERCENTAGE
    columns_with_percentage = ["[FDD]Cell Availability",
                               "S1 Signaling SR (NF)",
                               "RRC Setup SR (%) NFJ",
                               "CSSR (%) NFJ", "E-RAB Setup SR (%) NFJ_1508983807242-6",
                               "E-RAB Drop Rate (%) NFJ_1508918825286-9",
                               "Ratio of RRC Re-establishment _monitor",
                               "IFHO SR (%) NFJ",
                               "CSFB SR (%) NFJ",
                               "[LTE]S1-Signal Connection Establishment Success Rate",
                               "DL PRB Utilization (%) NFJ",
                               "UL PRB Utilization (%) NFJ",
                               "[LTE]DL 64QAM Modulation Scheme Usage",
                               "[LTE]DL 16QAM Modulation Scheme Usage",
                               "the ratio of  CQI>=7_monitor",
                               "DLResourceBlockUtilizingRate",
                               "ULResourceBlockUtilizingRate",
                               "[FDD]PDCCH CCE Utilization Rate",
                               "[FDD]PUSCH PRB Utilization Rate",
                               "[LTE]PRACH Usage",
                               "Paging Congestion Rate",
                               "paging success rate",
                               "eNB Paging Success Rate",
                               "[LTE]Paging Channel Usage",
                               "RadioNetworkAvailabilityRate",
                               "CQI>=7_(%)",
                               "AVAILABILITY 2G",
                               "Packet Loss Rate (dst 1st)",
                               "Packet Loss Rate (dst 2nd)"
                               ]
    for col in columns_with_percentage:
        df[col] = fix_columns_with_percentage(df[col])

    # FIX COLUMNS WITH DATE
    for col in ["Start Time", "End Time"]:
        df[col] = fix_date_columns(df[col])

    # FIX COLUMNS NEEDED FOR CHARTS
    columns_used_in_charts = ["AVAILABILITY 2G NUM",
                              "AVAILABILITY 2G DENUM",
                              "4G Payload (MByte) NFJ",
                              "Num CSFB SR NFJ",
                              "Denum CSFB SR NFJ",
                              "Num E-RAB Drop Rate NFJ",
                              "Denum E-RAB Drop Rate NFJ",
                              "Num E-RAB Setup SR NFJ",
                              "Denum E-RAB Setup SR NFJ",
                              "Num RRC Setup SR NFJ",
                              "Denum RRC Setup SR NFJ",
                              "S1 Signaling SR (NF) Num",
                              "S1 Signaling SR (NF) Denum",
                              "Num IFHO SR NFJ",
                              "Denum IFHO SR NFJ",
                              "CQI>=7 Num",
                              "CQI>=7 Denum",
                              "DL PRB Utilization (%) NFJ Num",
                              "DL PRB Utilization (%) NFJ Denum",
                              "Payload UL (MB)",
                              "Payload DL (MB)",
                              "Cell Availability Num 4G",
                              "Cell Availability Denum 4G"]
    for col in columns_used_in_charts:
        df[col] = clean_used_columns(df[col])
    
    # CREATE SIDEBAR FILTER
    create_sidebar_filter(df, *create_filter_list(df))

main()