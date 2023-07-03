import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import streamlit as st
import plotly.express as px

# PRESETS AND CONSTANTS
pd.options.plotting.backend = "plotly"
st.set_page_config(layout="wide")
CHART_COL_NUMBER = 3

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

@st.cache_data
def plot(df_, date_start_filter, date_end_filter, *args):
    filter_list = ["BTS NAME", "Vendor LC", "Vendor GS", "Cluster", "SUBNETWORK Name", "Spotbeam", "PROJECT", "TECHNOLOGY COLO", "Days per Week", "BTS VENDOR", "REGIONAL", "DESA"]
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
        col1, col2, col3 = st.columns(CHART_COL_NUMBER)

        with col1:
            bts_count = (df_
                        .groupby("Start Time").agg({"BTS NAME":"count"})
                        .reset_index())
            fig = px.line(bts_count, x="Start Time", y="BTS NAME", title="Count of Cell", markers=True)
            st.plotly_chart(fig, theme="streamlit", use_container_width=True)

            total_payload = (df_
                         .groupby("Start Time").agg({"2G Total Payload":"sum"})
                         .reset_index())
            fig = px.line(total_payload, x="Start Time", y="2G Total Payload", title="Total Payload (MB)", markers=True)
            st.plotly_chart(fig, theme="streamlit", use_container_width=True)

            sdsr = (df_
                    .groupby("Start Time").agg({"Num SDSR":"sum", "Denum SDSR":"sum"})
                    .reset_index()
                    .assign(sdsr_=lambda df_: df_["Num SDSR"] / df_["Denum SDSR"]))
            fig = px.line(sdsr, x="Start Time", y="sdsr_", title="SDSR (%)", markers=True)
            st.plotly_chart(fig, theme="streamlit", use_container_width=True)

            hosr = (df_
                    .groupby("Start Time").agg({"Num HOSR":"sum", "Denum HOSR":"sum"})
                    .reset_index()
                    .assign(hosr_=lambda df_: df_["Num HOSR"] / df_["Denum HOSR"]))
            fig = px.line(hosr, x="Start Time", y="hosr_", title="HOSR (%)", markers=True)
            st.plotly_chart(fig, theme="streamlit", use_container_width=True)

            tbf_dl_sr = (df_
                     .groupby("Start Time").agg({"Num TBF DL SR":"sum", "Denum TBF DL SR":"sum"})
                     .reset_index()
                     .assign(tbf_dl_sr_=lambda df_: df_["Num TBF DL SR"] / df_["Denum TBF DL SR"]))
            fig = px.line(tbf_dl_sr, x="Start Time", y="tbf_dl_sr_", title="TBF Est DL SR (%)", markers=True)
            st.plotly_chart(fig, theme="streamlit", use_container_width=True)

            transport = (df_
                     .groupby("Start Time").agg({"Received Speed(Kbps)":"mean", "Send Speed(Kbps)":"mean"})
                     .reset_index())
            fig = px.line(transport, x="Start Time", y=["Received Speed(Kbps)", "Send Speed(Kbps)"], title="Tranport Received (DL) vs Send (UL)", markers=True)
            st.plotly_chart(fig, theme="streamlit", use_container_width=True)

            pd_pl = (df_
                     .groupby("Start Time").agg({"Number of sent path-detection request packets":"sum", "Number of replies received in the watch time":"sum"})
                     .reset_index()
                     .assign(pd_pl_=lambda df_: (df_["Number of sent path-detection request packets"] - df_["Number of replies received in the watch time"]) / df_["Number of sent path-detection request packets"]))
            fig = px.line(pd_pl, x="Start Time", y="pd_pl_", title="PD Packet Loss (%)", markers=True)
            st.plotly_chart(fig, theme="streamlit", use_container_width=True)

        with col2:
            avail = (df_
                     .groupby("Start Time").agg({"Num TCH Available":"sum", "Denum TCH Available":"sum"})
                     .reset_index()
                     .assign(avails=lambda df_: df_["Num TCH Available"] / df_["Denum TCH Available"]))
            fig = px.line(avail, x="Start Time", y="avails", title="Availability (%)", markers=True)
            st.plotly_chart(fig, theme="streamlit", use_container_width=True)

            trx_num = (df_
                       .groupby("Start Time").agg({"Number of TRX":"sum"})
                       .reset_index()) 
            fig = px.line(trx_num, x="Start Time", y="Number of TRX", title="Number TRx", markers=True)
            st.plotly_chart(fig, theme="streamlit", use_container_width=True)

            sd_block = (df_
                         .groupby("Start Time").agg({"Num SD Blocking Rate":"sum", "Denum SD Blocking Rate":"sum"})
                         .reset_index()
                         .assign(sd_block_=lambda df_: df_["Num SD Blocking Rate"] / df_["Denum SD Blocking Rate"]))
            fig = px.line(sd_block, x="Start Time", y="sd_block_", title="SD BLOCK (%)", markers=True)
            st.plotly_chart(fig, theme="streamlit", use_container_width=True)

            tch_drop = (df_
                         .groupby("Start Time").agg({"Num TCH Drop Rate":"sum", "Denum TCH Drop Rate":"sum"})
                         .reset_index()
                         .assign(tch_drop_=lambda df_: df_["Num TCH Drop Rate"] / df_["Denum TCH Drop Rate"]))
            fig = px.line(tch_drop, x="Start Time", y="tch_drop_", title="TCH DROP (%)", markers=True)
            st.plotly_chart(fig, theme="streamlit", use_container_width=True)

            tbf_ul_sr = (df_
                     .groupby("Start Time").agg({"Num TBF UL SR":"sum", "Denum TBF UL SR":"sum"})
                     .reset_index()
                     .assign(tbf_ul_sr_=lambda df_: df_["Num TBF UL SR"] / df_["Denum TBF UL SR"]))
            fig = px.line(tbf_ul_sr, x="Start Time", y="tbf_ul_sr_", title="TBF Est UL SR (%)", markers=True)
            st.plotly_chart(fig, theme="streamlit", use_container_width=True)

            retain = (df_
                      .groupby("Start Time").agg({"Num TBF Comp SR":"sum", "Denum TCH Drop Rate":"sum", "Num TCH Drop Rate":"sum", "Denum TBF Comp SR":"sum"})
                      .reset_index()
                      .assign(retain_=lambda df_: (df_["Num TBF Comp SR"] + df_["Denum TCH Drop Rate"] - df_["Num TCH Drop Rate"]) /
                                                   (df_["Denum TBF Comp SR"] + df_["Denum TCH Drop Rate"])))
            fig = px.line(retain, x="Start Time", y="retain_", title="Retainability (%)", markers=True)
            st.plotly_chart(fig, theme="streamlit", use_container_width=True)

            lat_jit = (df_
                       .groupby("Start Time").agg({"Mean round-trip delay(ms)":"mean", "Mean delay jitter(ms)":"mean"})
                       .reset_index())
            fig = px.line(lat_jit, x="Start Time", y=["Mean round-trip delay(ms)", "Mean delay jitter(ms)"], title="Latency(ms) - Jitter(ms)", markers=True)
            st.plotly_chart(fig, theme="streamlit", use_container_width=True)
        
        with col3:
            tch_sd = (df_
                      .groupby("Start Time").agg({"TCH Traffic (erl)":"sum", "SDCCH Traffic (erl)":"sum"})
                      .reset_index())
            fig = px.line(tch_sd, x="Start Time", y=["TCH Traffic (erl)", "SDCCH Traffic (erl)"], title="TCH and SD Traffic (Erl)", markers=True)
            st.plotly_chart(fig, theme="streamlit", use_container_width=True)

            sdcch_seizure = (df_
                       .groupby("Start Time").agg({"Number of SDCCH seizure attempts for assignment(MOC)":"sum", "Number of SDCCH seizure attempts for assignment(MTC)":"sum", "Number of SDCCH seizure attempts for assignment(LOC)":"sum"})
                       .reset_index()) 
            fig = px.line(sdcch_seizure, x="Start Time",
                          y=["Number of SDCCH seizure attempts for assignment(MOC)", "Number of SDCCH seizure attempts for assignment(MTC)", "Number of SDCCH seizure attempts for assignment(LOC)"],
                          title="SDCCH Seizure Att", markers=True)
            st.plotly_chart(fig, theme="streamlit", use_container_width=True)

            tch_block = (df_
                      .groupby("Start Time").agg({"Num TCH Blocking Rate":"sum", "Denum TCH Blocking Rate":"sum"})
                      .reset_index()
                      .assign(tch_block_=lambda df_: df_["Num TCH Blocking Rate"] / df_["Denum TCH Blocking Rate"]))
            fig = px.line(tch_block, x="Start Time", y="tch_block_", title="TCH BLOCK (%)", markers=True)
            st.plotly_chart(fig, theme="streamlit", use_container_width=True)

            tbf_comp_sr = (df_
                        .groupby("Start Time").agg({"Num TBF Comp SR":"sum", "Denum TBF Comp SR":"sum"})
                        .reset_index()
                        .assign(tbf_comp_sr_=lambda df_: df_["Num TBF Comp SR"] / df_["Denum TBF Comp SR"]))
            fig = px.line(tbf_comp_sr, x="Start Time", y="tbf_comp_sr_", title="TBF COMP SR (%)", markers=True)
            st.plotly_chart(fig, theme="streamlit", use_container_width=True)

            zero_avail = (df_
                       .groupby(["Start Time", "PROJECT"]).agg({"ZeroAvail":"sum"})
                       .reset_index()) 
            fig = px.line(zero_avail, x="Start Time", y="ZeroAvail", title="Total of Zero Availability", color="PROJECT", markers=True)
            st.plotly_chart(fig, theme="streamlit", use_container_width=True)

            access = (df_
                      .groupby("Start Time").agg({"Num SDSR":"sum", "Num TCH Blocking Rate":"sum", "Num TBF DL SR":"sum", "Denum SDSR":"sum", "Denum TBF DL SR":"sum", "Denum TCH Blocking Rate":"sum"})
                      .reset_index()
                      .assign(access_=lambda df_: (df_["Num SDSR"] + df_["Num TCH Blocking Rate"] + df_["Num TBF DL SR"]) /
                                                   (df_["Denum SDSR"] + df_["Denum TCH Blocking Rate"] + df_["Denum TBF DL SR"])))
            fig = px.line(access, x="Start Time", y="access_", title="Accessibility (%)", markers=True)
            st.plotly_chart(fig, theme="streamlit", use_container_width=True)

            revenue = (df_.groupby("Start Time").agg({"REVENUE(IDR)":"sum"}).reset_index())
            fig = px.line(revenue, x="Start Time", y="REVENUE(IDR)", title="Revenue (IDR)", markers=True)
            st.plotly_chart(fig, theme="streamlit", use_container_width=True)        

        st.success(f"Plot Berhasil Dibuat!")

@st.cache_data
def create_filter_list(df_):
    min_date = df_["Start Time"].min()
    max_date = df_["Start Time"].max()
    cell_name_list = df_["BTS NAME"].unique()
    vendor_lc_list = df_["Vendor LC"].unique()
    vendor_gs_list = df_["Vendor GS"].unique()
    cluster_list = df_["Cluster"].unique()
    subnetwork_name_list = df_["SUBNETWORK Name"].unique()
    spotbeam_list = df_["Spotbeam"].unique()
    project_list = df_["PROJECT"].unique()
    technology_colo_list = df_["TECHNOLOGY COLO"].unique()
    days_per_week_list = df_["Days per Week"].unique()
    bts_vendor_list = df_["BTS VENDOR"].unique()
    regional_list = df_["REGIONAL"].unique()
    desa_list = df_["DESA"].unique()
    return (min_date, max_date, cell_name_list, vendor_lc_list, vendor_gs_list, cluster_list, subnetwork_name_list, spotbeam_list, 
            project_list, technology_colo_list, days_per_week_list, bts_vendor_list, regional_list, desa_list)

def create_sidebar_filter(df_, min_date, max_date, cell_name_list, vendor_lc_list, vendor_gs_list, cluster_list, subnetwork_name_list, spotbeam_list, 
            project_list, technology_colo_list, days_per_week_list, bts_vendor_list, regional_list, desa_list):
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
        desa = st.multiselect("Desa", options=desa_list)
        # text = st.text_input("Test") 
        # st.text(f"{st.session_state.date_start} {st.session_state.date_end} {st.session_state.vendor_lc}")
        filter_button = st.form_submit_button("Plot")
    if filter_button:
        plot(df_, date_start_filter, date_end_filter, cell_name, vendor_lc, vendor_gs, cluster, 
            subnetwork_name, spotbeam, project, technology_colo, days_per_week, bts_vendor, regional, desa)

def page_header():
    st.title("2G Dashboard")
    st.markdown("---")

def main():
    # PAGE HEADER
    page_header()
    # IMPORT FILE
    df = import_files(file_path="data/2G DASHBOARD_DAILY_NPI USO_2023.csv")

    # REPLACE "", "NIL", AND "#N/A" wtih np.nan
    df = df.replace(["", "NIL", "#N/A", "0x2a", "-", r"^\s*$"], np.nan, regex=True)
    # for col in df.columns:
    #     df[col] = df[col].str.replace("^\s*$", np.nan, regex=True)

    # FIX COLUMNS WITH PERCENTAGE
    columns_with_percentage = ["TCH Available",
                               "SDSR",
                               "HOSR",
                               "SD Blocking Rate",
                               "TCH Drop Rate",
                               "TBF DL SR",
                               "TBF UL SR",
                               "TBF Comp Rate",
                               "TCH Blocking Rate",
                               "Interference ICM [0-2]",
                               "Interference ICM [3-5]",
                               "PD Packet Loss Rate (%)",
                               ]
    for col in columns_with_percentage:
        df[col] = fix_columns_with_percentage(df[col])

    # FIX COLUMNS WITH DATE
    for col in ["Start Time", "End Time"]:
        df[col] = fix_date_columns(df[col])

    # FIX COLUMNS NEEDED FOR CHARTS
    columns_used_in_charts = ["Num TCH Available",
                              "Denum TCH Available",
                              "Num SDSR",
                              "Denum SDSR",
                              "Num HOSR",
                              "Denum HOSR",
                              "Num SD Blocking Rate",
                              "Denum SD Blocking Rate",
                              "Num TCH Drop Rate",
                              "Denum TCH Drop Rate",
                              "Num TBF DL SR",
                              "Denum TBF DL SR",
                              "Num TBF UL SR",
                              "Denum TBF UL SR",
                              "Num TBF Comp SR",
                              "Denum TBF Comp SR",
                              "Num TCH Blocking Rate",
                              "Denum TCH Blocking Rate",
                              "TCH Traffic (erl)",
                              "SDCCH Traffic (erl)",
                              "2G Total Payload",
                              "Number of sent path-detection request packets",
                              "Number of replies received in the watch time",
                              "Number of TRX",
                              "Number of SDCCH seizure attempts for assignment(MOC)",
                              "Number of SDCCH seizure attempts for assignment(MTC)",
                              "Number of SDCCH seizure attempts for assignment(LOC)",
                              "Mean round-trip delay(ms)",
                              "Mean delay jitter(ms)",
                              "Received Speed(Kbps)",
                              "Send Speed(Kbps)",
                              "REVENUE(IDR)"
                              ]
    for col in columns_used_in_charts:
        df[col] = clean_used_columns(df[col])
    
    # CREATE SIDEBAR FILTER
    create_sidebar_filter(df, *create_filter_list(df))

main()