import os
import streamlit as st
import altair as alt
import pandas as pd
import mongodb_ops as db_ops  # Importing the database operations script
from dotenv import load_dotenv

#load environment variables from .env file
load_dotenv()

# MongoDB Atlas credentials and connection details
username = os.environ.get('DB_USERNAME')
password = os.environ.get('DB_PASSWORD')
cluster_url = 'covidstates.chotlpx.mongodb.net'
database_name = 'CovidStatesDB'
collection_name1 = 'StatesDiff'
collection_name2 = 'StatesPop'

# Connecting to the database
client = db_ops.connect_to_mongodb(username, password, cluster_url, database_name)

if 'AllState' not in st.session_state:
    unique_states = db_ops.get_unique_states(client, database_name, collection_name2)
    st.session_state['AllState'] = unique_states

if 'MonthYear' not in st.session_state:
    month_year_list = db_ops.get_month_year_timeline(client, database_name, collection_name1)
    st.session_state['MonthYear'] = month_year_list

with st.form("inputs"):
    selected_states = st.multiselect('Select State(s)', st.session_state['AllState'], default=[st.session_state['AllState'][0]], max_selections=10)
    date_range = st.select_slider('Select Date Range', options=st.session_state['MonthYear'], value=(st.session_state['MonthYear'][0], st.session_state['MonthYear'][-1]))
    data_types = st.multiselect('Select Data Type', ['cases', 'deaths', 'cases per 100k', 'deaths per 100k'], default=['cases per 100k', 'deaths per 100k'])
    submitted = st.form_submit_button("Generate Plot")

def create_plot(selected_states, start_date, end_date, data_types):
    filtered_data = db_ops.query_data(client, database_name, collection_name1, selected_states, start_date, end_date)

    # Creating a combined 'Month-Year' column for plotting
    filtered_data['MonthYear'] = [pd.to_datetime(date_str, format='%Y %m').strftime('%b %Y') for date_str in filtered_data['YearMonth']]

    # Base chart for shared properties
    base = alt.Chart(filtered_data).encode(
        x=alt.X('MonthYear:N', sort=alt.SortField(field='YearMonth', order='ascending'))
    )

    # Cases line chart
    cases_chart = base.mark_line().encode(
        y=alt.Y('Total_Cases:Q', axis=alt.Axis(title='Cases')),
        color='State:N'  # Encode color by State
    )

    # Deaths line chart
    deaths_chart = base.mark_line().encode(
        y=alt.Y('Total_Deaths:Q', axis=alt.Axis(title='Deaths')),
        color='State:N'  # Encode color by State
    )

    # Cases/100k line chart
    cases100k_chart = base.mark_line().encode(
        y=alt.Y('Cases_per_100k:Q', axis=alt.Axis(title='Cases per 100k')),
        color='State:N'  # Encode color by State
    )

    # Deaths line chart
    deaths100k_chart = base.mark_line().encode(
        y=alt.Y('Deaths_per_100k:Q', axis=alt.Axis(title='Deaths per 100k')),
        color='State:N'  # Encode color by State
    )
    # Filter based on selected data types
    if len(selected_states) == 1 and 'cases per 100k' in data_types and 'deaths per 100k' in data_types:
        
        # Fold the data
        folded_data = base.transform_fold(
            ['Cases_per_100k', 'Deaths_per_100k'],
            as_=['Category', 'Value']
        )

        # Create line chart
        chart = folded_data.mark_line().encode(
            y=alt.Y('Value:Q', axis=alt.Axis(title='Count per 100k')),
            color=alt.Color('Category:N', legend=alt.Legend(title='Category')),
            tooltip=['Category:N', 'Value:Q']
        ).resolve_scale(
            y='independent'
        )
        
        st.altair_chart(chart, use_container_width=True)

    if 'cases' in data_types:
        st.altair_chart(cases_chart, use_container_width=True)
    if 'deaths' in data_types:
        st.altair_chart(deaths_chart, use_container_width=True)
    if 'cases per 100k' in data_types:
        st.altair_chart(cases100k_chart, use_container_width=True)
    if 'deaths per 100k' in data_types:
        st.altair_chart(deaths100k_chart, use_container_width=True)

if submitted and len(selected_states) > 0:
    start_date_str, end_date_str = date_range
    start_date = pd.to_datetime(start_date_str)
    end_date = pd.to_datetime(end_date_str) + pd.DateOffset(months=1)

    create_plot(selected_states, start_date, end_date, data_types)

# Close the MongoDB connection
db_ops.close_mongodb_connection(client)
