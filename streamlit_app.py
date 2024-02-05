from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import lit, col, date_add, current_date, year, monthname, to_date 
from snowflake.snowpark.types import DecimalType
import altair as alt
import streamlit as st

# Set page config
st.set_page_config(layout="wide")

# Get current session
session = get_active_session()

st.header(":lightning_cloud: Weather and Environmental State Explorer")

database_name = 'WEATHER__ENVIRONMENTAL_ESSENTIALS'
schema_name = 'CYBERSYN'

# load and cache state names
@st.cache_data()
def load_states():
    return (session.table(f"{database_name}.{schema_name}.geography_index")
                   .filter(col('LEVEL') == 'State')
                   .select(col('GEO_NAME'))
                   .sort(col('GEO_NAME'))      
            ).to_pandas()

states = load_states()

# create selectbox for user interaction
selected_state = st.selectbox("Select State", states)

@st.cache_data()
def load_data(filter_state: str):

    # use weather metrics data for daily precipitation
    weather_metrics_ts = session.table(f"{database_name}.{schema_name}.noaa_weather_metrics_timeseries")
    weather_stations = session.table(f"{database_name}.{schema_name}.noaa_weather_station_index")

    metrics_and_stations = (weather_metrics_ts.join(weather_stations, on="noaa_weather_station_id", how="inner")
                                              .filter(col("state_name") == filter_state)
                           )

    daily_precip = (metrics_and_stations.filter(col('VARIABLE') == 'precipitation')
                                        .filter(col('DATE') >= date_add(current_date(), -365))
                                        .group_by(col('DATE'))
                                        .avg(col('VALUE'))
                                        .select(col("DATE"), col('AVG(VALUE)').cast(DecimalType(16,2)).alias('AVG_PRECIPITATION'))
                                        .sort('DATE')
                    )

    #severe weather stats using SQL
    severe_weather = session.sql(f'''SELECT
                                        YEAR(ts.date) AS year,
                                        COUNT(DISTINCT ts.date) AS count_severe_weather_days
                                    FROM {database_name}.{schema_name}.noaa_weather_metrics_timeseries AS ts
                                    JOIN {database_name}.{schema_name}.noaa_weather_station_index AS idx
                                        ON (ts.noaa_weather_station_id = idx.noaa_weather_station_id)
                                    WHERE 
                                        ts.variable_name = 'Weather Type: Tornado, Waterspout, or Funnel Cloud'
                                        AND idx.state_name = '{filter_state}'
                                        AND ts.value = 1
                                        AND ts.date >= '2010-01-01'
                                    GROUP BY year
                                    ORDER BY year
                                ''')
    flood_claim_index = session.table(f"{database_name}.{schema_name}.fema_national_flood_insurance_program_claim_index")
    geo_index = session.table(f"{database_name}.{schema_name}.geography_index").filter(col("geo_name") == filter_state)
    
    insurance_claims = (flood_claim_index
                               .filter(col('DATE_OF_LOSS') >= to_date(lit('2010-01-01')))
                               .join(geo_index, flood_claim_index.state_geo_id == geo_index.geo_id, how="inner")
                               .group_by([year(col("DATE_OF_LOSS")), monthname(col("DATE_OF_LOSS"))])
                               .sum(col('BUILDING_DAMAGE_AMOUNT'), col('CONTENTS_DAMAGE_AMOUNT'))
                               .rename({col('YEAR(DATE_OF_LOSS)'): 'YEAR_OF_LOSS',
                                        col('MONTHNAME(DATE_OF_LOSS)'): 'MONTH_OF_LOSS',
                                        col('SUM(BUILDING_DAMAGE_AMOUNT)'): 'BUILDING_DAMAGE_AMOUNT',
                                        col('SUM(CONTENTS_DAMAGE_AMOUNT)'): 'CONTENTS_DAMAGE_AMOUNT'
                                       })
                                .sort(col("YEAR_OF_LOSS"))
                        )

    return daily_precip.to_pandas(), severe_weather.to_pandas(), insurance_claims.to_pandas()


# Load and cache data
daily_precip, severe_weather, insurance_claims = load_data(selected_state)

tab1, tab2, tab3 = st.tabs(['Daily Average Precipitation', 'Yearly Severe Weather Days', 'Insurance Flood Losses by Year and Month' ])

with tab1: 
    st.line_chart(daily_precip, x="DATE")

with tab2: 
    st.bar_chart(severe_weather, x="YEAR", y="COUNT_SEVERE_WEATHER_DAYS")

with tab3: 
    heatmap = alt.Chart(insurance_claims).mark_rect().encode(
                    x=alt.X('YEAR_OF_LOSS:N'),
                    y=alt.Y('MONTH_OF_LOSS:N', sort=['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']),
                    color=alt.Color('BUILDING_DAMAGE_AMOUNT:Q', legend=alt.Legend(orient='bottom'))
                )
    
    st.altair_chart(heatmap)
