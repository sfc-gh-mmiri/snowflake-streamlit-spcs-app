import streamlit as st
from snowflake.snowpark import Session
import snowflake.snowpark.functions as fn
#from snowflake.cortex import Complete
import json 
import pandas as pd
import altair as alt
#import branca.colormap as cm
import numpy as np
import pydeck as pdk
from pydeck.types import String
import matplotlib.pyplot as plt
import spcs_helpers

st.set_page_config(layout="wide",page_title="Historical Fires in QLD")

# Get User from SPCS Headers
from streamlit.web.server.websocket_headers import _get_websocket_headers
user = _get_websocket_headers().get("Sf-Context-Current-User") or "Visitor"

# Make connection to Snowflake and cache it
@st.cache_resource
def connect_to_snowflake():
    return spcs_helpers.session()

session = connect_to_snowflake()

# Pulling all parameter values as key-value pairs (to be used for filters in the UI)
@st.cache_data
def get_filter_values():
    df_values = session.sql("""
    select distinct 'Fire Type' category,fire_type value from qld_historical_fires
    union all 
    select distinct 'Burn Status' category,burn_status value from qld_historical_fires
    union all 
    select distinct 'Owning Agency' category,owning_agency value from qld_historical_fires
    union all 
    select distinct 'Station Name' category,station value from qld_fire_brigade_stations
    union all 
    select distinct 'Year' category, year(ignition_date)::string value from qld_historical_fires
    order by 1,2
    """).to_pandas()
    return df_values

# To be used later in the controls for filtering the data
df_filter_values = get_filter_values()
df_fire_stations = df_filter_values[df_filter_values["CATEGORY"]=='Station Name']
df_year = df_filter_values[df_filter_values["CATEGORY"]=='Year']
df_burn_status = df_filter_values[df_filter_values["CATEGORY"]=='Burn Status']

min_year,max_year = int(df_year["VALUE"].min()),int(df_year["VALUE"].max())


with st.sidebar:
    st.image('qfes-logo.png',width=150)
    st.title("Historical Fires in QLD")

    # Filters (UI controls)
    filter_station_name = st.selectbox('Fire Station:',pd.DataFrame({'<Select>'},columns=['VALUE'])._append(df_fire_stations[["VALUE"]]))    
    filter_fire_radius = st.slider('Lookup Radius (in Km):',min_value=10,max_value=100,step=1,value=10)
    filter_burn_status = st.multiselect('Burn Status:',df_burn_status["VALUE"])    
    filter_year_range = st.slider('Date Range (by Year):',min_value=min_year,max_value=max_year,value=(min_year,max_year))    
    filter_fire_age = st.radio('Fire Lifetime:',('All','0-7 days','8-30 days','31-90 days','91-180 days','181-360 days','+360 days'))

def get_fire_data(filter_station_name,filter_fire_radius,filter_year_range):  
    """ 
    This method returns the list of fires within a certain radius from the chosen station. 
    Date range is another parameter that is used to filter the returned resultset.
    This function returns a Snowpark dataframe. 
    """        
    sql = """            
            with cte_fires as 
            (
                select
                    fire_label,   
                    fire_type,    
                    burn_status,
                    general_location,
                    owning_agency,      
                    datediff(day,ignition_date,out_date) fire_duration_days,        
                    year(ignition_date) ignition_year,
                    ignition_date::string ignition_date, 
                    ifnull(out_date::string,'Unknown') out_date, 
                    percentage_burnt,
                    round(area_hectare,2) area_hectare,
                    geometry,
                    geography,
                    count_of_intersecting_properties 
                from qld_historical_fires
            )
            ,cte_fire_station as 
            (
                select
                    station,      
                    longitude station_longitude,
                    latitude station_latitude,  
                    geometry station_geometry,
                    geography station_geography,
                    ifnull(brigade_name,'') brigade_name,
                    ifnull(rural_area,'') rural_area
                from qld_fire_brigade_stations                
            )
            select
            fs.*,            
            hf.*,
            ifnull(hf.fire_duration_days::string,'') fire_lifetime_days,
            round(st_distance(fs.station_geography,hf.geography)/1000,2) distance_from_station_km
            from cte_fire_station fs
            inner join cte_fires hf
            on st_distance(fs.station_geography,hf.geography)<={fire_radius}            
            where "STATION"='{station_name}'
            and ignition_year between {start_year} and {end_year}
    """.format(fire_radius=filter_fire_radius*1000,station_name=filter_station_name,start_year=filter_year_range[0],end_year=filter_year_range[1])
    fires = session.sql(sql)
    return fires 


#@st.cache_data 
def get_fire_data_filtered(filter_station_name,filter_fire_radius,filter_year_range,filter_burn_status,filter_fire_age):
    """ 
    This method applies additional filters on top of the resultset generated from get_fire_data().
    The output is a Snowpark dataframe. 
    """        
    fires = get_fire_data(filter_station_name,filter_fire_radius,filter_year_range)
            
    if filter_fire_age == 'All':
        fires_filtered = fires        
    elif filter_fire_age == '+360 days':
        fires_filtered = fires[fires['FIRE_DURATION_DAYS']>90]
    else:
        fire_age_range = filter_fire_age.split(' ')[0].split('-')
        min_fire_age,max_fire_age=int(fire_age_range[0]),int(fire_age_range[1])
        fires_filtered = fires.filter((fn.col("FIRE_DURATION_DAYS") >= min_fire_age) & (fn.col("FIRE_DURATION_DAYS") <= max_fire_age))        
    if len(filter_burn_status)>0:
        fires_filtered = fires_filtered[fires_filtered['BURN_STATUS'].isin(filter_burn_status)]    

    return fires_filtered


def aggregate_fire_data(fires,aggregation_type):
    """ 
    This method applies aggregations on top of the fire dataset returned from the above functions. 
    There are different aggregation types that corrspond to the visuals used in the app. 
    Aggregated resultsets are returned as Snowpark dataframe objects.
    """        
    if aggregation_type == 'Year':
        #fires_aggregated = fires.group_by(fn.col("IGNITION_YEAR"),fn.col("PERCENTAGE_BURNT"),fn.col("FIRE_TYPE")).agg(fn.count(fn.col("FIRE_LABEL")).alias("COUNT_OF_FIRES"))
        fires_aggregated = fires.with_column("DISTANCE_FROM_STATION",fn.round(fn.col("DISTANCE_FROM_STATION_KM")/10)).\
        group_by(fn.col("IGNITION_YEAR"),fn.col("DISTANCE_FROM_STATION")).agg(fn.count(fn.col("FIRE_LABEL")).alias("COUNT_OF_FIRES"))
    if aggregation_type == 'Year-Type':
        fires_aggregated = fires.select(fn.col("IGNITION_YEAR").alias("Ignition Year"),fn.col("FIRE_TYPE").alias("Fire Type"),fn.col("FIRE_LABEL"))\
            .group_by(fn.col("Ignition Year"),fn.col("Fire Type")).agg(fn.count(fn.col("FIRE_LABEL")).alias("Count of Fires"))        
    elif aggregation_type == 'Distance-Age':
        fires_aggregated = fires.filter(fn.col("FIRE_DURATION_DAYS")>=0)\
        .select(fn.col("FIRE_DURATION_DAYS").alias("Fire Duration (Days)"),
                                        fn.col("DISTANCE_FROM_STATION_KM").alias("Distance from Station (Km)"),fn.col("FIRE_LABEL"))\
        .group_by(fn.col("Fire Duration (Days)"),fn.col("Distance from Station (Km)"))\
        .agg(fn.count(fn.col("FIRE_LABEL")).alias("Count of Fires"))
    elif aggregation_type == 'Distance-Burn':   
        fires_aggregated = fires.select(fn.col("PERCENTAGE_BURNT").alias("Burn Percentage"),
                                        fn.col("DISTANCE_FROM_STATION_KM").alias("Distance from Station (Km)"),
                                        fn.col("FIRE_LABEL"))\
            .group_by(fn.col("Burn Percentage"),fn.col("Distance from Station (Km)"))\
            .agg(fn.count(fn.col("FIRE_LABEL")).alias("Count of Fires"))    
    elif aggregation_type == 'Measures':
        fires_aggregated = fires.group_by(fn.col("IGNITION_YEAR"))\
            .agg(fn.count(fn.col("FIRE_LABEL")).alias("Count of Fires"),
                 fn.avg(fn.col("PERCENTAGE_BURNT")).alias("Average % Burned"),
                 fn.avg(fn.col("FIRE_DURATION_DAYS")).alias("Average Fire Duration"),
                 fn.sum(fn.col("COUNT_OF_INTERSECTING_PROPERTIES")).alias("Count of Intersecting Properties"),
                 fn.max(fn.col("PERCENTAGE_BURNT")).alias("Highest % Burned"),
                 fn.max(fn.col("FIRE_DURATION_DAYS")).alias("Highest Fire Age (Days)"),
                 fn.max(fn.col("FIRE_DURATION_DAYS")).alias("Highest Fire Duration"),                 
                 fn.max(fn.col("AREA_HECTARE")).alias("Largest Fire Area"))                             
    return fires_aggregated

def format_axis(df, column_name):
    """Formats the year column in a dataframe to display only the year as an integer."""
    
    ignition_years_formatted = [str(int(year)) for year in df[column_name]]  # Assuming it's a list or Series
    df[column_name] = ignition_years_formatted
    return df

if filter_station_name!='<Select>':           
    fires = get_fire_data_filtered(filter_station_name,filter_fire_radius,filter_year_range,filter_burn_status,filter_fire_age)
else:
    fires = None

# Tabs for separating app sections
tab_map,tab_stats,tab_raw_data, tab_ai, tab_about = st.tabs(["Map","Analytics","Raw Data","AI","About"])

with tab_map:
    if fires!=None: 
        with st.spinner('Loading...'):          
            df_fires = fires.to_pandas()

            if len(df_fires)>0:

                def get_coordinates(row):
                    if row is not None:
                        try:
                            return json.loads(row)["coordinates"][0]
                        except (KeyError, TypeError):
                            return None
                    else:
                        return None

                df_fires["coordinates"] = df_fires["GEOMETRY"].apply(lambda row: json.loads(row)["coordinates"][0])
                df_fires["station_coordinates"] = df_fires["STATION_GEOMETRY"].apply(lambda row: json.loads(row)["coordinates"])                        

                # Station information (Label and coordinates) - Only picking the first row
                df_stations = df_fires[["STATION","station_coordinates"]].iloc[[0]]

                # Average Lat/Long to position the map at render time
                average_latitude = df_fires['STATION_LATITUDE'][0]
                average_longitude = df_fires['STATION_LONGITUDE'][0]    

                stations = {
                    'position': [(average_latitude,average_longitude)]
                }

                def get_color(value):
                    """ 
                    This function can be used to return conditional color combinations based on a set criteria.
                    Currently, it only returns a fixed value, but this can be customized.
                    """

                    #if value < 80:
                    return [255, 0, 0]  # Red    
                    #else:
                    #    return [0, 0, 255]  # Blue

                # Adding the color component to the dataframe
                df_fires['color'] = df_fires['PERCENTAGE_BURNT'].apply(get_color)

                # Fire boundaries on the map
                fires_layer = pdk.Layer(
                        "PolygonLayer",
                        df_fires,
                        id="fires",
                        opacity=0.2,
                        stroked=False,
                        get_polygon="coordinates",
                        filled=True,
                        extruded=True,
                        elevation_scale=0,
                        wireframe=True,                
                        get_fill_color="color",
                        get_line_color=[0, 0, 0],
                        line_width_min_pixels=1,
                        auto_highlight=True,
                        highlight_color=[189, 219, 0],
                        pickable=True,
                )
                
                # Station point on the map
                stations_layer = pdk.Layer(
                    "ScatterplotLayer",
                    df_stations,
                    id="stations",    
                    opacity=1,                            
                    get_position="station_coordinates",                           
                    stroked=True,     
                    get_fill_color=[255, 234, 0],
                    get_radius=150,
                    radius_scale=1,
                    get_line_color=[0, 0, 0],
                    line_width_min_pixels=2,
                    filled=True,
                    auto_highlight=True,
                    pickable=False
                )

                # Title of the fire station
                stations_title_layer = pdk.Layer(
                    type="TextLayer",
                    data=df_stations,
                    pickable=False,
                    get_position="station_coordinates",
                    get_text="STATION",
                    get_size=20,
                    get_color=[0, 0, 0],
                    get_angle=0,
                    getTextAnchor= '"start"',
                    get_alignment_baseline='"bottom"'
                )

                # Adding the abovementioned layers to the map
                included_layers = [fires_layer,stations_layer,stations_title_layer]
                
                # Custom zoom based on the selected radius
                def get_zoom_size(radius):
                    if radius<=10:
                        zoom=12
                    elif radius>10 and radius<=50:
                        zoom=10
                    elif radius>50:
                        zoom=8
                    return zoom

                map = pdk.Deck(
                    map_style=None,
                    initial_view_state=pdk.ViewState(
                        longitude=average_longitude,
                        latitude=average_latitude,
                        zoom=get_zoom_size(filter_fire_radius),
                        pitch=0,
                        height=800
                    ),
                    layers=included_layers,#[fires_layer,stations_layer,stations_title_layer],
                    tooltip={
                    'html':        
                    '<table>'                  
                    '<tr><td><b>Fire Label:</b></td><td>{FIRE_LABEL}<br/></td></tr>'
                    '<tr><td><b>General Location:</b></td><td>{GENERAL_LOCATION}<br/></td></tr>'
                    '<tr><td><b>Fire Type:</b></td><td>{FIRE_TYPE}<br/></td></tr>'                    
                    '<tr><td><b>Burn Status:</b></td><td>{BURN_STATUS}<br/></td></tr>'
                    '<tr><td><b>Ignition Date:</b></td><td>{IGNITION_DATE}<br/></td></tr>'
                    '<tr><td><b>Out Date:</b></td><td>{OUT_DATE}<br/></td></tr>'
                    '<tr><td><b>Fire Life (days):</b></td><td>{FIRE_LIFETIME_DAYS}<br/></td></tr>'
                    '<tr><td><b>Area (ha):</b></td><td>{AREA_HECTARE}<br/></td></tr>'
                    '<tr><td><b>Percentage Burnt:</b></td><td>{PERCENTAGE_BURNT}<br/></td></tr>' 
                    '<tr><td><b>Distance from Station (km):</b></td><td>{DISTANCE_FROM_STATION_KM}<br/></td></tr>'       
                    '<tr><td><b>Brigade Name:</b></td><td>{BRIGADE_NAME}<br/></td></tr>'
                    '<tr><td><b>Rural Area:</b></td><td>{RURAL_AREA}<br/></td></tr>'
                    '<tr><td><b>Intersecting Properties:</b></td><td>{COUNT_OF_INTERSECTING_PROPERTIES}<br/></td></tr>'
                    ,
                    'style': {
                        'color': 'white'
                    }
                    }        
                )     

                pydeck_chart = st.pydeck_chart(map,use_container_width=True)
            else:
                st.markdown('### No fire records found based on the selected criteria!')

with tab_stats:    
    if fires!=None: 

        # Annual Fire Count
        aggregated_fires_by_year_type = aggregate_fire_data(fires,'Year-Type').to_pandas()                
        
        if len(aggregated_fires_by_year_type)>0:
            st.subheader('Annual Fires by Type')
            chart = st.bar_chart(aggregated_fires_by_year_type, x="Ignition Year", y="Count of Fires", use_container_width=True, color="Fire Type")

            # Heatmap chart
            st.subheader('Count of Fires by Year/Distance from Station')
            aggregated_fires_by_year = aggregate_fire_data(fires,'Year').to_pandas()        
            
            fires_heatmap_data = (aggregated_fires_by_year.pivot_table(index="DISTANCE_FROM_STATION", columns="IGNITION_YEAR", values="COUNT_OF_FIRES",fill_value=0))
                    
            plt.rcParams.update({'font.size': 5})

            fig, ax = plt.subplots(figsize=(6,2))
            ax.pcolormesh(
                fires_heatmap_data.columns, fires_heatmap_data.index, fires_heatmap_data.values, cmap="YlGnBu"
            )

            ax.set_xlabel("Ignition Year")
            ax.set_ylabel("Distance from Fire Station (x10Km)")            

            st.pyplot(fig)

            # Fire Breakdown by Age and Distance from Station
            aggregated_fires_by_distance = aggregate_fire_data(fires,'Distance-Age').to_pandas()        
            aggregated_fires_by_distance = format_axis(aggregated_fires_by_distance.copy(), "Count of Fires")
            c2 = (alt.Chart(aggregated_fires_by_distance)
                .mark_circle(color='blue',opacity=0.3, size=50)
                .encode(x=alt.X("Fire Duration (Days)"), y="Distance from Station (Km)")
            )
            
            # Fire Breakdown by % Burned and Distance from Station
            aggregated_fires_by_distance_perc = aggregate_fire_data(fires,'Distance-Burn').to_pandas()                
            c3 = (alt.Chart(aggregated_fires_by_distance_perc)
                .mark_circle(color='red',opacity=0.3, size=50)
                .encode(x=alt.X("Burn Percentage"), y="Distance from Station (Km)")                    
            )

            chart1,chart2 = st.columns(2)
            
            with chart1:
                st.subheader('Breakdown by Duration & Distance')
                st.altair_chart(c2, use_container_width=True)
            with chart2:
                st.subheader('Breakdown by % Burned and Distance')
                st.altair_chart(c3, use_container_width=True)

                
            st.subheader('Annual Trend')        
            aggregated_fires_measures = aggregate_fire_data(fires,'Measures').to_pandas()         

            aggregated_fires_measures = format_axis(aggregated_fires_measures.copy(), "IGNITION_YEAR")

            filter_included_measures = st.multiselect("Select Measures to Display:", aggregated_fires_measures.columns[1:], default=['Average % Burned','Average Fire Duration'], key="aggregated_fires_measures")
            
            # Filter data based on selection
            fires_to_plot = aggregated_fires_measures[["IGNITION_YEAR"] + filter_included_measures]
            
            data_dict = {}

            filtered_df = aggregated_fires_measures[filter_included_measures + ["IGNITION_YEAR"]]


            # Populate the data dictionary with selected columns
            for column in filter_included_measures:
                data_dict[column] = filtered_df.set_index("IGNITION_YEAR")[column]
            
            # Line chart
            if filter_included_measures:            
                st.line_chart(pd.DataFrame(data_dict))
            else:
                st.write("Please select at least one measure.")
        else:
            st.markdown('### No fire records found based on the selected criteria!')



with tab_raw_data:
    if fires!=None:
        df_fires = fires.select(fn.col("STATION"),
                    fn.col("FIRE_LABEL"),
                    fn.col("GENERAL_LOCATION"),
                    fn.col("FIRE_TYPE"),
                    fn.col("BURN_STATUS"),
                    fn.col("IGNITION_DATE"),
                    fn.col("OUT_DATE"),
                    fn.col("FIRE_DURATION_DAYS"),
                    fn.col("PERCENTAGE_BURNT"),
                    fn.col("AREA_HECTARE"),                 
                    fn.col("COUNT_OF_INTERSECTING_PROPERTIES")).to_pandas()

        if len(df_fires)>0:
            st.dataframe(df_fires)
        else:
            st.markdown('### No fire records found based on the selected criteria!')    

# Set to "False" if Cortex LLM functions are not available in the region where the app is going to be deployed.
include_ai = True
    
with tab_ai:
    if include_ai:
        prompt_full = """
        Be concise in your answers.
        Introduce yourself as QFES-GPT, an AI Sales Assistant.
        You are a Snowflake SQL Expert named QFES-GPT.
        Your goal is to give correct, executable sql query to users.
        You MUST generate only one SQL query.
        You will be replying to users who will be confused if you don't respond in the character of QFES-GPT.
        You are given two tables, the first table name is in <firstTableName> tag, the columns are in <firstTableColumns> tag.
        The second table name is in <secondTableName> tag, the columns are in <secondTableColumns> tag.
        The user will ask questions, for each question you should respond and include a sql query based on the question and one or both of the tables. 
        Sometimes these tables need to be joined together to produce meaningful results.

        Here is the first table name <firstTableName> QLD_FIRE_BRIGADE_STATIONS </firstTableName>

        <firstTableDescription>
        This tables holds information about fire stations, their address, their levy type, and their location information in a variety of data types including geography and geometry. Location data holds points for showing the exact location of fire stations on the map.
        </firstTableDescription>

        Here are the columns of the QLD_FIRE_BRIGADE_STATIONS

        <columns>

        STATION: TEXT
        ADDRESS: TEXT
        LOCALITY: TEXT
        ALTERNATIVE_ADDRESS: TEXT
        CREWING: TEXT
        LEVYTYPE: TEXT
        LONGITUDE: NUMBER
        LATITUDE: NUMBER
        GEOMETRY: GEOMETRY
        GEOGRAPHY: GEOGRAPHY
        BRIGADE_ID: NUMBER
        BRIGADE_NAME: TEXT
        RURAL_AREA: TEXT
        BRIGADE_CLASS_ID: TEXT
        BRIGADE_GEOGRAPHY: GEOGRAPHY
        BRIGADE_GEOMETRY: GEOMETRY
        MAPPED_TO_BRIGADE: BOOLEAN

        </columns>


        Here is the second table name <secondTableName> QLD_HISTORICAL_FIRES </secondTableName>

        <secondTableDescription>
        This tables holds information about historical fires, fire type, their burn status, owning agency, ignition and out date, length of fire, percentage burnt, and its location data in the form of geometry and geography data types. The location data includes multi-polygons showing the areas of each fire.
        Fire area is stored in column FIRE_AREA, and the unit is in hectates, so if other units are mentioned, you need to convert them to hectare first.
        Fire Type is stored in column FIRE_TYPE column and contains one of these values: Unknown, Planned Burn, Wildfire

        </firstTableDescription>

        Here are the columns of the QLD_HISTORICAL_FIRES

        <columns>

        FIRE_LABEL: TEXT
        FIRE_TYPE: TEXT
        BURN_STATUS: TEXT
        GENERAL_LOCATION: TEXT
        OWNING_AGENCY: TEXT
        IGNITION_DATE: DATE
        OUT_DATE: DATE
        OUT_YEAR: NUMBER
        OUT_MONTH: NUMBER
        PERCENTAGE_BURNT: NUMBER
        AREA_HECTARE: NUMBER 
        GEOMETRY: GEOMETRY
        GEOGRAPHY: GEOGRAPHY
        COUNT_OF_INTERSECTING_PROPERTIES: NUMBER

        </columns>


        Here are critical rules for the interaction you must abide:
        <rules>
        1. You must only return a SQL statement that is ready to run in your response and nothing else.
        2. Text / string where clauses must be fuzzy match e.g ilike %keyword%
        3. Make sure to generate a single snowflake sql code, not multiple. 
        4. You should only use the table columns given in <columns>, and the table given in <tableName>, you MUST NOT hallucinate about the table names nor columns
        5. DO NOT put numerical at the very front of sql variable.
        6. Whenever fire and fire station information is required in the same query, join the two tables. In the join condition, use ST_DISTANCE geospatial function to return stations and fires where both tables' GEOGRAPHY columns are within a certain distance in meters. Use the Distance that was provided by the user. 
        7. When referencing columns that are present in both tables, use prefixes to avoid ambiguity in column names.
        8. Use "ilike %keyword%" OR "not ilike %keyword%" for fuzzy match queries.
        9. Never return more than one query.
        10. Column STATION holds fire station name. Column BRIGADE_NAME stores brigade name. A brigade may contain multiple fire stations.
        11. When fire station name is provided and has to be used to filter based on the value of column STATION, use the provided value in upper letters.
        12. Always use short alias values when referencing tables.
        13. Do not use GEOGRAPHY and GEOMETRY in any "GROUP BY" statement.
        14. Do not use columns GEOGRAPHY and GEOMETRY in any "PARTITION BY" statement.
        14. Always make sure the response starts with ```sql and ends with ```, so if SQL_STATEMENT is the generated query, the output should be like:
        ```aql SQL_STATEMENT ```
        </rules>

        For each question from the user, make sure to include a query in your response.

        """

        query = st.text_area('Enter your question:')

        if query!='':
            df_response = session.sql("""
                SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large', CONCAT($${prompt}$$, 'Question: ', $$ {query} $$, 'Answer: ' )) as RESPONSE
            """.format(prompt=prompt_full,query=query)).to_pandas()

            sql_query = str(df_response.iloc[0,0])            

            sql_query_fixed = sql_query.replace('```sql','').replace('```','')

            try:
                st.write('Query output:')
                st.dataframe(session.sql(sql_query_fixed).to_pandas())
            except:
                st.write('Error when running the query.')    

            with st.expander('SQL Query'):
                        st.write(sql_query)
    else:
        st.markdown('### This feature is inactive as Cortex LLM functions are not yet available!')

with tab_about:
    st.markdown('')
    st.markdown('##### About the App')
    st.markdown("""
    This app showcases various features and capabilities of Snowflake, including the following:

    - **Secure data sharing:** All datasets used in this app are publicy available data that is published on Snowflake Marketplace.
    - **Geospatial capabilities:** The app incorporates geospatial attributes (such as station locations and boundaries of historical fires) stored in native geospatial format. It utilizes geospatial functions extensively for joining datasets based on their geospatial specifications. Examples include:
        - Matching fire stations with fire brigades based on their geographic location
        - Calculating the number of properties (lots) intersecting areas impacted by each fire
        - Geojoining stations and fires to provide a list of fires within a certain distance from a fire station
    - **Cortex LLM functions:** Specifically, the COMPLETE function is employed to submit prompts to a Large Language Model (LLM) hosted within Snowflake. This feature enables the generation of queries based on user prompts.
    - **Streamlit:** The app utilizes Streamlit, an open-source Python library, to deliver an interactive web interface with controls, tabs, visuals, and interactive map functionality.
    - **Snowpark Container Services:** This app is hosted and operated as an always-on service within the security boundaries of Snowflake leveraging SPCS (Snowpark Container Services).            
    """)
    st.markdown('')
    st.markdown('##### Data Sources')
    st.markdown("""
    The datasets used for this app are all sourced from the following data products on Snowflake Marketplace (provided by <b>The PropTech Cloud</b>). Click on the following links to find out more about them:
    """)
    st.markdown("""
    - [Cadastre - Boundaries & Attributes - Australia](https://app.snowflake.com/marketplace/listing/GZSUZCN98A/the-proptech-cloud-cadastre-boundaries-attributes-australia?originTab=provider&providerName=The%20Proptech%20Cloud&profileGlobalName=GZSUZCN982)
        - <b>QLD_CADASTRE_LOT_GDA2020</b> 
    - [Wildfire - Fire Locations & Fire Stations - Australia](https://app.snowflake.com/marketplace/listing/GZSUZCN99B/the-proptech-cloud-wildfire-fire-locations-fire-stations-australia-free?originTab=provider&providerName=The%20Proptech%20Cloud&profileGlobalName=GZSUZCN982)
        - <b>QLD_RURAL_FIRE_BRIGADE_BOUNDARIES_GDA2020</b> 
        - <b>QLD_HISTORICAL_FIRE_RECORDS_GDA2020</b>
        - <b>QLD_URBAN_FIRE_STATION_LOCATIONS_GDA2020</b>
        - <b>QLD_RURAL_FIRE_STATION_LOCATIONS_GDA2020</b>
    """)