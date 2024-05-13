use role accountadmin;

----------------------------------------
---------------- PART 1 ----------------
----------------------------------------

-- A security integration is required to enable authentication to the deployed app
create security integration if not exists snowservices_ingress_oauth
  type=oauth
  oauth_client=snowservices_ingress
  enabled=true;

grant bind service endpoint on account to role sysadmin;

-- Compute pool for running the service (For this demo, we use the smallest compute pool available)
-- Note that the app still needs a virtual warehouse 
create compute pool qfes_streamlit_compute_pool
  min_nodes = 1
  max_nodes = 1
  instance_family = CPU_X64_XS;
  
grant usage,monitor on compute pool qfes_streamlit_compute_pool to role sysadmin;

use role sysadmin;

-- Virtual warehouse for running the queries generated from the app
-- The name of this warehouse needs to be embedded into the speficiation YAML file of the CREATE SERVICE command at a later point
create warehouse app_wh 
  warehouse_size=xsmall 
  auto_suspend=60 
  auto_resume=true 
  initially_suspended=true;

create image repository if not exists spcs_image_repository;




----------------------------------------
---------------- PART 2 ----------------
----------------------------------------

use role accountadmin;

-- Network rule for accessing the internet
create network rule allow_mapbox_rule
  type = 'HOST_PORT'
  mode = 'EGRESS'
  value_list = ('0.0.0.0:443','0.0.0.0:80');

-- External access integration
create or replace external access integration mapbox_api_access_integration
  allowed_network_rules = (allow_mapbox_rule)
  enabled = true;

grant usage on integration mapbox_api_access_integration to role sysadmin;

use role sysadmin;

-- Creating the service
-- Replace <database>, <schema>, <image_repository>, <image_name> and <warehouse_name> with the right values
create service streamlit_spcs
  in compute pool qfes_streamlit_compute_pool
  external_access_integrations = (MAPBOX_API_ACCESS_INTEGRATION)
  from specification $$
spec:
  containers:
    - name: streamlitapp
      image: sfseapac-au-demo93.registry.snowflakecomputing.com/<database>/<schema>/<image_repository>/<image_name>
      env:
        SNOWFLAKE_WAREHOUSE: <warehouse_name>
  endpoints:
    - name: streamlitapp
      port: 8501
      public: true
  $$;

-- Verifying and checking that the service is created 
show services;

-- Verifying that the service is ready (not pending)
select system$get_service_status('streamlit_spcs');

-- Getting the URL for the app (This may take a few minutes to generate)
show endpoints in service streamlit_spcs;