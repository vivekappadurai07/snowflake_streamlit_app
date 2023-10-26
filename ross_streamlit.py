import streamlit as st
import pandas as pd
from streamsets.sdk import ControlHub
import json
def Authenticate():
   sch = ControlHub(credential_id='489da572-9926-42df-b713-db491e8d5423',
                    token='eyJ0eXAiOiJKV1QiLCJhbGciOiJub25lIn0.eyJzIjoiNzYxY2Q3YWEzNjg4MjljN2U2YzFiYzgyODBlYzFlMjE2MmFiY2I3MzRhNWVjZWMwMWNjMjM4MTVmMGQzZGRkOGMxMGQyYWIzNDA5NGRiZWIyMWYxYmVmODA0NjhhMzUxYmI5MmQwMzQ1ZjAyMzRkMjg4N2MyYjE0MTBlOWVmMTgiLCJ2IjoxLCJpc3MiOiJuYTAxIiwianRpIjoiNDg5ZGE1NzItOTkyNi00MmRmLWI3MTMtZGI0OTFlOGQ1NDIzIiwibyI6IjRmNzE1YjA1LTNlMmUtMTFlZC05OWMzLWQzNTA4MDEzY2RhZSJ9.')
   return sch


def create_pipeline(pipeline_name,commit_msg,origin,orgin_config,dest):

   sch = Authenticate()
   sdc = sch.data_collectors.get(engine_url='http://20.29.254.56:18631')
   builder = sch.get_pipeline_builder(engine_type='data_collector', engine_id=sdc.id)

   src_Amazon_S3 = builder.add_stage(origin, type='origin')
   src_stage_attrs = orgin_config

   src_Amazon_S3.set_attributes(**src_stage_attrs)

   dest_Trash_0 = builder.add_stage(dest, type='destination')

   dest_stage_attrs = {'stage_name': 'trash_vivek'}

   dest_Trash_0.set_attributes(**dest_stage_attrs)

   src_Amazon_S3 >> [dest_Trash_0]

   pipeline = builder.build(pipeline_name)

   p = sch.publish_pipeline(pipeline=pipeline, commit_message=commit_msg)

   print(p.response)

   print('DC_PIPELINE_ID_0=' + pipeline.pipeline_id)
   return f'pipeline with name: **{pipeline_name}** and id: **{pipeline.pipeline_id}** created'

def fetch_allJobs():
   sch = Authenticate()

   query = 'name == "***"'
   jobs = sch.jobs.get_all(search=query)

   job_lst = []
   for job in jobs:
       job_lst.append(job.job_name)

   #comma_separated = str(",".join(job_lst)).strip()
   print(type(job_lst))
   return job_lst

def start_job1(job_name):
   sch = Authenticate()
   query = f'name == "*{job_name}*"'
   job = sch.jobs.get(search=query)
   #job = sch.jobs.get(name=job_name)
   resp =  sch.start_job(job)
   return resp.response



st.sidebar.slider("test")
with st.form(key="a"):
   col1, col2 = st.columns([0.65, 0.35])
   with col1:
       st.title("Create Pipeline :clap:   ")
   with col2:
       st.image('/home/azureuser/logo.png', width=240)

   pipeline_name = st.text_input(label="Pipeline name:")
   commit_msg = st.text_input(label="Commit Message:")
   origin = st.selectbox(
       'Origins?',
       ('Amazon S3','Dev Raw Data Source'))

   orgin_config_data =   {'authentication_method': 'WITH_IAM_ROLES',
                      'bucket': 'streamsets-customer-success-internal/wilsonshamim/automated',
                      'data_format': 'JSON',
                      'common_prefix': 'wilsonshamim',
                      'prefix_pattern': 'emp.csv'}



   orgin_config = st.text_area("Origin Configurations",value=json.dumps(orgin_config_data,indent=4))
   dest = st.selectbox(
       'Destinations?',
       ('Trash','Amazon S3'))


   submit = st.form_submit_button(label="submit")
   if submit:
       resp = create_pipeline(pipeline_name,commit_msg,origin,orgin_config,dest)
       result = f'ok {resp}'
       st.write(result)


with st.form(key="b"):

   col1, col2 = st.columns([0.65, 0.35])
   with col1:
       st.title("Start Job :balloon:   ")
   with col2:
       st.image('/home/azureuser/logo.png', width=240)

   job_lists = st.selectbox(
       'select job?',
       fetch_allJobs())

   submit1 = st.form_submit_button(label="submit1")
   if submit1:
       resp = start_job1(job_lists)
       result = f'ok {resp}'
       st.write(result)
