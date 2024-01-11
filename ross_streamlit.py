import streamlit as st
import pandas
from streamsets.sdk import ControlHub
import json
import ast

# Define a global variable to store the pipeline list
pipeline_list = None

def Authenticate():
    sch = ControlHub(credential_id='489da572-9926-42df-b713-db491e8d5423',
                     token='eyJ0eXAiOiJKV1QiLCJhbGciOiJub25lIn0.eyJzIjoiNDkzNjFjNTgyOTFjODI3MWE1NjllNGNjNGI3NzBmZWVjNjdiOTRiY2ZlMTFjNDA4NTExMGIwNzYwNThkYTI1NDdlZjk2YWU0OTc5Njg5N2Q1NTFhNmQ5MjgyNzVkODMxNjlkODdkODdiZjEyMjA5M2I0MGQ4NTNkOTc3YTZkZDgiLCJ2IjoxLCJpc3MiOiJuYTAxIiwianRpIjoiNDg5ZGE1NzItOTkyNi00MmRmLWI3MTMtZGI0OTFlOGQ1NDIzIiwibyI6IjRmNzE1YjA1LTNlMmUtMTFlZC05OWMzLWQzNTA4MDEzY2RhZSJ9.')
    return sch
def Fetch_all_Pipelines():
    global pipeline_list
    if pipeline_list is not None:
        return pipeline_list

    sch = Authenticate()
    query = 'name=="***" and version!= "*DRAFT*"'
    pipelines = sch.pipelines.get_all(search=query)
    pipeline_lst = [pipeline.name for pipeline in pipelines]
    pipeline_list = pipeline_lst
    return pipeline_lst

def create_streamsets_job(job_name, pipeline_name, runtime_parameters, tags):
    # Authenticate with StreamSets Control Hub
    sch = Authenticate()
    pipeline = sch.pipelines.get(name=pipeline_name)
    # Get a job builder
    job_builder = sch.get_job_builder()
    # Build the job with the specified parameters
    job = job_builder.build(
        job_name=job_name,
        pipeline=pipeline,
        runtime_parameters=runtime_parameters,
        tags=tags
    )
    sch.add_job(job)
    # Print a success message or perform further actions
    st.success(f"StreamSets job '{job_name}' has been created!")
    return job.job_id

st.set_page_config(layout="wide")
st.title(':blue[**Streamsets Job Creation from Pipeline Template**] ')
col1, col2 = st.columns([0.5, 0.5])
with col1:
    st.image('/Users/vivekappadurai/PycharmProjects/pythonProject/venv/share/image/Ross_Stores_logo.svg.png', width=240)
with col2:
    st.image('/Users/vivekappadurai/PycharmProjects/pythonProject/venv/share/image/streamsets.png', width=240)


# sch = Authenticate()
# job = sch.jobs.get(job_id='d4e208f0-9020-4a27-a728-5c386735d218:4f715b05-3e2e-11ed-99c3-d3508013cdae')
# st.write(f"StreamSets job Status is '{job.history[0].status} '.")
# st.write(f"Input Record Count is '{job.metrics[0].input_count} '.")
# st.write(f"Ouput Record Count is '{job.metrics[0].output_count} '.")
# st.write(f"Error Record Count is '{job.metrics[0].error_count} '.")


st.header("Create a StreamSets Job")
lst =  Fetch_all_Pipelines()
my_dict = {value: value for index, value in enumerate(lst)}
pipeline_name = st.selectbox('Select a Pipeline :', list(my_dict.keys()))

# Streamlit UI elements for user input
job_name = st.text_input("Enter Job Name <Env>_<Pipeline_Name>")
runtime_parameters = st.text_input('Runtime Parameters (JSON format {"a":"b"})')
tags = st.text_input("Enter Tags (comma-separated)")

if st.button("Create Job"):
    try:
        # Parse the JSON input for runtime parameters
        runtime_parameters_dict = json.loads(runtime_parameters) if runtime_parameters else None
        # Parse the comma-separated tags
        tags_list = tags.split(",") if tags else None
        # Create the StreamSets job
        job_output = create_streamsets_job(job_name, pipeline_name, runtime_parameters_dict, tags_list)
        job_output.replace(":", "@")
        url_prefix = "https://na01.hub.streamsets.com/sch/jobs/detail/"
        url = url_prefix + job_output
        # Display the URL in Streamlit
        st.write(f"Verify the Job in Control Hub: {url}")
    except Exception as e:
        st.error(f"An error occurred: {str(e)}")


