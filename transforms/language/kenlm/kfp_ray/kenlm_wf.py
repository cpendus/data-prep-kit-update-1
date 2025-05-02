#!/usr/bin/env python

# (C) Copyright IBM Corp. 2024.
# Licensed under the Apache License, Version 2.0 (the “License”);
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#  http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an “AS IS” BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
import os, json, yaml
from typing import NamedTuple
import kfp.compiler
import kfp.components
import kfp.dsl 
import kfp
from workflow_support.compile_utils import (
    DEFAULT_KFP_COMPONENT_SPEC_PATH,
    ONE_HOUR_SEC,
    ONE_WEEK_SEC,
    ComponentUtils,
)
from python_apiserver_client.params import (
    EnvironmentVariables,
    EnvVarFrom,
    EnvVarSource,
)
#
# KFP pipeline with kenlm
#
KFP_NAME = "kenlm-dpkt"
KFP_DESCRIPTION = "kenlm transform"
KFP_IMAGE = "us.icr.io/cil15-shared-registry/preprocessing-pipelines/kfp-data-processing:0.2.3"
KFP_IMAGE_PULL_SECRET = "prod-all-icr-io"

# Data locations
INPUT_FOLDER = "cos-optimal-llm-pile/multilingual_jbross/fw2_sampled/sampled/subset=filtered/language=deu_Latn/split=test"
OUTPUT_FOLDER = "cos-optimal-llm-pile/multilingual_jbross/debug"
TMP_FOLDER = "output/tmp"

# The secret name for all S3 locations: input, output and temp
S3_SECRET = "cos-access"

# Input limits 
DATA_MAX_FILES = -1
DATA_NUM_SAMPLES = -1
DATA_CHECKPOINTING = False

COMPONENT_SPEC_PATH = os.getenv("KFP_COMPONENT_SPEC_PATH", DEFAULT_KFP_COMPONENT_SPEC_PATH)
if not os.path.isfile(os.path.join(COMPONENT_SPEC_PATH, "createRayClusterComponent.yaml")):
    import workflow_support.compile_utils
    base_dir = workflow_support.compile_utils.__file__.split("/")
    while base_dir and base_dir[-1] != "kfp":
        base_dir = base_dir[:-1]
    for root, dirs, files in os.walk("/".join(base_dir)):
        if "createRayClusterComponent.yaml" in files:
            COMPONENT_SPEC_PATH = root
            break

# Compute execution parameters for all transforms
def compute_exec_params_func(
    input_folder: str,
    output_folder: str,
    tmp_folder: str,

    data_max_files: int,
    data_num_samples: int,
    data_checkpointing: bool,

    runtime_pipeline_id: str,

    
    # args for kenlm
    kenlm_runtime_job_id: str,
    kenlm_content_column_name: str,
    kenlm_default_value: float,
    kenlm_lang_column_name: str,
    kenlm_model_file: str,
    kenlm_model_path: str,
    kenlm_output_perplexity_column_name: str,
    kenlm_process_languages: str,
    kenlm_sentence_piece_model_file: str,
    kenlm_sentence_piece_model_path: str,
    kenlm_ray_name: str,
    kenlm_ray_head_options: dict,
    kenlm_ray_worker_options: dict,
    kenlm_runtime_actor_options: dict,
    kenlm_runtime_code_location: dict,
    kenlm_additional_params: dict,
    
    ) -> NamedTuple('Outputs', [('kenlm', dict)]):
    from runtime_utils import KFPUtils
    import os

    if not tmp_folder: 
        tmp_folder = os.path.join(output_folder, "tmp") 
    kenlm_data_s3_config = dict(input_folder=input_folder, output_folder=output_folder)

    return (
        dict(
            data_s3_config= str(kenlm_data_s3_config),
            data_max_files= data_max_files,
            data_num_samples= data_num_samples,
            data_checkpointing= data_checkpointing,
            runtime_num_workers= KFPUtils.default_compute_execution_params(str(kenlm_ray_worker_options), str(kenlm_runtime_actor_options)),
            runtime_worker_options= str(kenlm_runtime_actor_options),
            runtime_pipeline_id= runtime_pipeline_id,
            runtime_code_location= str(kenlm_runtime_code_location),
            runtime_job_id = kenlm_runtime_job_id,
            # parameters for kenlm
            kenlm_content_column_name=kenlm_content_column_name,
            kenlm_default_value=kenlm_default_value,
            kenlm_lang_column_name=kenlm_lang_column_name,
            kenlm_model_file=kenlm_model_file,
            kenlm_model_path=kenlm_model_path,
            kenlm_output_perplexity_column_name=kenlm_output_perplexity_column_name,
            kenlm_process_languages=kenlm_process_languages,
            kenlm_sentence_piece_model_file=kenlm_sentence_piece_model_file,
            kenlm_sentence_piece_model_path=kenlm_sentence_piece_model_path
        )
    )

# KFPv1 and KFP2 uses different methods to create a component from a function. KFPv1 uses the
# `create_component_from_func` function, but it is deprecated by KFPv2 and so has a different import path.
# KFPv2 recommends using the `@kfp.dsl.component` decorator, which doesn't exist in KFPv1. Therefore, here we use
# this if/else statement and explicitly call the decorator.
if os.getenv("KFPv2", "0") == "1":
    compute_exec_params_op = kfp.dsl.component_decorator.component(
        func=compute_exec_params_func, base_image=KFP_IMAGE
    )
else:
    compute_exec_params_op = kfp.components.create_component_from_func(func=compute_exec_params_func, base_image=KFP_IMAGE)

def load_component(yaml_fn):
    with open(os.path.join(COMPONENT_SPEC_PATH, yaml_fn), 'r') as file:
        component_yaml = yaml.safe_load(file)
    component_yaml["implementation"]["container"]["image"] = KFP_IMAGE
     
    if os.path.basename(yaml_fn) == "deleteRayClusterComponent.yaml":
        component_yaml["inputs"] = component_yaml["inputs"][:-1]
        component_yaml["implementation"]["container"]["command"] = component_yaml["implementation"]["container"]["command"][:-2]
    
    component_txt = yaml.dump(component_yaml, sort_keys=False)
    return kfp.components.load_component_from_text(component_txt)

# create Ray cluster
create_ray_op = load_component("createRayClusterComponent.yaml")
# execute job
execute_ray_jobs_op = load_component("executeRayJobComponent.yaml")
# clean up Ray
cleanup_ray_op = load_component("deleteRayClusterComponent.yaml")

@kfp.dsl.pipeline(
        name = "kenlm-dpkt",
        description = "kenlm transform",
)
def kenlm_dpkt(
    input_folder: str = INPUT_FOLDER,
    output_folder: str = OUTPUT_FOLDER,
    tmp_folder: str = TMP_FOLDER,
    data_s3_access_secret: str = S3_SECRET,
    data_max_files: int = DATA_MAX_FILES,
    data_num_samples: int = DATA_NUM_SAMPLES,
    data_checkpointing: bool = DATA_CHECKPOINTING,
    runtime_pipeline_id: str = "pipeline_id",
    server_url: str = "http://kuberay-apiserver-service.kuberay.svc.cluster.local:8888",
    
    # args for kenlm
    kenlm_ray_run_id_KFPv2: str = "",    
    kenlm_ray_name: str = 'kenlm-dpkt-kenlm-kfp-ray',    
    kenlm_ray_head_options: dict = {'cpu': 1, 'memory': 4, 'image': 'us.icr.io/cil15-shared-registry/preprocessing-pipeline/dpk/language/kenlm-ray:1.0.3fr', 'image_pull_secret': 'prod-all-icr-io'},    
    kenlm_ray_worker_options: dict = {'replicas': 2, 'max_replicas': 2, 'min_replicas': 2, 'cpu': 4, 'memory': 16, 'image': 'us.icr.io/cil15-shared-registry/preprocessing-pipeline/dpk/language/kenlm-ray:1.0.3fr', 'image_pull_secret': 'prod-all-icr-io'},    
    kenlm_runtime_actor_options: dict = {'num_cpus': 1},    
    kenlm_runtime_code_location: dict = {'github': 'github', 'commit_hash': '12345', 'path': 'path'},    
    kenlm_additional_params: dict = {'wait_interval': 2, 'wait_cluster_ready_tmout': 400, 'wait_cluster_up_tmout': 300, 'wait_job_ready_tmout': 400, 'wait_print_tmout': 30, 'http_retries': 5, 'delete_cluster_delay_minutes': 0},
    kenlm_content_column_name: str = 'contents',
    kenlm_default_value: float = -1.0,
    kenlm_lang_column_name: str = 'lid',
    kenlm_model_file: str = '{lang}.arpa.bin',
    kenlm_model_path: str = '/models',
    kenlm_output_perplexity_column_name: str = 'kenlm',
    kenlm_process_languages: str = '',
    kenlm_sentence_piece_model_file: str = '{lang}.sp.model',
    kenlm_sentence_piece_model_path: str = '',
    
):
    """
    kenlm-dpkt - kenlm transform
    :param input_folder:
    :param output_folder:
    :param tmp_folder:
    :param data_s3_access_secret: s3 access secret
    :param data_max_files: maximum number of files to process
    :param data_num_samples: number of samples to process
    :param data_checkpointing: data checkpointing 
    :param runtime_pipeline_id: 
    :param server_url:
    :param kenlm_content_column_name: Name of the column with the text
    :param kenlm_default_value: Perplexity value for when a model is not found
    :param kenlm_lang_column_name: Name of the column with the language identifier
    :param kenlm_model_file: Format string to generate the kenlm model file name
    :param kenlm_model_path: Path to search for knlm model files
    :param kenlm_output_perplexity_column_name: Column name to store the perplexity score label
    :param kenlm_process_languages: Comma separated list of languages to process. Leave empty to attempt to process all languages.
    :param kenlm_sentence_piece_model_file: Format string to generate the sentence piece model file name.
    :param kenlm_sentence_piece_model_path: Path to search for sentence piece model files. Leave empty to search in the kenlm_model_path.
    :param kenlm_ray_name: name of the kenlm Ray cluster
    :param kenlm_ray_head_options: Ray head options
    :param kenlm_ray_worker_options: Ray worker options
    :param kenlm_runtime_actor_options: Runtime actor options
    :param kenlm_runtime_code_location: git repository information
    :param kenlm_additional_params: additional parameters for the Ray control commands
    
    :return: None
    """
    # In KFPv2 dsl.RUN_ID_PLACEHOLDER is deprecated and cannot be used since SDK 2.5.0. On another hand we cannot create
    # a unique string in a component (at runtime) and pass it to the `clean_up_task` of `ExitHandler`, due to
    # https://github.com/kubeflow/pipelines/issues/10187. Therefore, meantime the user is requested to insert
    # a unique string created at run creation time.
    if os.getenv("KFPv2", "0") == "1":
        print("WARNING: the ray cluster name can be non-unique at runtime, please do not execute simultaneous Runs of the "
              "same version of the same pipeline !!!")
        kenlm_run_id = kenlm_ray_run_id_KFPv2
    else:
        kenlm_run_id = kfp.dsl.RUN_ID_PLACEHOLDER
    # create the final clean_up task
    
    kenlm_clean_up_task = cleanup_ray_op(ray_name=kenlm_ray_name, run_id=kenlm_run_id, server_url=server_url)
    
    ComponentUtils.add_settings_to_component(kenlm_clean_up_task, ONE_HOUR_SEC * 2)
    kenlm_clean_up_task.set_display_name("Stop kenlm Ray cluster")
    with kfp.dsl.ExitHandler(kenlm_clean_up_task):
        # compute execution params
        compute_exec_params_task = compute_exec_params_op(
            input_folder= input_folder,
            output_folder= output_folder,
            tmp_folder= tmp_folder,

            data_max_files= data_max_files,
            data_num_samples= data_num_samples,
            data_checkpointing= data_checkpointing,

            runtime_pipeline_id= runtime_pipeline_id,
            
            # args for kenlm
            kenlm_runtime_job_id= kenlm_run_id,
            kenlm_content_column_name=kenlm_content_column_name,
            kenlm_default_value=kenlm_default_value,
            kenlm_lang_column_name=kenlm_lang_column_name,
            kenlm_model_file=kenlm_model_file,
            kenlm_model_path=kenlm_model_path,
            kenlm_output_perplexity_column_name=kenlm_output_perplexity_column_name,
            kenlm_process_languages=kenlm_process_languages,
            kenlm_sentence_piece_model_file=kenlm_sentence_piece_model_file,
            kenlm_sentence_piece_model_path=kenlm_sentence_piece_model_path,
            kenlm_ray_name= kenlm_ray_name,
            kenlm_ray_head_options= kenlm_ray_head_options,
            kenlm_ray_worker_options= kenlm_ray_worker_options,
            kenlm_runtime_actor_options= kenlm_runtime_actor_options,
            kenlm_runtime_code_location= kenlm_runtime_code_location,
            kenlm_additional_params= kenlm_additional_params,
            
        )
        ComponentUtils.add_settings_to_component(compute_exec_params_task, ONE_HOUR_SEC * 2)
        # start Ray cluster for kenlm
        kenlm_ray_cluster = create_ray_op(
            ray_name=kenlm_ray_name,
            run_id=kenlm_run_id,
            ray_head_options=kenlm_ray_head_options,
            ray_worker_options=kenlm_ray_worker_options,
            server_url=server_url,
            additional_params=str(kenlm_additional_params),
        )
        kenlm_ray_cluster.set_display_name("Start kenlm Ray cluster")
        ComponentUtils.add_settings_to_component(kenlm_ray_cluster, ONE_HOUR_SEC * 2)
        kenlm_ray_cluster.after(compute_exec_params_task)

        # execute kenlm job
        kenlm_execute_job = execute_ray_jobs_op(
            ray_name=kenlm_ray_name,
            run_id=kenlm_run_id,
            additional_params=str(kenlm_additional_params),
            exec_params=compute_exec_params_task.outputs["kenlm"],
            exec_script_name="-m dpk_kenlm.ray.runtime",
            server_url=server_url,
        )
        kenlm_execute_job.set_display_name("Run kenlm")
        ComponentUtils.add_settings_to_component(kenlm_execute_job, ONE_WEEK_SEC)
        if os.getenv("KFPv2", "0") == "1":     
            from kfp import kubernetes
            
            # FIXME: Due to kubeflow/pipelines#10914, secret names cannot be provided as pipeline arguments.
            # As a workaround, the secret name is hard coded.
            env2key = ComponentUtils.set_secret_key_to_env()
            kubernetes.use_secret_as_env(task=kenlm_execute_job, secret_name=S3_SECRET, secret_key_to_env=env2key)
        else:
            ComponentUtils.set_s3_env_vars_to_component(kenlm_execute_job, data_s3_access_secret)
        kenlm_execute_job.after(kenlm_ray_cluster)

if __name__ == "__main__":
    import argparse
    default_output = os.path.relpath(__file__.replace(".py", ".yaml"))
    parser = argparse.ArgumentParser(description="KFP pipeline with kenlm")
    parser.add_argument("-o", "--output", type=str, default=default_output, help="output kubeflow file (%(default)s)")
    parser.add_argument("-i", "--image", type=str, default=KFP_IMAGE, help="KFP base image (%(default)s)")
    parser.add_argument("-p", "--pull_secret", type=str, default=KFP_IMAGE_PULL_SECRET, help="KFP base image pull secret name (%(default)s)")
    parser.add_argument("-s", "--s3_access_secret", type=str, default=S3_SECRET, help="COS access secret name (%(default)s)")
    parser.add_argument("-t", "--tmp_folder", type=str, default=TMP_FOLDER, help="temporary storage (COS folder) (%(default)s)")
    parser.add_argument("-f", "--input_folder", type=str, default=INPUT_FOLDER, help="COS folder with data to be processed (%(default)s)")
    parser.add_argument("-d", "--output_folder", type=str, default=OUTPUT_FOLDER, help="COS folder to store the processed data (%(default)s)")
    parser.add_argument("-r", "--run_name", type=str, default=None, help="start a run with this name")
    parser.add_argument("-e", "--experiment_name", type=str, default=None, help="set the exeperiment name for the run")

    args = parser.parse_args()
    KFP_IMAGE=args.image
    KFP_IMAGE_PULL_SECRET==args.pull_secret
    S3_SECRET=args.s3_access_secret
    INPUT_FOLDER=args.input_folder
    OUTPUT_FOLDER=args.output_folder
    TMP_FOLDER=args.tmp_folder

    if args.run_name:
        # Run the pipeline
        kfp.Client().create_run_from_pipeline_func(kenlm_dpkt, run_name=args.run_name, experiment_name=args.experiment_name)
    else:
        # Compile the pipeline
        kfp.compiler.Compiler().compile(kenlm_dpkt, args.output)