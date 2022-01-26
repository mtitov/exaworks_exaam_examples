#!/usr/bin/env python3

"""
ExaAM & ExaWorks projects
Original code: https://code.ornl.gov/ecpcitest/exaam/workflow/example_exaconstit
"""

import json
import os

import radical.entk as re

RESOURCE_DESCRIPTION = {
    'resource'     : '',
    'project'      : '',
    'queue'        : '',  # (None value will use default queue name from RP)
    'access_schema': 'local',
    'walltime'     : 10,  # total running time (in minutes)
    'cpus'         : 1,   # total number of required cores
    'gpus'         : 0    # total number of required gpus
}

CREDS_FILE_NAME = 'creds.json'  # see its format at the end of this file


# ------------------------------------------------------------------------------
#
class Workflow:

    def __init__(self, **kwargs):
        self.input_dir = kwargs.get('input_dir', '.')

    def get(self):
        # generate pipeline with stages
        pipeline = re.Pipeline()
        # fill-up `pipeline` with stages
        pipeline.add_stages([
            self.stage_setup(),
            self.stage_preprocess(),
            self.stage_main(),
            self.stage_postprocess()
        ])
        return pipeline

    def stage_setup(self):
        # NOTE: could be split into 3 tasks with the following executables:
        #       - `setup_conda`
        #       - `setup_spack`
        #       - `install_flux`
        task = re.Task({
            'executable'     : '%s/' % self.input_dir +
                               'scripts/workflow/setup.sh',
            'pre_exec'       : ['export WORK_DIR=$PWD'],
            'link_input_data': ['$SHARED/%s/' % self.input_dir]
        })
        stage = re.Stage()
        stage.add_tasks(task)
        return stage

    def stage_preprocess(self):
        work_dir = '%s/preprocessing' % self.input_dir
        task = re.Task({
            'executable'     : '%s/exaconstit_cli_preprocessing.py' % work_dir,
            'arguments'      : ['-ifdir', '%s/' % work_dir,
                                '-ifile', 'exaca.csv',
                                '-ofdir', './output/',
                                '-runame', 'simulation',
                                '-c', '1',
                                '-mg', '-mgdir', '${exaconstit_build_dir}/bin/',
                                '-t', '298.0',
                                '-fprops', './props_cp_voce_in625.txt',
                                '-nprops', '17',
                                '-fstate', './state_cp_voce.txt',
                                '-nstates', '24'],
            'pre_exec'       : ['source %s/scripts/paths.sh' % self.input_dir,
                                'mkdir -p ${workflow_dir}',
                                'cd ${workflow_dir}',
                                'mkdir output'],
            'link_input_data': ['$SHARED/%s/' % self.input_dir]
        })
        stage = re.Stage()
        stage.add_tasks(task)
        return stage

    def stage_main(self):
        work_dir = '%s/main_simulations' % self.input_dir
        task = re.Task({
            'executable'     : '%s/job_cli.py' % work_dir,
            'arguments'      : ['-sdir', '%s/' % work_dir,
                                '-odir', './runs/',
                                '-imtfile', 'options_master.toml',
                                '-iotfile', 'options.toml',
                                '-ijfile', 'hip_mechanics.flux',
                                '-ijfd', '%s/' % work_dir,
                                '-iofile', 'simulation_test_matrix.csv'],
            'pre_exec'       : ['source %s/scripts/paths.sh' % self.input_dir,
                                'cd ${workflow_dir}',
                                'mkdir runs'],
            # 'cpu_reqs'       : {'cpu_processes'   : 1,
            #                     'cpu_process_type': None,   # 'MPI'
            #                     'cpu_threads'     : 1,
            #                     'cpu_thread_type' : None},  # 'OpenMP',
            # 'gpu_reqs'       : {'gpu_processes'   : 1,
            #                     'gpu_process_type': None,   # 'MPI'
            #                     'gpu_threads'     : 1,
            #                     'gpu_thread_type' : None},  # 'OpenMP'
            'link_input_data': ['$SHARED/%s/' % self.input_dir]
        })
        stage = re.Stage()
        stage.add_tasks(task)
        return stage

    def stage_postprocess(self):
        work_dir = '%s/postprocessing' % self.input_dir
        task  = re.Task({
            'executable'     : '%s/barlat_optimize.py' % work_dir,
            'arguments'      : ['-sdir', './runs/',
                                '-odir', './runs/',
                                '-rve_id', 'simulation'],
            'pre_exec'       : ['source %s/scripts/paths.sh' % self.input_dir,
                                'cd ${workflow_dir}'],
            'link_input_data': ['$SHARED/%s/' % self.input_dir]
        })
        stage = re.Stage()
        stage.add_tasks(task)
        return stage


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    # read credentials
    with open(CREDS_FILE_NAME, encoding='utf8') as f:
        creds = json.load(f)

    os.environ['RADICAL_PILOT_DBURL'] = creds['mongodb']['url']

    appman = re.AppManager(**creds['rabbitmq'])
    appman.resource_desc = RESOURCE_DESCRIPTION
    appman.workflow      = [Workflow(input_dir='input_data').get()]
    appman.shared_data   = ['example_exaconstit/ > $SHARED/input_data']

    appman.run()

# ------------------------------------------------------------------------------

# JSON file with credentials ("creds.json")
# {
#     "rabbitmq": {
#         "hostname": "",
#         "port"    : "",
#         "username": "",
#         "password": ""
#     },
#     "mongodb": {
#         "url"     : "mongodb://<user>:<pass>@<host>:<port>/<db>"
#     }
# }

# ------------------------------------------------------------------------------
