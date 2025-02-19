
import logging
from typing import Optional, Union

from schema_salad.exceptions import ValidationException

from cwltool.errors import WorkflowException
from cwltool.command_line_tool import CommandLineJob, ExpressionJob
from cwltool.workflow import Workflow
from cwltool.workflow_job import WorkflowJob
from cwltool.executors import (
        JobExecutor,
        Process,
        CWLObjectType,
        RuntimeContext
    )


class DaskExecutor(JobExecutor):
    def run_jobs(
            self,
            process: Process,
            job_order_object: CWLObjectType,
            logger: logging.Logger,
            runtime_context: RuntimeContext
    ) -> None:        
        process_run_id: Optional[str] = None

        # define provenance profile for single commandline tool
        if not isinstance(process, Workflow) and runtime_context.research_obj is not None:
            # 
            print("Provenance happens")
            # 

            process.provenance_object = runtime_context.research_obj.initialize_provenance(
                full_name=runtime_context.cwl_full_name,
                # following are only set from main when directly command line tool
                # when nested in a workflow, they should be disabled since they would
                # already have been provided/initialized by the parent workflow prov-obj
                host_provenance=runtime_context.prov_host,
                user_provenance=runtime_context.prov_user,
                orcid=runtime_context.orcid,
                # single tool execution, so RO UUID = wf UUID = tool UUID
                run_uuid=runtime_context.research_obj.ro_uuid,
                fsaccess=runtime_context.make_fs_access(""),
            )
            process.parent_wf = process.provenance_object

        # Recursively collect all Workflow Steps
        def traverse(
                process: Process, 
                dependencies: dict = {},
                tools: list = [],
                # groupings: list = []  #TODO steps in sub-workflows are independent from steps higher up in the workflow chain
            ):
            if isinstance(process, Workflow):
                # Iterate sub-workflows, add CommandLineTools/ExpressionTools
                # to step_list. Explore sub-workflows, while keeping track of
                # dependencies between steps.
                for step in process.steps:  # step: WorkflowStep
                    sub_proc = ...
                    self.traverse(sub_proc)

            elif isinstance(process, Union[CommandLineJob, ExpressionJob]):
                # Add CommandLineTools/ExpressionTools to tools
                tools.append(...)
                return
            
            return tools
                

        # 

        # jobiter = process.job(job_order_object, self.output_callback, runtime_context)

        # try:
        #     for job in jobiter:
        #         if job is not None:
        #             if runtime_context.builder is not None and hasattr(job, "builder"):
        #                 job.builder = runtime_context.builder
        #             if job.outdir is not None:
        #                 self.output_dirs.add(job.outdir)
        #             if runtime_context.research_obj is not None:
        #                 if not isinstance(process, Workflow):
        #                     prov_obj = process.provenance_object
        #                 else:
        #                     prov_obj = job.prov_obj
        #                 if prov_obj:
        #                     runtime_context.prov_obj = prov_obj
        #                     prov_obj.fsaccess = runtime_context.make_fs_access("")
        #                     prov_obj.evaluate(
        #                         process,
        #                         job,
        #                         job_order_object,
        #                         runtime_context.research_obj,
        #                     )
        #                     process_run_id = prov_obj.record_process_start(process, job)
        #                     runtime_context = runtime_context.copy()
        #                 runtime_context.process_run_id = process_run_id
        #             if runtime_context.validate_only is True:
        #                 if isinstance(job, WorkflowJob):
        #                     name = job.tool.lc.filename
        #                 else:
        #                     name = getattr(job, "name", str(job))
        #                 print(
        #                     f"{name} is valid CWL. No errors detected in the inputs.",
        #                     file=runtime_context.validate_stdout,
        #                 )
        #                 return
        #             job.run(runtime_context)
        #         else:
        #             logger.error("Workflow cannot make any more progress.")
        #             break
        # except (
        #     ValidationException,
        #     WorkflowException,
        # ):  # pylint: disable=try-except-raise
        #     raise
        # except Exception as err:
        #     logger.exception("Got workflow error")
        #     raise WorkflowException(str(err)) from err

