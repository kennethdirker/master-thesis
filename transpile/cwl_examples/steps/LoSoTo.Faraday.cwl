#!/usr/bin/env cwl-runner

class: CommandLineTool
cwlVersion: v1.2
id: losoto_faraday

doc: |
  Faraday rotation extraction from either a rotation table or a
  circular phase (of which the operation get the polarisation difference).


requirements:
  InlineJavascriptRequirement:
    expressionLib:
      - { $include: utils.js}
  InitialWorkDirRequirement:
    listing:
      - entryname: 'parset.config'
        entry: $(get_losoto_config('FARADAY').join('\n'))

      - entryname: $(inputs.input_h5parm.basename)
        entry: $(inputs.input_h5parm)
        writable: true

baseCommand: "losoto"

arguments:
  - --verbose
  - $(inputs.input_h5parm.basename)
  - parset.config

hints:
  DockerRequirement:
    dockerPull: astronrd/linc

inputs:
  - id: input_h5parm
    type: File
  - id: soltab
    type: string?
    doc: "Solution table"
    default: sol000/rotation000
  - id: soltabOut
    type: string?
    doc: output table name (same solset)
  - id: refAnt
    type: string?
    doc:  Reference antenna, by default the first.
  - id: maxResidual
    type: float?
    doc: |
      Max average residual in radians before flagging datapoint,
      by default 1. If 0: no check.

outputs:
  - id: output_h5parm
    type: File
    outputBinding:
      glob: $(inputs.input_h5parm.basename)
  - id: log
    type: File[]
    outputBinding:
      glob: '$(inputs.input_h5parm.basename)-losoto*.log'

stdout: $(inputs.input_h5parm.basename)-losoto.log
stderr: $(inputs.input_h5parm.basename)-losoto_err.log
