#!/usr/bin/env cwl-runner

class: CommandLineTool
cwlVersion: v1.2
id: losoto_clocktec

doc: |
  Separate phase solutions into Clock and TEC.
  The Clock and TEC values are stored in the specified output soltab with type 'clock', 'tec', 'tec3rd'.

# requirements:
#   - class: InlineJavascriptRequirement
#     expressionLib:
#       - { $include: utils.js }
#       - |
#         /**
#          * A merely illustrative example function that uses a function
#          * from the included custom-functions.js file to create a
#          * Hello World message.
#          *
#          * @param {Object} message - CWL document input message
#          */
#         var createHelloWorldMessage = function (message) {
#           return capitalizeWords(message);
#         };
#   - class: InitialWorkDirRequirement
#     listing:
#       - entryname: 'parset.config'
#         entry: $(get_losoto_config('CLOCKTEC').join('\n'))
#       - entryname: $(inputs.input_h5parm.basename)
#         entry: $(inputs.input_h5parm)
#         writable: true

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
    type: string
    doc: "Solution table"
  - id: clocksoltabOut
    doc: output soltab name for clock
    type: string?
  - id: tecsoltabOut
    doc: output soltab name for tec
    type: string?
  - id: offsetsoltabOut
    doc: output soltab name for phase offset
    type: string?
  - id: tec3rdsoltabOut
    doc: output soltab name for tec3rd offset
    type: string?
  - id: FlagBadChannels
    type: boolean?
    doc: Detect and remove bad channel before fitting, by default True.
  - id: flagCut
    type: float?
  - id: chi2cut
    type: float?
  - id: CombinePol
    type: boolean?
    doc: |
      Find a combined polarization solution, by default False.
  - id: removePhaseWraps
    type: boolean?
    doc: |
      Detect and remove phase wraps, by default True.
  - id: Fit3rdOrder
    type: boolean?
    doc: |
      Fit a 3rd order ionospheric ocmponent (usefult <40 MHz). By default False.
  - id: Circular
    type: boolean?
    doc: |
      Assume circular polarization with FR not removed. By default False.
  - id: reverse
    type: boolean?
    doc:
      Reverse the time axis. By default False.
  - id: invertOffset
    type: boolean?
    doc: |
      Invert (reverse the sign of) the phase offsets. By default False. Set to True
      if you want to use them with the residuals operation.

outputs:
  - id: output_h5parm
    type: File
    outputBinding:
      glob: $(inputs.input_h5parm.basename)
  - id: parset
    type: File
    outputBinding:
      glob: parset.config
  - id: log
    type: File[]
    outputBinding:
      glob: '$(inputs.input_h5parm.basename)-losoto*.log'

stdout: $(inputs.input_h5parm.basename)-losoto.log
stderr: $(inputs.input_h5parm.basename)-losoto_err.log
