class: CommandLineTool
cwlVersion: v1.2

id: h5parm_collector

baseCommand:
  - H5parm_collector.py
inputs:
  - id: h5parmFiles
    type:
      - File[]
      - File
    inputBinding:
      position: 0
    doc: List of h5parm files
  - default: sol000
    id: insolset
    type: string?
    inputBinding:
      position: 0
      prefix: '--insolset'
    doc: Input solset name
  - default: sol000
    id: outsolset
    type: string?
    inputBinding:
      position: 0
      prefix: '--outsolset'
    doc: Output solset name
  - id: insoltab
    type: string?
    inputBinding:
      position: 0
      prefix: '--insoltab'
    doc: Output solset name
  - default: output.h5
    id: outh5parmname
    type: string
    doc: Output h5parm name
    inputBinding:
      position: 0
      prefix: '--outh5parm'
  - id: squeeze
    type: boolean
    default: false
    inputBinding:
      position: 0
      prefix: '-q'
    doc: removes all axes with the length of 1
  - default: true
    id: verbose
    type: boolean
    inputBinding:
      position: 0
      prefix: '-v'
    doc: verbose output
  - default: true
    id: clobber
    type: boolean
    inputBinding:
      position: 0
      prefix: '-c'
    doc: overwrite output
    
outputs:
  - id: outh5parm
    doc: Output h5parm
    type: File
    outputBinding:
      glob: $(inputs.outh5parmname)
  - id: log
    type: File[]
    outputBinding:
      glob: '$(inputs.outh5parmname)-parm_collector_output*.log'
label: H5parm_collector
hints:
  - class: DockerRequirement
    dockerPull: astronrd/linc
stdout: $(inputs.outh5parmname)-parm_collector_output.log
stderr: $(inputs.outh5parmname)-parm_collector_output_err.log
requirements:
  - class: InlineJavascriptRequirement
