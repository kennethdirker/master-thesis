class: CommandLineTool
cwlVersion: v1.2
id: applybeam
baseCommand:
  - DP3
inputs:
  - id: max_dp3_threads
    type: int?
    inputBinding:
      position: 0
      prefix: numthreads=
      separate: false
  - id: msin
    type: Directory
    inputBinding:
      position: 0
      prefix: msin=
      separate: false
    doc: Input Measurement Set
  - default: DATA
    id: msin_datacolumn
    type: string
    inputBinding:
      position: 0
      prefix: msin.datacolumn=
      separate: false
    doc: Input data Column
  - id: msout_datacolumn
    type: string
    inputBinding:
      position: 0
      prefix: msout.datacolumn=
      separate: false
    doc: Output data column
  - default: applybeam
    id: type
    type: string?
    inputBinding:
      position: 0
      prefix: applybeam.type=
      separate: false
    doc: >
      Type of correction to perform. When using H5Parm, this is for now the name
      of the soltab; the type will be deduced from the metadata in that soltab,
      except for full Jones, in which case correction should be 'fulljones'.
  - id: storagemanager
    type: string
    default: ""
    inputBinding:
      prefix: msout.storagemanager=
  - id: databitrate
    type: int?
    inputBinding:
       prefix: msout.storagemanager.databitrate=
       separate: false
  - id: updateweights
    type: boolean?
    inputBinding:
      position: 0
      prefix: applybeam.updateweights=True
      separate: false
  - id: usechannelfreq
    type: boolean?
    default: true
    inputBinding:
      valueFrom: $(!self)
      position: 0
      prefix: applybeam.usechannelfreq=False
      separate: false
  - id: invert
    type: boolean?
    default: true
    inputBinding:
      valueFrom: $(!self)
      position: 0
      prefix: applybeam.invert=False
      separate: false
  - id: beammode
    type: string?
    inputBinding:
      position: 0
      prefix: applybeam.beammode=
      separate: false
  
outputs:
  - id: msout
    doc: Output Measurement Set
    type: Directory
    outputBinding:
      glob: $(inputs.msin.basename)
  - id: logfile
    type: File[]
    outputBinding:
      glob: 'applycal_$(inputs.type)*.log'
stdout: applycal_$(inputs.type).log
stderr: applycal_$(inputs.type)_err.log
arguments:
  - 'steps=[applybeam,count]'
  - msout=.
requirements:
  - class: InplaceUpdateRequirement
    inplaceUpdate: true
  - class: InitialWorkDirRequirement
    listing:
      - entry: $(inputs.msin)
        writable: true
  - class: InlineJavascriptRequirement
  - class: ResourceRequirement
    coresMin: $(inputs.max_dp3_threads)
  - class: EnvVarRequirement
    envDef:
      DP3_DATA_DIR: /data
      DP3_CACHE_DIR: $(inputs.storagemanager)
hints:
  - class: DockerRequirement
    dockerPull: astronrd/linc

