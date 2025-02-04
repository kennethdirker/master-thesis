class: CommandLineTool
cwlVersion: v1.2
id: check_ateam_separation
baseCommand:
  - Ateamclipper.py
inputs:
  - id: msin
    type:
      - Directory
      - type: array
        items: Directory
    inputBinding:
      position: 0
    doc: Input measurement set
outputs:
  - id: msout
    doc: Output MS
    type: Directory
    outputBinding:
      glob: $(inputs.msin.basename)
  - id: logfile
    type: File[]
    outputBinding:
      glob: Ateamclipper.log
  - id: output
    type: File
    outputBinding:
      glob: Ateamclipper.txt
label: Ateamclipper
hints:
  - class: InitialWorkDirRequirement
    listing:
      - entry: $(inputs.msin)
        writable: true
  - class: InplaceUpdateRequirement
    inplaceUpdate: true
  - class: DockerRequirement
    dockerPull: astronrd/linc
  - class: InlineJavascriptRequirement
  - class: ResourceRequirement
    coresMin: 8
stdout: Ateamclipper.log
