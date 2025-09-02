id: compress
label: compress
class: CommandLineTool
cwlVersion: v1.2
inputs: 
  - id: directory
    type: Directory
outputs: 
  - id: compressed
    type: File
    outputBinding:
      glob: 'out/*.tar'
baseCommand: 
 - 'bash'
 - 'tar.sh'
doc: 'Create archive from directory'
requirements:
  InitialWorkDirRequirement:
    listing:
      - entry: $(inputs.directory)
      - entryname: 'tar.sh' 
        entry: |
          #!/bin/bash
          mkdir out
          tar -h -cvf out/$(inputs.directory.basename).tar $(inputs.directory.basename) 
