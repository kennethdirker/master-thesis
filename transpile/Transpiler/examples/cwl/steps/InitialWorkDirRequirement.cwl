#!/usr/bin/env cwl-runner
cwlVersion: v1.2
class: CommandLineTool
id: InitialWorkDirRequirement
baseCommand: ["example.sh"]
arguments:
  - valueFrom: ;
    shellQuote: false
  - cat
  - InitialWorkDirRequirement.yaml

  # CWL needs to cover special characters, we dont
  # MSG="\${PREFIX} $(inputs.message)"
  # echo \${MSG}

requirements:
  ShellCommandRequirement: {}
  InitialWorkDirRequirement:
    listing:
      - entryname: example.sh
        entry: |-
          PREFIX='Message is:'
          MSG="${PREFIX} $(inputs.message)"
          echo ${MSG}
      - entry: $(inputs.stage)

inputs:
  message: string
  stage:
    type: File
    default:
      class: File
      path: "InitialWorkDirRequirement.yaml"
outputs: []
