#!/usr/bin/env cwl-runner
cwlVersion: v1.2
class: CommandLineTool
baseCommand: ["sh", "example.sh", ";", "cat", "InitialWorkDirRequirement.yaml"]

requirements:
  ShellCommandRequirement: {}
  InitialWorkDirRequirement:
    listing:
      - entryname: example.sh
        entry: |-
          PREFIX='Message is:'
          MSG="\${PREFIX} $(inputs.message)"
          echo \${MSG}
      - entry: $(inputs.stage)

inputs:
  message: string
  stage:
    type: File
    default:
      class: File
      path: "InitialWorkDirRequirement.yaml"
outputs: []
