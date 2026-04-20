#!/usr/bin/env cwl-runner

cwlVersion: v1.2
class: Workflow
label: multilevel_scatter_example

requirements:
- class: StepInputExpressionRequirement
- class: InlineJavascriptRequirement
- class: SubworkflowFeatureRequirement
- class: ScatterFeatureRequirement

inputs:
- id: words
  type: string[]

outputs:
  dot_files:
    type: File[]
    outputSource: combine_dot/output_files
  cross_files:
    type: File[]
    outputSource: combine_cross/output_files
  dot_echoes:
    type:
      type: array
      items:
        type: array
        items: string
    outputSource: combine_dot/content_logs
  cross_echoes:
    type:
      type: array
      items:
        type: array
        items: string
    outputSource: combine_cross/content_logs

steps:
- id: combine_dot
  in:
  - id: str
    source: words
  - id: num
    default: [1,2,3]
  scatter:
  - str
  - num
  scatterMethod: dotproduct
  run: multilevel_scatter_create_echo.cwl
  out: [output_files, content_logs]

- id: combine_cross
  in:
  - id: str
    source: words
  - id: num
    default: [1,2]
  scatter:
  - str
  - num
  scatterMethod: flat_crossproduct
  run: multilevel_scatter_create_echo.cwl
  out: [output_files, content_logs]

id: multilevel_scatter_1_example
