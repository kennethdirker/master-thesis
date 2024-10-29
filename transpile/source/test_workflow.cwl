#!/usr/bin/env cwl-runner

cwlVersion: v1.2
class: Workflow

requirements:
- class: ScatterFeatureRequirement
- class: SubworkflowFeatureRequirement

inputs:
- id: surls
  type: string[]

outputs:
- id: solutions
  type: File
  outputSource:
  - linc_calibrator/solutions
- id: summary
  type: File
  outputSource:
  - linc_calibrator/summary
- id: inspection_plots
  type: File
  outputSource:
  - compress_inspection_plots/compressed
- id: log_files
  type: File
  outputSource:
  - compress_logs/compressed
- id: quality
  type: Any
  outputSource:
  - format_quality/quality

steps:
- id: format_quality
  in:
  - id: plots
    source: list_inspection_plots/output
  - id: summary
    source: linc_calibrator/summary
  run: ../steps/format_quality.cwl
  out:
  - id: quality
- id: list_inspection_plots
  in:
  - id: input
    source: linc_calibrator/inspection_plots
  run: ../steps/list_directory_files.cwl
  out:
  - id: output
- id: fetch_data
  in:
  - id: surl_link
    source: surls
  scatter: surl_link
  run: ../steps/fetch_data.cwl
  out:
  - id: uncompressed
- id: linc_calibrator
  in:
  - id: msin
    source: fetch_data/uncompressed
  run: HBA_calibrator.cwl
  out:
  - id: solutions
  - id: summary
  - id: inspection_plots
  - id: log_files
- id: compress_inspection_plots
  in:
  - id: directory
    source: linc_calibrator/inspection_plots
  run: ../steps/compress.cwl
  out:
  - id: compressed
- id: compress_logs
  in:
  - id: directory
    source: linc_calibrator/log_files
  run: ../steps/compress.cwl
  out:
  - id: compressed

