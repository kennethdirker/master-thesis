class: Workflow
cwlVersion: v1.2
id: top_process_images
label: process_images
inputs:
  - id: list_of_fits
    type: File[]

outputs:
  - id: before_noise_remover
    outputSource:
      - subworkflow/before_noise_remover
    type: File
  - id: after_noise_remover
    outputSource:
      - subworkflow/after_noise_remover_plot
    type: File
  - id: top_before_noise_remover_plot
    outputSource:
      - imageplotter/output
    type: File
  - id: top_after_noise_remover_plot
    outputSource:
      - after_plot_inspect/output
    type: File

steps:
  - id: subworkflow
    in:
      - id: fit_list
        source:
          - list_of_fits
    out:
      - id: before_noise_remover
      - id: after_noise_remover_plot
    run: process_images_offline.cwl
    label: subworkflow

  - id: imageplotter
    in:
      - id: input_fits
        source:
          - list_of_fits
      - id: output_image
        default: top_before_noise_remover.png
    out:
      - id: output
    run: ../steps/imageplotter.cwl
    label: imageplotter

  - id: noiseremover
    in:
      - id: input
        source: list_of_fits
        valueFrom: $(self[0])
      - id: output_file_name
        valueFrom: $('top_no_noise_' + inputs.input[0].basename)
    out:
      - id: output
    run: ../steps/noiseremover.cwl
    label: noiseremover

  - id: after_plot_inspect
    in:
      - id: input_fits
        source: noiseremover/output
        valueFrom: $([self])
      - id: output_image
        default: top_after_noise_remover.png
    out:
      - id: output
    run: ../steps/imageplotter.cwl
    label: imageplotter

requirements:
  - class: StepInputExpressionRequirement
  - class: InlineJavascriptRequirement
  - class: SubworkflowFeatureRequirement
