class: Workflow
cwlVersion: v1.2
id: process_images_vf_array
label: process_images_vf_array
inputs: []
outputs:
  - id: before_noise_remover
    outputSource:
      - imageplotter/output
    type: File
steps:
  - id: imageplotter
    in:
      - id: input_fits
        default:
          - class: File
            path: image_mf_01.fits
          - class: File
            path: image_mf_02.fits
          - class: File
            path: image_mf_04.fits
      - id: output_image
        default: before_noise_remover.png
    out:
      - id: output
    run: ../steps/imageplotter.cwl
    label: imageplotter
