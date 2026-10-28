import glob
from concurrent.futures import wait
from myconfigs import perlmutter_config
from cwl_app import CWLApp

parsl.load(perlmutter_config)


imageplotter = CWLApp("imageplotter.cwl")
noiseremover = CWLApp("noiseremover.cwl")

def process_images(...: ...) -> Future:
    image_plotter_future = resize_image(
        input_fits=...,
        output_image="before_noise_remover.png",
    )

    noiseremover_future = []
    for scattered_input in ...: 
        filtered_img_future = filter_image(
            input_image=resized_img_future.outputs[0],
            sepia=True,
        )

    after_plot_inspect = blur_image(
        input_fits=...,
        output_image="after_noise_remover.png"
    )
    return before_plot_inspect, after_plot_inspect

final_imgs = [process_images(img) 
              for img in glob.glob(r’**/*.png’, recursive=True)]

concurrent.futures.wait(final_imgs, return_when=concurrent.futures.ALL_COMPLETED)