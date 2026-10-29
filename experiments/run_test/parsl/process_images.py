import glob, parsl
from concurrent.futures import wait

from parsl import Config
from parsl.executors import HighThroughputExecutor
from parsl.providers import SlurmProvider
from parsl.launchers import SrunLauncher
from parsl.data_provider.files import File

from cwl_app import CWLApp


config = Config(
    executors=[
        HighThroughputExecutor(
            max_workers_per_node=4,
            provider=SlurmProvider(
                # account="kdirker",
                walltime="0:15:00",
                nodes_per_block=1,
                cores_per_node=16,
                launcher=SrunLauncher(),
                # worker_init="source /var/scratch/kdirker/master-thesis/run_test/parsl/env/bin/activate",
            )
        )
    ]
)





parsl.load(config)



input_fits = [
    File("/var/scratch/kdirker/image_mf_01.fits"),
    File("/var/scratch/kdirker/image_mf_02.fits"),
    File("/var/scratch/kdirker/image_mf_04.fits"),
]

def process_images(input_fits: list):
    imageplotter = CWLApp("imageplotter.cwl")
    # noiseremover = CWLApp("noiseremover.cwl")
    out = File("./before_noise_remover.png")
    return imageplotter(
    # image_plotter_future = resize_image(
        input_fits=input_fits,
        output_image="before_noise_remover.png",
        output=out
    )
    

    # noiseremover_future = []
    # for scattered_input in ...: 
    #     filtered_img_future = filter_image(
    #         input_image=resized_img_future.outputs[0],
    #         sepia=True,
    #     )

    # after_plot_inspect = blur_image(
    #     input_fits=...,
    #     output_image="after_noise_remover.png"
    # )
    # return before_plot_inspect, after_plot_inspect

# final_imgs = [process_images(img) 
            #   for img in glob.glob(r’**/*.png’, recursive=True)]

# concurrent.futures.wait(final_imgs, return_when=concurrent.futures.ALL_COMPLETED)

print(process_images(input_fits).result())