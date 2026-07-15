#!/usr/bin/env python3
from astropy.io import fits
import sys
from argparse import ArgumentParser
import matplotlib.pyplot as plt
import numpy as np


def parse_args():
    parser = ArgumentParser(description='Reduce noise in a fits image')
    parser.add_argument('images', nargs='+')
    parser.add_argument('output_file')
    parser.add_argument('--cut_size', type=int, default=200)
    parser.add_argument('--max_intensity', type=float, default=1.e3)
    parser.add_argument('--max_columns', type=int, default=2)
    
    return parser.parse_args()
    

def open_image(image):
    hdus = fits.open(image)
    return hdus[0]

def plot_images(images_list, output_file, cut_size=200, v_max= 1.e3, max_columns=2):
    n_images = len(images_list)
    max_rows = n_images // max_columns + 1
    figure = plt.figure()
    for i in range(n_images):
        ax = figure.add_subplot(max_columns, max_rows, i + 1)
        hdu = open_image(images_list[i])
        size = hdu.data.shape[2]
        data_sub_domain = hdu.data[0, 0, (size//2 - 400): (size//2 + 401), (size//2 - 400):(size//2 + 401)]
        ax.imshow(data_sub_domain, vmax=1.e-3)
    plt.tight_layout()
    figure.savefig(output_file)
    return hdu
    
if __name__ == '__main__':
    args = parse_args()
    plot_images(args.images, args.output_file,
                args.cut_size, args.max_intensity, args.max_columns)
