#!/usr/bin/env python3
from astropy.io import fits
import sys
from argparse import ArgumentParser
import numpy as np


def parse_args():
    parser = ArgumentParser(description='Reduce noise in a fits image')
    parser.add_argument('input_fits')
    parser.add_argument('output_fits')
    return parser.parse_args()
    
def open_image(image):
    hdus = fits.open(image)
    return hdus[0]

def clean_image(input_image, output_image):
    hdu = open_image(input_image)
    
    noise = np.std(hdu.data)
    hdu.data = hdu.data - 3 * noise
    hdu.data[hdu.data < 0] = 0
    hdu.data += np.random.normal(0, noise, hdu.data.shape)
    
    fits.writeto(output_image, hdu.data, hdu.header)
    return hdu
    
if __name__ == '__main__':
    args = parse_args()
    clean_image(args.input_fits, args.output_fits)
