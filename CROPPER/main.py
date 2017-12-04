from PIL import Image
import glob
import os
import cv2
import sys
from utils import cv2grey, DetectFace, cropFaces

global BASE_PATH


# Check if the base path is provided with the argument
if len(sys.argv) != 2:
    print "CROPPER USAGE: main <base_path>"
    sys.exit(1)

# Get the base path from the argument
BASE_PATH = sys.argv[1]

cropFaces(str(BASE_PATH) + '/*.jpg', BASE_PATH, box_scale = 1)
