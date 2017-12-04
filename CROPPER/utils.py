from PIL import Image
import cv2
import numpy as np
import glob
import os
import shutil

global BASE_PATH

def cv2grey(_image):
    return cv2.cvtColor(_image, cv2.COLOR_BGR2GRAY)

def imageCrop(_image, x, y, w, h, box_scale = 1):
    crop = _image[y: y + h, x: x + w]
    # xDelta = max(_box[2] * (box_scale - 1), 0)
    # yDelta = max(_box[3] * (box_scale - 1), 0)

    # box = [_box[0] - xDelta, _box[1] - yDelta, _box[0] + _box[2] + xDelta, _box[1] + _box[3] + yDelta]

    return crop

def DetectFace(_image, _face_cascade, DEBUG = False, FACES = False):
    min_size = (20, 20)
    haar_scale = 1.1
    min_neighbors = 3
    haar_flags = 0

    # np.histogram(_image.flatten(), 256, [0, 256])

    faces = _face_cascade.detectMultiScale(_image, haar_scale, min_neighbors, haar_flags, min_size)

    if len(faces) and FACES:
        for (x, y, w, h) in faces:
            point_1 = (int(x), int(y))
            point_2 = (int(x + w), int(y + h))
            cv2.rectangle(_image, point_1, point_2, (255, 0, 0), 5, 8, 0)

        if DEBUG:
            cv2.imshow('Face Detected Image', _image)

    if FACES:
        return faces
    else:
        return _image

def cropFaces(_image, BASE_PATH, box_scale = 1):
    face_cascade = cv2.CascadeClassifier('/home/shankz/IdeaProjects/VIZION/models/haarcascade_frontalface_alt.xml')

    global frame_count
    frame_count = 0

    image_list = glob.glob(_image)

    if len(image_list) <= 0:
        print 'No File Found'
        return

    for image in image_list:
        pil_img = Image.open(image)
        img = cv2.imread(image)
        cv_grey_img = cv2grey(img)
        head, tail = os.path.split(image)
        frame_count = tail.split("-")[1].split(".")[0]
        faces = DetectFace(cv_grey_img, face_cascade, FACES = True)

        if len(faces) and faces.any():
            dir = str(BASE_PATH) + '/frame_' + str(frame_count)
            if os.path.exists(dir):
                shutil.rmtree(dir)
            os.mkdir(dir)
            num = 1

            for (x, y, w, h) in faces:
                cropped_image = imageCrop(img, x, y, w, h, box_scale = box_scale)
                frame, ext = os.path.splitext(image)
                print 'Found faces'
                name = str(BASE_PATH) + '/frame_' + str(frame_count) + '/' + str(num) + ext
                cv2.imwrite(name, cropped_image)
                num += 1
        else:
            print 'No faces found: ', image

