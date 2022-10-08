from pathlib import Path

import dlib

cnn_path = Path(__file__).parent.resolve() / "mmod_human_face_detector.dat"
cnn_face_detector = dlib.cnn_face_detection_model_v1(str(cnn_path))


def detect_faces(
    username: str, image: Path, thumb_folder: Path, new_folder: Path
) -> int:
    img = dlib.load_rgb_image(str(image))
    # The 1 in the second argument indicates that we should upsample the image
    # 1 time.  This will make everything bigger and allow us to detect more faces.
    dets = cnn_face_detector(img, 1)
    """
    This detector returns a mmod_rectangles object. This object contains a list of mmod_rectangle objects.
    These objects can be accessed by simply iterating over the mmod_rectangles object
    The mmod_rectangle object has two member variables, a dlib.rectangle object, and a confidence score.
    
    It is also possible to pass a list of images to the detector.
        - like this: dets = cnn_face_detector([image list], upsample_num, batch_size = 128)

    In this case it will return a mmod_rectangless object.
    This object behaves just like a list of lists and can be iterated over.
    """
    # print("Number of faces detected: {}".format(len(dets)))

    if len(dets) >= 2:
        # Move all images from that streamer into a different folder
        for file in thumb_folder.glob(f"{username}-*"):
            file.rename(new_folder / file.name)
