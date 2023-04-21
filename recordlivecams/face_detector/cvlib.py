from pathlib import Path

# import cnn
from face_recognition import face_locations, load_image_file

import cv2
import cvlib as cv


def detect_faces(
    username: str, image_path: Path, thumb_folder: Path, new_folder: Path
) -> int:
    image = cv2.imread(str(image_path))
    faces, confidences = cv.detect_face(image)

    if len(faces) >= 2:
        # Double check with another model that this isn't a false positive
        # cnn_faces = cnn.detect_faces(image_path)
        image = load_image_file(str(image_path))
        faces = face_locations(image, 0, model="cnn")

        if len(faces) >= 2:
            move_to_folder = new_folder / image_path.parent.name
            move_to_folder.mkdir(parents=True, exist_ok=True)
            # Move all images from that streamer into a different folder
            for file in thumb_folder.rglob(f"**/{username}-*"):
                file.rename(move_to_folder / file.name)

    return faces
