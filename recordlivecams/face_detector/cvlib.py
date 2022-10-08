from pathlib import Path

import cv2

import cvlib as cv


def detect_faces(
    username: str, image_path: Path, thumb_folder: Path, new_folder: Path
) -> int:
    image = cv2.imread(str(image_path))
    faces, confidences = cv.detect_face(image)

    if len(faces) >= 2:
        # Move all images from that streamer into a different folder
        for file in thumb_folder.glob(f"{username}-*"):
            file.rename(new_folder / file.name)

    return faces
